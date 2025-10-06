#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "dt.h"
#include <stdlib.h>
#include <string.h>
// #include <time.h>
#include <stdio.h>
#include <limits.h>

#ifdef DEBUG // define this macro from makefile to enable debug print
    #define DEBUG_PRINT(format, ...) printf(format, ##__VA_ARGS__)
#else
    #define DEBUG_PRINT(format, ...)
#endif

/*----------------------local data structures----------------------*/
// metadata structure for each frame in the buffer pool
typedef struct Frame {
    BM_PageHandle pageHandle; // frame handle, including pageNum (page number in pages file) and data buffer in frame
    bool isDirty;             // dirty flag
    int fixCount;             // fix count
    // related with FIFO
    unsigned int enterCounter;  // counter when the page was loaded into the frame (for FIFO)
    // related with LRU
    unsigned int lastAccessCounter;   // last access counter value(for LRU)
    // related with LFU
    int refCount;             // reference count (for LFU)
    // related with LRU-K
    unsigned long int *accessTimes;    // access history (for LRU-K)
    int accessCount;          // access history size (for LRU-K)
    // related with CLOCK policy
    int clockBit;             // clock bit (for CLOCK policy)
} Frame;

// metadata structure for the buffer pool
typedef struct BM_MgmtData {
    Frame *frames;       // pointer to a frame array
    SM_FileHandle fileHandle;// file handle
    int numReadIO;           // number of read IO
    int numWriteIO;          // number of write IO
    // related with CLOCK
    int clockHand;           // clock hand (for CLOCK policy)
    // related with LRU-K
    int k;                   // LRU-K's K value
    unsigned long int globalTime; // counter for LRU-K
} BM_MgmtData;

// local counter for every time a page is loaded into a frame
static unsigned int gLoadCounter = 0;
static unsigned int gAccessCounter = 0;
/*----------------------Debug functions ----------------------*/
/** 
* @brief show the buffer pool metadata
* @param bm, input value, a buffer pool structure pointer
*/
#ifdef DEBUG
static void showBufferPool(BM_BufferPool *bm) {
    if (bm == NULL) {
        printf("Buffer pool is NULL\n");
        return;
    }
    printf("Buffer Pool: %s, numPages: %d, policy: %d\n", bm->pageFile, bm->numPages, bm->strategy);
}

/** 
* @brief show the frame metadata
* @param frame, input value, a frame metadata structure pointer
*/
static void showFrames(BM_BufferPool *bm) {
    if (bm == NULL) {
        printf("Buffer pool is NULL\n");
        return;
    }
    BM_MgmtData *mgmt = (BM_MgmtData *)bm->mgmtData;
    if (mgmt == NULL) {
        printf("Buffer pool metadata is NULL\n");
        return;
    }
    for (int i = 0; i < bm->numPages; i++) {
        printf("Frame %d: pageNum %d, isDirty %d, fixCount %d, refCount %d, clockBit %d\n",
               i, mgmt->frames[i].pageHandle.pageNum, mgmt->frames[i].isDirty, mgmt->frames[i].fixCount, mgmt->frames[i].refCount, mgmt->frames[i].clockBit);
    }
    printf("Clock Hand: %d\n", mgmt->clockHand);
    printf("k: %d\n", mgmt->k);
    printf("numReadIO: %d, numWriteIO: %d\n", mgmt->numReadIO, mgmt->numWriteIO);
    printf("\n");
}
#endif

/*----------------------Utility functions ----------------------*/
/** 
* @brief get the index of a frame in the buffer pool
* @param bm, input value, a buffer pool structure pointer
* @param pageNum, input value, page number to find
* @return int, the index of the frame in the buffer pool
*/
static int getFrameIndex(BM_BufferPool *bm, PageNumber pageNum) {
    BM_MgmtData *mgmt = (BM_MgmtData *)bm->mgmtData;
    if (mgmt == NULL) THROW(-1, "Buffer pool bm->mgmtData == NULL");

    for (int i = 0; i < bm->numPages; i++) {
        if (mgmt->frames[i].pageHandle.pageNum == pageNum) {
            return i;
        }
    }

    DEBUG_PRINT("pageNum %d not found in buffer pool\n", pageNum);
    return -1;
}

/** 
* @brief find a free frame in the buffer pool
* @param bm, input value, a buffer pool structure pointer
* @return int, the index of the free frame in the buffer pool
*/
static int findFreeFrame(BM_BufferPool *bm) {
    BM_MgmtData *mgmt = (BM_MgmtData *)bm->mgmtData;
    if (mgmt == NULL) THROW(RC_UNVALID_HANDLE, "findFreeFrame: bm->mgmtData == NULL");

    for (int i = 0; i < bm->numPages; i++) {
        if (mgmt->frames[i].fixCount == 0 && mgmt->frames[i].pageHandle.pageNum == NO_PAGE) {
            return i;
        }
    }
    DEBUG_PRINT("No free frame found in buffer pool\n");
    return -1;
}

// 辅助函数：更新页面访问时间戳
static RC recordAccess(BM_BufferPool *bm, int frameIndex) {
    BM_MgmtData *mgmt = (BM_MgmtData *)bm->mgmtData;
    if (mgmt == NULL) THROW(RC_UNVALID_HANDLE, "recordAccess: bm->mgmtData == NULL");

    Frame *frame = &mgmt->frames[frameIndex];
    mgmt->globalTime++;  // 全局时间递增

    if (frame->accessCount < mgmt->k) {
        // 未满K次，直接记录
        frame->accessTimes[frame->accessCount++] = mgmt->globalTime;
    } else {
        // 已满K次，移位并更新最近一次
        for (int i = 0; i < mgmt->k - 1; i++) {
            frame->accessTimes[i] = frame->accessTimes[i + 1];
        }
        frame->accessTimes[mgmt->k - 1] = mgmt->globalTime;
    }
    return RC_OK;
}

/** 
* @brief select a victim frame in the buffer pool according to the replacement strategy.
* @param bm, input value, a buffer pool structure pointer
* @return int, the index of the victim frame in the buffer pool
*/
static int selectReplacementFrame(BM_BufferPool *bm) {
    BM_MgmtData *mgmt = (BM_MgmtData *)bm->mgmtData;
    if (mgmt == NULL) 
        THROW(RC_UNVALID_HANDLE, "selectReplacementFrame: bm->mgmtData == NULL");

    int victimIndex = -1;
    int numPages = bm->numPages;

    // find all frames with fixCount=0
    int *candidates = malloc(numPages * sizeof(int));
    int candiCount = 0;
    for (int i = 0; i < numPages; i++) {
        if (mgmt->frames[i].fixCount == 0) {
            candidates[candiCount++] = i;
        }
    }
    if (candiCount == 0) {
        free(candidates);
        THROW(RC_UNVALID_HANDLE, "No available victim (all frames are pinned)");
    }

    // select a victim from candidates to replace
    switch (bm->strategy) {
        case RS_FIFO: {
            // traverse all candidates to find the ealiest frame
            victimIndex = candidates[0];
            unsigned int earliestTime = mgmt->frames[victimIndex].enterCounter;
            for (int j = 0; j < candiCount; j++) {
                int currIdx = candidates[j];
                if (mgmt->frames[currIdx].enterCounter < earliestTime) {
                    earliestTime = mgmt->frames[currIdx].enterCounter;
                    victimIndex = currIdx;
                }
            }
            break;
        }

        case RS_LRU: {
            victimIndex = candidates[0];
            unsigned int earliestAccess = mgmt->frames[victimIndex].lastAccessCounter;
            for (int j = 0; j < candiCount; j++) {
                int currIdx = candidates[j];
                if (mgmt->frames[currIdx].lastAccessCounter < earliestAccess) {
                    earliestAccess = mgmt->frames[currIdx].lastAccessCounter;
                    victimIndex = currIdx;
                }
            }
            break;
        }

        case RS_CLOCK: {
            // CLOCK：从当前clockHand开始寻找clockBit=0的帧
            int start = mgmt->clockHand;
                while (1) {
                    int currIdx = mgmt->clockHand;
                    // 移动时钟指针（循环）
                    mgmt->clockHand = (mgmt->clockHand + 1) % numPages;
                    // 检查当前帧是否为候选（fixCount=0）
                    if (mgmt->frames[currIdx].fixCount == 0) {
                        if (mgmt->frames[currIdx].clockBit == 0) {
                            victimIndex = currIdx;
                            break;
                        } else {
                            // 重置引用位，继续寻找
                            mgmt->frames[currIdx].clockBit = 0;
                        }
                    }
                    // 防止死循环（理论上不会触发，因为已有候选帧）
                    if (mgmt->clockHand == start) break;
                }
            break;
        }
        case RS_LFU: {
            // LFU：选择引用计数最少的帧
            victimIndex = candidates[0];
                int minRefCount = mgmt->frames[victimIndex].refCount;
                // 寻找引用计数最小的帧
                for (int j = 1; j < candiCount; j++) {
                    int currIdx = candidates[j];
                    if (mgmt->frames[currIdx].refCount < minRefCount) {
                        minRefCount = mgmt->frames[currIdx].refCount;
                        victimIndex = currIdx;
                    }
                }
            break;
        }
        case RS_LRU_K: {
            // LRU-K：选择第K次最近访问时间最早的帧
            victimIndex = -1;
            unsigned long int minKthTime = ULONG_MAX;

            for (int j = 0; j < bm->numPages; j++) {
                int currIdx = j;
                Frame *frame = &mgmt->frames[currIdx];
                
                unsigned long int kthTime;
                if (frame->accessCount < mgmt->k) {
                    // 访问次数不足K次，使用首次访问时间作为判断依据
                    kthTime = frame->accessTimes[0];
                } else {
                    // 访问次数达到K次，使用第K次访问时间
                    kthTime = frame->accessTimes[mgmt->k - 1];
                }
                
                // 寻找第K次访问时间最早的页面
                if (kthTime < minKthTime) {
                    minKthTime = kthTime;
                    victimIndex = currIdx;
                }
            }
            break;
        }
        default:

            THROW(-1, "Unsupported replacement strategy");
    }

    return victimIndex;
}

/** 
* @brief flush a frame to disk by frame index
* @param bm, input value, a buffer pool structure pointer
* @param frameIdx, input value, frame index to flush
* @return RC, return code
*/
static RC  flushFrame(BM_BufferPool *bm, int frameIdx) {
    BM_MgmtData *mgmt = (BM_MgmtData *)bm->mgmtData;
    if (mgmt == NULL) THROW(RC_UNVALID_HANDLE, "flushFrame: bm->mgmtData == NULL");

    if (frameIdx < 0) 
        THROW(RC_UNVALID_HANDLE, "flushFrame: invalid frame index");


    Frame *frame = &mgmt->frames[frameIdx];
    if (frame->pageHandle.pageNum == NO_PAGE) 
        return RC_OK;

    if (frame->isDirty) {
        // write back the dirty frame to pages file
        DEBUG_PRINT("the frame %d is dirty, write back to pages file\n", frameIdx); // only for debug

        // 检查文件句柄是否有效
        if (&mgmt->fileHandle == NULL) {
            // 文件句柄已无效，无法写回脏页，清除脏位并记录警告
            fprintf(stderr, "Warning: Cannot write back dirty frame %d, file handle is invalid\n", frameIdx);
            frame->isDirty = false;
            return RC_OK;
        }

        mgmt->numWriteIO++;
        SM_FileHandle *fh = &mgmt->fileHandle;
        CHECK(writeBlock(frame->pageHandle.pageNum, fh, (char *)frame->pageHandle.data));

        frame->isDirty = false; // clear dirty bit
        DEBUG_PRINT("the frame %d is flushed\n", frameIdx); // only for debug
    }
    else {
        DEBUG_PRINT("the frame %d is not dirty, do nothing\n", frameIdx); // only for debug
    }
    return RC_OK;
}

/**
 * @brief replace a frame by frame index: flush if dirty, then clear metadata
 * @param bm, input value, a buffer pool structure pointer
 * @param frameIdx, input value, frame index to replace
 * @return RC, return code
 */
static RC replaceFrame(BM_BufferPool *bm, int frameIdx) {
    // flush the frame if dirty
    CHECK(flushFrame(bm, frameIdx));

    BM_MgmtData *mgmt = (BM_MgmtData *)bm->mgmtData;
    Frame *frame = &mgmt->frames[frameIdx];
    if (bm->strategy == RS_LRU_K) {
        memset(frame->accessTimes, 0, mgmt->k * sizeof(unsigned long int));
        frame->accessCount = 0;
    }
    // clear metadata (only do this when replacing)
    frame->pageHandle.pageNum = NO_PAGE;
    frame->fixCount = 0;
    frame->refCount = 0;
    frame->clockBit = 0;
    memset(frame->pageHandle.data, 0, PAGE_SIZE);

    return RC_OK;
}
/*----------------------functions for manipulating buffer pool ----------------------*/
/** 
* @brief create and initialize the buffer pool
* @param bm, input value, a buffer pool structure pointer
* @param pageFileName, input value, page file name
* @param numPages, input value, number of pages in the buffer pool
* @param strategy, input value, replacement strategy
* @param stratData, input value, strategy data
* @return RC, return code
*/
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
                 const int numPages, ReplacementStrategy strategy, void *stratData) {
    
    // initialize buffer pool basic information
    bm->pageFile = (char *)malloc(strlen(pageFileName) + 1); 
    if (bm->pageFile == NULL) THROW(RC_MEMORY_ALLOC_FAILED, "Memory allocation failed for pageFile");
    
    strcpy(bm->pageFile, pageFileName); // copy page file name
    bm->numPages = numPages; // set number of pages
    bm->strategy = strategy; // set replacement strategy
    
    gLoadCounter = 0; // initialize global load counter
    gAccessCounter = 0; // initialize access counter

    // initialize buffer pool meta data
    BM_MgmtData *mgmt = (BM_MgmtData *)malloc(sizeof(BM_MgmtData)); 
    if (mgmt == NULL) THROW(RC_MEMORY_ALLOC_FAILED, "Memory allocation failed in initBufferPool() for BM_MgmtData");
    mgmt->frames = (Frame *)calloc(numPages, sizeof(Frame)); // allcate frames matadata
    if (mgmt->frames == NULL) THROW(RC_MEMORY_ALLOC_FAILED, "Memory allocation failed in initBufferPool() for frames");
    
    mgmt->numReadIO = 0;
    mgmt->numWriteIO = 0;
    mgmt->clockHand = 0;
    mgmt->k = (strategy == RS_LRU_K) ? (stratData ? *(int *)stratData : 2) : 0; // set k for LRU-K
    mgmt->globalTime = 0;
    // initialize frames metadata
    for (int i = 0; i < numPages; i++) {
        mgmt->frames[i].isDirty = false;
        mgmt->frames[i].fixCount = 0;
        mgmt->frames[i].enterCounter = 0;
        mgmt->frames[i].lastAccessCounter = 0;
        mgmt->frames[i].refCount = 0;
        mgmt->frames[i].clockBit = 0;
        mgmt->frames[i].pageHandle.pageNum = NO_PAGE; // indicate frame is free
        mgmt->frames[i].pageHandle.data = (char *)malloc(PAGE_SIZE); // allcate real frame for page data
        if (mgmt->frames[i].pageHandle.data == NULL) THROW(RC_MEMORY_ALLOC_FAILED, "Memory allocation failed in initBufferPool() for frame data");
        if (strategy == RS_LRU_K) {
            mgmt->frames[i].accessTimes = (long unsigned int *)malloc(mgmt->k * sizeof(long unsigned int));
            mgmt->frames[i].accessCount = 0;
        }
    }

    // open the page file
    if (openPageFile(pageFileName, &mgmt->fileHandle) != RC_OK) {
        // file does not exist, throw error
        THROW(RC_FILE_NOT_FOUND, "Page file not found");
    }

    bm->mgmtData = mgmt;
#ifdef DEBUG
    showBufferPool(bm);// show buffer pool meta data for debug
    showFrames(bm); // show frames meta data for debug
#endif
    return RC_OK;
}

/** 
* @brief flush all dirty pages to disk, close the page file, release resources
* @param bm, input value, a buffer pool structure pointer
* @return RC, return code
*/
RC shutdownBufferPool(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) THROW(RC_UNVALID_HANDLE, "shutdownBufferPool: bm->mgmtData == NULL");
    BM_MgmtData *mgmt = (BM_MgmtData *)bm->mgmtData; // recode buffer pool meta data
    RC rc = RC_OK;
    // flush all dirty pages to disk
    rc=forceFlushPool(bm);
    if(rc != RC_OK)
    {
        DEBUG_PRINT("Warning: forceFlushPool failed\n");
    }
    
    // close the page file
    rc=closePageFile(&mgmt->fileHandle);
    if(rc != RC_OK)
    {
        DEBUG_PRINT("Warning: closePageFile failed\n");
    }
    
    // release resources
    // 释放pageHandle.data和accessTimes（只循环一次）
    for (int i = 0; i < bm->numPages; i++) {
        if (mgmt->frames[i].pageHandle.data != NULL) {
            free(mgmt->frames[i].pageHandle.data);
            mgmt->frames[i].pageHandle.data = NULL;
        }

        // 不管策略是什么，都检查并释放accessTimes
        // 这段代码应该放在外层循环内，但不能嵌套在另一个循环中
        if (mgmt->frames[i].accessTimes != NULL) {
            free(mgmt->frames[i].accessTimes);
            mgmt->frames[i].accessTimes = NULL;
            mgmt->frames[i].accessCount = 0;
        }
    }
    // 释放其他资源
    if(mgmt->frames != NULL)
    {
        free(mgmt->frames);
        mgmt->frames = NULL;
    }
    if(mgmt != NULL)
    {
        free(mgmt);
        mgmt = NULL;
    }

    if(bm->pageFile != NULL)
    {
        free(bm->pageFile);
        bm->pageFile = NULL;
    }
    bm->mgmtData = NULL;
    
    return RC_OK;
}

/** 
* @brief flush all dirty pages in the buffer pool to page file
* @param bm, input value, a buffer pool structure pointer
* @return RC, return code
*/
RC forceFlushPool(BM_BufferPool *const bm) {
    if (bm == NULL) THROW(RC_UNVALID_HANDLE, "forceFlushPool: bm == NULL");
    if (bm->mgmtData == NULL) THROW(RC_UNVALID_HANDLE, "forceFlushPool: mgmtData is NULL");
    
    // BM_MgmtData *mgmt = (BM_MgmtData *)bm->mgmtData;
    // 遍历所有帧的索引（0到numPages-1），直接刷新每个帧
    for (int frameIdx = 0; frameIdx < bm->numPages; frameIdx++) {
        CHECK(flushFrame(bm, frameIdx));
    }
    return RC_OK;
}

/** 
* @brief mark a page as dirty
* @param bm, input value, a buffer pool structure pointer
* @param page, input value, a page handle structure pointer
* @return RC, return code
*/
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {

    if (bm == NULL || bm->mgmtData == NULL || page == NULL) THROW(RC_UNVALID_HANDLE, "markDirty: Invalid buffer pool or page handle");

    int frameIdx = getFrameIndex(bm, page->pageNum);
    if (frameIdx == -1) THROW(RC_UNVALID_HANDLE, "Can not mark page as dirty, Page not in buffer pool");

    BM_MgmtData *mgmt = (BM_MgmtData *)bm->mgmtData;
    mgmt->frames[frameIdx].isDirty = true;
    return RC_OK;
}

/** 
* @brief release a frame in the buffer pool, if needed, write back to page file.
* @param bm, input value, a buffer pool structure pointer
* @param page, input value, a page handle structure pointer
* @return RC, return code
*/
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {

    if (bm == NULL || bm->mgmtData == NULL || page == NULL) THROW(RC_FILE_HANDLE_NOT_INIT, "Invalid buffer pool or page handle");

    int frameIdx = getFrameIndex(bm, page->pageNum);
    if (frameIdx == -1) THROW(RC_READ_NON_EXISTING_PAGE, "Page not in buffer pool");

    BM_MgmtData *mgmt = (BM_MgmtData *)bm->mgmtData;
    if (mgmt->frames[frameIdx].fixCount > 0) 
        mgmt->frames[frameIdx].fixCount--;

    gAccessCounter++; // increment global access counter every time a page is accessed

    switch(bm->strategy)
    {
        case RS_LFU: {
            mgmt->frames[frameIdx].refCount++;  // 访问时递增引用计数
            break;
        }
        case RS_LRU:{
            mgmt->frames[frameIdx].lastAccessCounter = gAccessCounter;
            break;
        }
        case RS_LRU_K:{
            recordAccess(bm, frameIdx);
            break;
        }
        case RS_CLOCK:{
            mgmt->frames[frameIdx].clockBit = 1;
            break;
        }
        default:
            DEBUG_PRINT("Can not support %d stratagy\n",bm->strategy);
    }
    return RC_OK;
}

/** 
* @brief write back a page to pages file
* @param bm, input value, a buffer pool structure pointer
* @param page, input value, a page handle structure pointer
* @return RC, return code
*/
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {

    if (bm == NULL || bm->mgmtData == NULL || page == NULL) THROW(RC_FILE_HANDLE_NOT_INIT, "Invalid buffer pool or page handle");
    
    int frameIdx = getFrameIndex(bm, page->pageNum);
    if (frameIdx < 0) THROW(RC_READ_NON_EXISTING_PAGE, "Page not in buffer pool");
    
    return flushFrame(bm, frameIdx);
}

/** 
* @brief load a page to buffer pool frame, if frame is existing, increase fix count...
* @param bm, input value, a buffer pool structure pointer
* @param page, output value, a page handle structure pointer
* @param pageNum, input value, a page number
* @return RC, return code
*/
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {

    if (bm == NULL || bm->mgmtData == NULL || page == NULL || pageNum < 0) 
        THROW(RC_FILE_HANDLE_NOT_INIT, "Invalid buffer pool, page handle or page number");

    BM_MgmtData *mgmt = (BM_MgmtData *)bm->mgmtData;

    int frameIdx = getFrameIndex(bm, pageNum);

    gAccessCounter++; // increment global access counter every time a page is accessed

    // if the page is already in the buffer pool
    if (frameIdx >= 0) {
        // DEBUG_PRINT("the page %d is already in the buffer pool\n", pageNum); // only for debug

        mgmt->frames[frameIdx].fixCount++; // increase fix count
        mgmt->frames[frameIdx].lastAccessCounter = gAccessCounter; // update last access time
        mgmt->frames[frameIdx].refCount++; // increase ref count
        mgmt->frames[frameIdx].clockBit = 1; // set clock bit
        
        page->pageNum = pageNum;
        page->data = mgmt->frames[frameIdx].pageHandle.data;
    }
    else{
        // if the page is not in the buffer pool, find a free frame or select a victim frame to replace
        // DEBUG_PRINT("the page %d is not in the buffer pool, find a free frame or select a victim frame to replace\n", pageNum); // only for debug
        
        gLoadCounter++; // increment global load counter every time a page is loaded
        
        frameIdx = findFreeFrame(bm); // find a free frame
        if (frameIdx == -1) {
            // if no free frame, select a victim frame to replace
            frameIdx = selectReplacementFrame(bm);
            DEBUG_PRINT("no free frame, select a victim frame %d to replace\n", frameIdx); // only for debug
            if (frameIdx == -1) 
                THROW(RC_PAGE_NOT_FOUND, "No victim frame found");

            CHECK(replaceFrame(bm, frameIdx)); // replace the victim frame if dirty
        }

        DEBUG_PRINT("ensure the page %d exists in the page file\n", pageNum); // only for debug
        // ensure the page exists in the page file
        if (pageNum > mgmt->fileHandle.totalNumPages - 1) 
        {
            int pagesToAdd = pageNum - (mgmt->fileHandle.totalNumPages - 1);
            for (int i = 0; i < pagesToAdd; i++) {
                CHECK(appendEmptyBlock(&mgmt->fileHandle));
            }
        }

        DEBUG_PRINT("read the page %d from pages file to frame %d\n", pageNum, frameIdx); // only for debug
        // read the page from pages file
        SM_PageHandle pageData = (SM_PageHandle)malloc(PAGE_SIZE);
        if (pageData == NULL) 
            THROW(RC_MEMORY_ALLOC_FAILED, "Memory allocation failed in pinPage() for pageData");

        mgmt->numReadIO++;
        CHECK(readBlock(pageNum, &mgmt->fileHandle, pageData));

        // DEBUG_PRINT("reset the frame metadata\n"); // only for debug
        // update frame metadata
        Frame *frame = &mgmt->frames[frameIdx];
        frame->pageHandle.pageNum = pageNum;
        frame->isDirty = false;
        frame->fixCount = 1;

        memcpy(frame->pageHandle.data, pageData, PAGE_SIZE); // 复制页面数据
        free(pageData);
        pageData = NULL;

        // according to the replacement strategy to update access info
        switch (bm->strategy) {
            case RS_FIFO:
                // FIFO: 
                frame->enterCounter = gLoadCounter;
                break;
            case RS_LRU:
                frame->lastAccessCounter = gAccessCounter; // 更新最近访问时间
                break;
            case RS_CLOCK:
                frame->clockBit = 1; // 标记为被引用
                break;
            case RS_LFU:
                frame->refCount = 1; // 增加引用计数
                break;
            case RS_LRU_K: 
                // 更新访问历史（保留最近k次访问）
                recordAccess(bm, frameIdx);
                break;

            default:
                break;
        }

        // update page handle
        page->pageNum = pageNum;
        page->data = frame->pageHandle.data;

        DEBUG_PRINT("the page %d is pinned\n", pageNum); // only for debug
    }

    return RC_OK;
}

/** 
* @brief get the contents of all frames in the buffer pool
* @param bm, input value, a buffer pool structure pointer
* @return PageNumber *, return a page number array
*/
PageNumber *getFrameContents(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return NULL;

    BM_MgmtData *mgmt = (BM_MgmtData *)bm->mgmtData;
    PageNumber *contents = (PageNumber *)malloc(bm->numPages * sizeof(PageNumber));
    for (int i = 0; i < bm->numPages; i++) {
        contents[i] = mgmt->frames[i].pageHandle.pageNum;
    }
    return contents;
}

/** 
* @brief get the dirty flags of all frames in the buffer pool
* @param bm, input value, a buffer pool structure pointer
* @return bool *, return a boolean array
*/
bool *getDirtyFlags(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return NULL;

    BM_MgmtData *mgmt = (BM_MgmtData *)bm->mgmtData;
    bool *dirtyFlags = (bool *)malloc(bm->numPages * sizeof(bool));
    for (int i = 0; i < bm->numPages; i++) {
        dirtyFlags[i] = mgmt->frames[i].isDirty;
    }
    return dirtyFlags;
}

/** 
* @brief get the fix counts of all frames in the buffer pool
* @param bm, input value, a buffer pool structure pointer
* @return int *, return an integer array
*/
int *getFixCounts(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return NULL;

    BM_MgmtData *mgmt = (BM_MgmtData *)bm->mgmtData;
    int *fixCounts = (int *)malloc(bm->numPages * sizeof(int));
    for (int i = 0; i < bm->numPages; i++) {
        fixCounts[i] = mgmt->frames[i].fixCount;
    }
    return fixCounts;
}

/** 
* @brief get the number of read IO operations
* @param bm, input value, a buffer pool structure pointer
* @return int, return the number of read IO operations
*/
int getNumReadIO(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return -1;
    return ((BM_MgmtData *)bm->mgmtData)->numReadIO;
}

/** 
* @brief get the number of write IO operations
* @param bm, input value, a buffer pool structure pointer
* @return int, return the number of write IO operations
*/
int getNumWriteIO(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return -1;
    return ((BM_MgmtData *)bm->mgmtData)->numWriteIO;
}