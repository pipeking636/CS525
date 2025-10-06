#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef SIMULATE
#include <unistd.h>
#include <time.h>
#endif
/*----------------------macros----------------------*/
#ifdef SIMULATE
#define LATENCY_LOW 5
#define LATENCY_HIGH 20
#endif

#ifdef DEBUG // define this macro from makefile to enable debug print
    #define DEBUG_PRINT(format, ...) printf(format, ##__VA_ARGS__)
#else
    #define DEBUG_PRINT(format, ...)
#endif
/*----------------------global virables----------------------*/
// create a zero page in memory to avoid efficiency issues by dynamic memory allocation
// reduce the  risk of memory leakage
const unsigned char ZeroPage[PAGE_SIZE]={0};
#ifdef SIMULATE
static int totalLatency = 0;
#endif

/*----------------------local auxiliary functions----------------------*/
#ifdef SIMULATE
static int latency(void)
{
    int latency = LATENCY_LOW + (rand() % (LATENCY_HIGH - LATENCY_LOW + 1));
    usleep(latency * 1000); // sleep for latency microseconds
    totalLatency += latency;
    return latency;
}

int getTotalLatency(void)
{
    return totalLatency;
}

void resetTotalLatency(void)
{
    totalLatency = 0;
}
#endif

/*----------------------functions for manipulating page files----------------------*/
/** 
* @brief initialize storage manager
* @param void
* @return void 
*/
void initStorageManager (void)
{
#ifdef SIMULATE
    srand(time(NULL)); // seed random number generator
#endif
    //do nothing just show log
    printf("page size setting to %d\n", PAGE_SIZE);
    printf("Storage Manager initialized !\n");
}

/** 
* @brief create a page file
* @param fileName, input value, a string pointer to string of file name 
* @return error code
*/
RC createPageFile (char *fileName)
{
    // check file name is valid or not
    if (fileName == NULL) 
        return RC_FILE_NOT_FOUND;

    // uses file system function to create a file
    FILE *fp = fopen(fileName, "w+b");
    if (fp == NULL) 
        return RC_FILE_NOT_FOUND;

    // write one zero page to file
    size_t written = fwrite((void *)ZeroPage, 1, PAGE_SIZE, fp);
    fclose(fp);

#ifdef SIMULATE
    printf("%s(): latency %d\n", __func__, latency());
#endif

    if(written != PAGE_SIZE) {
        return RC_WRITE_FAILED;
    }
    else {
        return RC_OK;
    }
}

/** 
* @brief open a page file
* @param fileName, input value, a string pointer to string of file name
* @param fHandle, output value, a storage manager file structure pointer
* @return error code
*/
RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
    long size = 0;
    int totalPages = 0;
    // check file name and file handle are valid or not
    if (fileName == NULL || fHandle == NULL) 
        return RC_FILE_NOT_FOUND;
    // open file
    FILE *fp = fopen(fileName, "r+b");
    if (fp == NULL) 
        return RC_FILE_NOT_FOUND;

    // get file size
    if (fseek(fp, 0L, SEEK_END) != 0) {
        fclose(fp);
        return RC_FILE_NOT_FOUND;
    }
    size = ftell(fp);
    if (size < 0) {
        fclose(fp);
        return RC_FILE_NOT_FOUND;
    }
    totalPages = (int)(size / PAGE_SIZE);

    // fill the file handle values
    fHandle->fileName = fileName;
    fHandle->totalNumPages = totalPages;
    fHandle->curPagePos = 0;
    fHandle->mgmtInfo = fp;

    DEBUG_PRINT("open page file %s, total pages %d\n", fileName, totalPages); // only for debug
    /* set position to beginning */
    fseek(fp, 0L, SEEK_SET);    
    return RC_OK;
}

/** 
* @brief close a page file
* @param fHandle, input value, a storage manager file structure pointer. This value would be updated.
* @return error code
*/
RC closePageFile (SM_FileHandle *fHandle)
{
    // check file handle is valid or not
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) 
        return RC_FILE_HANDLE_NOT_INIT;

    // close the page file
    FILE *fp = (FILE *)fHandle->mgmtInfo;
    if (fclose(fp) != 0) 
        return RC_CLOSE_FAILED;

    // clear all data
    fHandle->fileName = NULL;
    fHandle->totalNumPages = 0;
    fHandle->curPagePos = 0;
    fHandle->mgmtInfo = NULL;
    return RC_OK;   
}

/** 
* @brief delete a page file
* @param fileName, input value, a string pointer to string of file name.
* @return error code
*/
RC destroyPageFile (char *fileName)
{
    // check file name is valid or not
    if (fileName == NULL) 
        return RC_FILE_NOT_FOUND;
    // remove this file
    if (remove(fileName) != 0) 
        return RC_FILE_NOT_FOUND;
#ifdef SIMULATE
    printf("%s(): latency %d\n", __func__, latency());
#endif
    return RC_OK;
}

/*----------------------functions for reading blocks from disc----------------------*/
/** 
* @brief read a page from page file
* @param pageNum, input value, read page number
* @param fHandle, input value, a c
* @param memPage, output value, a memory to store page data
* @return error code
*/
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    long offset=0;
    // check file handle is ok or not
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) 
        return RC_FILE_HANDLE_NOT_INIT;
    // check memory pointer is ok or not
    if (memPage == NULL) 
        return RC_FILE_HANDLE_NOT_INIT;
    // check page number is valid or not
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) 
        return RC_READ_NON_EXISTING_PAGE;
    // set read offset to file pointer
    FILE *fp = (FILE *)fHandle->mgmtInfo;
    offset = (long)pageNum * PAGE_SIZE;
    if (fseek(fp, offset, SEEK_SET) != 0) 
        return RC_READ_FAILED;
    // read one page and save data to memPage
    size_t read = fread(memPage, 1, PAGE_SIZE, fp);
    if (read < PAGE_SIZE) {
        // if partial read, fill rest with zeros
        memset(((char *)memPage) + read, 0, PAGE_SIZE - read);
    }
    // update current page number
    fHandle->curPagePos = pageNum;
#ifdef SIMULATE
    printf("%s(): latency %d\n", __func__, latency());
#endif
    return RC_OK;    
}

/** 
* @brief read a page from page file
* @param fHandle, input value, a storage manager file structure pointer
* @return int, current page number or -1
*/
int getBlockPos (SM_FileHandle *fHandle)
{   
    // check file handle is ok or not
    if (fHandle == NULL) 
        return -1;
    // just return the current page value value of storage manager file structure
    return fHandle->curPagePos;
}

/** 
* @brief read the first page from page file
* @param fHandle, input value, a storage manager file structure pointer
* @param memPage, output value, a memory to store page data
* @return error code
*/
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // just call the read block function to get the first page
    return readBlock(0, fHandle, memPage);
}

/** 
* @brief read the previous page from page file
* @param fHandle, input value, a storage manager file structure pointer
* @param memPage, output value, a memory to store page data
* @return error code
*/
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // check file handle is ok or not
    if (fHandle == NULL) 
        return RC_FILE_HANDLE_NOT_INIT;

    // get previous page from handle
    int prevPage = fHandle->curPagePos - 1;

    // check the previous page is valid or not
    if (prevPage < 0) 
        return RC_READ_NON_EXISTING_PAGE;
    
    //just call the read block function to get the previous page 
    return readBlock(prevPage, fHandle, memPage);
}

/** 
* @brief read the current page from page file
* @param fHandle, input value, a storage manager file structure pointer
* @param memPage, output value, a memory to store page data
* @return error code
*/
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // check file handle is ok or not
    if (fHandle == NULL) 
        return RC_FILE_HANDLE_NOT_INIT;
    // just call read block to read current page
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

/** 
* @brief read the next page from page file
* @param fHandle, input value, a storage manager file structure pointer
* @param memPage, output value, a memory to store page data
* @return error code
*/
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // check the file handle is ok or not
    if (fHandle == NULL) 
        return RC_FILE_HANDLE_NOT_INIT;

    // get the next page from handle
    int nextPage = fHandle->curPagePos + 1;
    
    if (nextPage >= fHandle->totalNumPages) 
        return RC_READ_NON_EXISTING_PAGE;

    return readBlock(nextPage, fHandle, memPage);
}

/** 
* @brief read the last page from page file
* @param fHandle, input value, a storage manager file structure pointer
* @param memPage, output value, a memory to store page data
* @return error code
*/
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // check the file handle is ok or not
    if (fHandle == NULL) 
        return RC_FILE_HANDLE_NOT_INIT;
    // check the total pages number is valid or not
    if (fHandle->totalNumPages == 0) 
        return RC_READ_NON_EXISTING_PAGE;
    // just call the read block function to get the last page
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

/*----------------------functions for writing blocks to a page file----------------------*/
/** 
* @brief overwrite or extend a page to page file
* @param pageNum, input value, write page to where
* @param fHandle, input value, a storage manager file structure pointer
* @param memPage, input value, a memory pointer of page data
* @return error code
*/
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    long offset=0;
    // check input parameters are ok or not
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) 
        return RC_FILE_HANDLE_NOT_INIT;
    if (memPage == NULL) 
        return RC_WRITE_FAILED;
    if (pageNum < 0) 
        return RC_INVALID_PAGE_NUM;

    // ensure capacity so pageNum exists
    RC rc = ensureCapacity(pageNum + 1, fHandle);
    if (rc != RC_OK) return rc;

    // calculate the offset
    FILE *fp = (FILE *)fHandle->mgmtInfo;
    offset = (long)pageNum * PAGE_SIZE;
    
    //set file pointer
    if (fseek(fp, offset, SEEK_SET) != 0) 
        return RC_WRITE_FAILED;

    // write page data to file
    size_t written = fwrite(memPage, 1, PAGE_SIZE, fp);
    if (written != PAGE_SIZE) 
        return RC_WRITE_FAILED;

    // keep all data written to file
    fflush(fp);

    // update current page value
    fHandle->curPagePos = pageNum;
#ifdef SIMULATE
    printf("%s(): latency %d\n", __func__, latency());
#endif
    return RC_OK;    
}

/** 
* @brief overwrite current page of page file
* @param fHandle, input value, a storage manager file structure pointer
* @param memPage, input value, a memory pointer of page data
* @return error code
*/
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // check file handle is ok or not
    if (fHandle == NULL) 
        return RC_FILE_HANDLE_NOT_INIT;
    // just call the write block function to write page data
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

/** 
* @brief append one zero page to page file
* @param fHandle, input value, a storage manager file structure pointer
* @return error code
*/
RC appendEmptyBlock (SM_FileHandle *fHandle)
{
    // check file handle is ok or not
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) 
        return RC_FILE_HANDLE_NOT_INIT;

    // set file pointer to end of the file
    FILE *fp = (FILE *)fHandle->mgmtInfo;
    if (fseek(fp, 0L, SEEK_END) != 0) 
        return RC_WRITE_FAILED;

    // write the zero page to file
    size_t written = fwrite((void *)ZeroPage, 1, PAGE_SIZE, fp);
    if (written != PAGE_SIZE) 
        return RC_WRITE_FAILED;

    // keep all data written to file
    fflush(fp);

    // update total number of pages and current page number
    fHandle->curPagePos = fHandle->totalNumPages;
    fHandle->totalNumPages += 1;
#ifdef SIMULATE
    printf("%s(): latency %d\n", __func__, latency());
#endif
    return RC_OK;    
}
/** 
* @brief append zero pages to page file
* @param numberOfPages, input value, target number of pages should be extended
* @param fHandle, input value, a storage manager file structure pointer
* @return error code
*/
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
    // check input parameters is valid or not
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) 
        return RC_FILE_HANDLE_NOT_INIT;
    if (numberOfPages <= 0) 
        return RC_OK;

    // extend empty page one by one
    while (fHandle->totalNumPages < numberOfPages) {
        RC rc = appendEmptyBlock(fHandle);
        if (rc != RC_OK) 
            return rc;
    }
    return RC_OK;
}

// RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
//     if (fHandle == NULL || fHandle->mgmtInfo == NULL) 
//         return RC_FILE_HANDLE_NOT_INIT;
//     if (numberOfPages <= 0 || numberOfPages <= fHandle->totalNumPages) 
//         return RC_OK;

//     FILE *fp = (FILE *)fHandle->mgmtInfo;
//     int pagesToAdd = numberOfPages - fHandle->totalNumPages;

//     // 移动到文件末尾
//     if (fseek(fp, 0L, SEEK_END) != 0) 
//         return RC_WRITE_FAILED;

//     // 批量写入空页（减少I/O操作）
//     for (int i = 0; i < pagesToAdd; i++) {
//         size_t written = fwrite(ZeroPage, 1, PAGE_SIZE, fp);
//         if (written != PAGE_SIZE) 
//             return RC_WRITE_FAILED;
//         fHandle->totalNumPages++;
//     }

//     fflush(fp);
//     return RC_OK;
// }