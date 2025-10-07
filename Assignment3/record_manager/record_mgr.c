#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "tables.h"
#include "dberror.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#ifdef DEBUG
    #define DEBUG_PRINT(format, ...) printf(format, ##__VA_ARGS__)
#else
    #define DEBUG_PRINT(format, ...)
#endif

#define DEFAULT_BUFFER_POOL_SIZE 10
#define MAX_ATTR_NUM 10

// 槽位目录项（每个槽位的元数据，存储在页头后的槽位目录中）
typedef struct SlotDirEntry {
    int offset;       // 记录在页内的起始偏移量（从页头结束位置开始计算）
    bool isValid;     // 槽位是否有效（false表示已删除，可复用）
} SlotDirEntry;

// 数据页的页头（每个数据页的元数据，位于页的起始位置）
typedef struct PageHeader {
    int slotDirOffset;   // 槽位目录的起始偏移量（从页首开始计算，页头本身占用固定大小）
    int slotCount;       // 总槽位数量（包括已使用和空闲）
    int freeSlotCount;   // 空闲槽位数量（可复用的已删除槽位）
    int nextFreePage;    // 空闲页链表中的下一页（-1表示无）
} PageHeader;

// 表信息（存储在第0页，描述表的全局元数据）
// TableInfo仅存Schema的原始参数（无指针，可安全序列化）
typedef struct TableInfo {
    char tableName[100];       // 表名
    int recordSize;            // 记录大小
    int numTuples;             // 总记录数
    int totalPages;            // 总页数
    int freePageListHead;      // 空闲页链表头

    // Schema的原始构建参数（核心！用于open时重建Schema）
    int schemaNumAttr;         // 属性数量
    DataType schemaDataTypes[10];  // 数据类型（假设最多10个属性，足够测试）
    int schemaTypeLength[10];      // 类型长度（仅字符串有效）
    int schemaKeySize;            // 主键属性数量
    int schemaKeyAttrs[10];       // 主键属性索引
    char schemaAttrNames[10][50]; // 属性名（每个最多50字符，足够测试）
} TableInfo;

// 表管理数据（RM_TableData的mgmtData实际类型）
typedef struct RM_TableMgmt {
    BM_BufferPool bufferPool;  // 关联的缓冲池
    SM_FileHandle fileHandle;  // 关联的存储管理器文件句柄
    TableInfo tableInfo;       // 缓存的表信息（从第0页读取）
    Schema *schema;            // 内存中重建的Schema（新增！）
    int numReadIO;             // 读IO统计
    int numWriteIO;            // 写IO统计
} RM_TableMgmt;

// // 扫描状态管理（RM_ScanHandle的mgmtData实际类型）
// typedef struct ScanMgmtData {
//     Expr *cond;                // 扫描条件表达式（NULL表示全表扫描）
//     int currentPage;           // 当前扫描的页号
//     int currentSlot;           // 当前页内扫描的槽位
//     bool isEnd;                // 扫描是否结束
//     // 缓存当前页的槽位目录（减少重复IO）
//     SlotDirEntry *slotDir;
//     int slotDirSize;
// } ScanMgmtData;

// ------------------------------
// Schema functions
// ------------------------------
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keyAttrs) {
    if (numAttr <= 0 || dataTypes == NULL) return NULL;
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    if (schema == NULL) return NULL;

    schema->numAttr = numAttr;
    // 分配属性名内存
    schema->attrNames = (char **)malloc(numAttr * sizeof(char *));
    for (int i = 0; i < numAttr; i++) {
        schema->attrNames[i] = (char *)malloc(strlen(attrNames[i]) + 1);
        strcpy(schema->attrNames[i], attrNames[i]);
    }
    // 分配数据类型内存
    schema->dataTypes = (DataType *)malloc(numAttr * sizeof(DataType));
    memcpy(schema->dataTypes, dataTypes, numAttr * sizeof(DataType));
    // 分配类型长度内存
    schema->typeLength = (int *)malloc(numAttr * sizeof(int));
    if (typeLength != NULL) memcpy(schema->typeLength, typeLength, numAttr * sizeof(int));
    else memset(schema->typeLength, 0, numAttr * sizeof(int));
    // 分配主键内存
    schema->keySize = keySize;
    schema->keyAttrs = (int *)malloc(keySize * sizeof(int));
    if (keyAttrs != NULL && keySize > 0) memcpy(schema->keyAttrs, keyAttrs, keySize * sizeof(int));
    else memset(schema->keyAttrs, 0, keySize * sizeof(int));

    return schema;
}

RC freeSchema(Schema *schema) {
    if (schema == NULL) return RC_OK;
    // 释放属性名
    for (int i = 0; i < schema->numAttr; i++) free(schema->attrNames[i]);
    free(schema->attrNames);
    // 释放其他动态内存
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);
    free(schema);
    return RC_OK;
}

int getRecordSize(Schema *schema) {
    if (schema == NULL) return 0;
    int size = 0;
    for (int i = 0; i < schema->numAttr; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT: size += sizeof(int); break;
            case DT_FLOAT: size += sizeof(float); break;
            case DT_STRING: size += schema->typeLength[i]; break;
            case DT_BOOL: size += sizeof(bool); break;
            default: break;
        }
    }
    return size;
}
// ------------------------------------
// page operation auxiliary functions
// ------------------------------------
// 辅助函数：初始化数据页
static RC initDataPage(BM_BufferPool *bp, BM_PageHandle *ph) {
    PageHeader *header = (PageHeader *)ph->data;
    header->slotDirOffset = sizeof(PageHeader); // 槽位目录紧跟页头
    header->slotCount = 0;
    header->freeSlotCount = 0;
    header->nextFreePage = -1;
    markDirty(bp, ph);
    return RC_OK;
}
// 辅助函数：在数据页中查找空闲槽位
static RC findFreeSlotInPage(BM_BufferPool *bp, BM_PageHandle *ph, int *slotNum) {
    PageHeader *header = (PageHeader *)ph->data;
    SlotDirEntry *slotDir = (SlotDirEntry *)(ph->data + header->slotDirOffset);
    
    // 先查找已删除但未复用的槽位
    for (int i = 0; i < header->slotCount; i++) {
        if (!slotDir[i].isValid) {
            *slotNum = i;
            header->freeSlotCount--;
            return RC_OK;
        }
    }
    
    // 计算页中剩余空间是否足够新增一个槽位和记录
    int recordSize = ((RM_TableMgmt *)bp->mgmtData)->tableInfo.recordSize;
    int pageDataAreaSize = PAGE_SIZE - header->slotDirOffset - (header->slotCount + 1) * sizeof(SlotDirEntry);
    int remainingFreeSpace = pageDataAreaSize - (header->slotCount * recordSize);
    
    if (remainingFreeSpace >= recordSize) {
        *slotNum = header->slotCount;
        header->slotCount++;
        return RC_OK;
    }
    
    return RC_RM_NO_MORE_SLOT;
}
// 辅助函数：在指定页和槽位插入记录
static RC insertRecordIntoSlot(BM_BufferPool *bp, BM_PageHandle *ph, int slotNum, Record *record, int recordSize) {
    PageHeader *header = (PageHeader *)ph->data;
    SlotDirEntry *slotDir = (SlotDirEntry *)(ph->data + header->slotDirOffset);
    
    // 计算记录的偏移量（从页底向上增长）
    int dataAreaStart = header->slotDirOffset + header->slotCount * sizeof(SlotDirEntry);
    int recordOffset = dataAreaStart + slotNum * recordSize;
    
    // 更新槽位目录
    slotDir[slotNum].offset = recordOffset;
    slotDir[slotNum].isValid = TRUE;
    
    // 复制记录数据
    memcpy(ph->data + recordOffset, record->data, recordSize);
    
    markDirty(bp, ph);
    return RC_OK;
}
// 辅助函数：从缓冲区获取页
// 修改getPageFromBuffer函数，确保正确处理页面固定
static RC getPageFromBuffer(BM_BufferPool *bp, BM_PageHandle *ph, PageNumber pageNum) {
    RC rc = pinPage(bp, ph, pageNum);
    if (rc != RC_OK) {
        DEBUG_PRINT("Failed to pin page %d: %s\n", pageNum, errorMessage(rc));
        return rc;
    }
    // 确保mgmtData不为空再增加IO计数
    if (bp->mgmtData != NULL) {
        ((RM_TableMgmt *)bp->mgmtData)->numReadIO++;
    }
    return RC_OK;
}
// 辅助函数：将页返回缓冲区
// 修改releasePageToBuffer函数，确保正确处理页面释放
static RC releasePageToBuffer(BM_BufferPool *bp, BM_PageHandle *ph, bool isDirty) {
    if (isDirty) {
        markDirty(bp, ph);
        // 确保mgmtData不为空再增加IO计数
        if (bp->mgmtData != NULL) {
            ((RM_TableMgmt *)bp->mgmtData)->numWriteIO++;
        }
    }
    RC rc = unpinPage(bp, ph);
    if (rc != RC_OK) {
        DEBUG_PRINT("Failed to unpin page %d: %s\n", ph->pageNum, errorMessage(rc));
    }
    return rc;
}

// table and manager
/**
 * @brief record manager initialization
 * 
 * @param mgmtData, pointer to the record manager management data, not used in this function
 * @return RC_OK
 */
RC initRecordManager (void *mgmtData)
{
    initStorageManager(); // 依赖存储管理器初始化
    return RC_OK;
}

/**
 * @brief record manager shutdown
 * 
 * @return RC_OK
 */
RC shutdownRecordManager ()
{
    return RC_OK;
}
/**
 * @brief create a table, write table info to page 0
 * 
 * @param name, name of the table
 * @param schema, schema of the table
 * @return RC_OK
 */
RC createTable (char *name, Schema *schema)
{
    if (name == NULL || schema == NULL) return RC_INVALID_PARAMS;

    // 限制属性数量（避免超出TableInfo的数组大小）
    if (schema->numAttr > MAX_ATTR_NUM) return RC_RM_TOO_MANY_ATTRS;

    // 1. 创建物理文件
    RC rc = createPageFile(name);
    if (rc != RC_OK) return rc;

    // 2. 初始化TableInfo（仅存Schema的原始参数，无指针）
    TableInfo tableInfo;
    // 表名（确保\0终止）
    strncpy(tableInfo.tableName, name, sizeof(tableInfo.tableName) - 1);
    tableInfo.tableName[sizeof(tableInfo.tableName) - 1] = '\0';
    // 基础信息
    tableInfo.recordSize = getRecordSize(schema);
    tableInfo.numTuples = 0;
    tableInfo.totalPages = 1;
    tableInfo.freePageListHead = -1;
    // Schema原始参数（复制值，无指针）
    tableInfo.schemaNumAttr = schema->numAttr;
    memcpy(tableInfo.schemaDataTypes, schema->dataTypes, schema->numAttr * sizeof(DataType));
    memcpy(tableInfo.schemaTypeLength, schema->typeLength, schema->numAttr * sizeof(int));
    tableInfo.schemaKeySize = schema->keySize;
    memcpy(tableInfo.schemaKeyAttrs, schema->keyAttrs, schema->keySize * sizeof(int));
    // 复制属性名（字符串直接存到数组，无指针）
    for (int i = 0; i < schema->numAttr; i++) {
        strncpy(tableInfo.schemaAttrNames[i], schema->attrNames[i], sizeof(tableInfo.schemaAttrNames[i]) - 1);
        tableInfo.schemaAttrNames[i][sizeof(tableInfo.schemaAttrNames[i]) - 1] = '\0';
    }

    // 3. 写入第0页（表信息页，全是值类型，可安全memcpy）
    SM_FileHandle fh;
    rc = openPageFile(name, &fh);
    if (rc != RC_OK) return rc;

    SM_PageHandle infoPage = (SM_PageHandle)malloc(PAGE_SIZE);
    memset(infoPage, 0, PAGE_SIZE);
    memcpy(infoPage, &tableInfo, sizeof(TableInfo));  // 无指针，安全！
    rc = writeBlock(0, &fh, infoPage);

    // 4. 清理
    free(infoPage);
    closePageFile(&fh);
    return rc;
}
/**
 * @brief open a table, read table info from page 0
 * 
 * @param rel, pointer to the table data
 * @param name, name of the table
 * @return RC_OK
 */
RC openTable (RM_TableData *rel, char *name)
{
    if (rel == NULL || name == NULL) return RC_INVALID_PARAMS;
    
    // 1. 初始化管理结构体
    RM_TableMgmt *mgmt = (RM_TableMgmt *)malloc(sizeof(RM_TableMgmt));
    if (mgmt == NULL) return RC_OUT_OF_MEMORY;

    // 2. 打开物理文件
    RC rc = openPageFile(name, &mgmt->fileHandle);
    if (rc != RC_OK) {
        free(mgmt);
        return rc;
    }

    // 3. 读取第0页的TableInfo（全是值类型，安全）
    SM_PageHandle infoPage = (SM_PageHandle)malloc(PAGE_SIZE);
    rc = readBlock(0, &mgmt->fileHandle, infoPage);
    if (rc != RC_OK) {
        closePageFile(&mgmt->fileHandle);
        free(mgmt);
        free(infoPage);
        return rc;
    }
    memcpy(&mgmt->tableInfo, infoPage, sizeof(TableInfo));
    free(infoPage);

    // 4. 用TableInfo的原始参数重建Schema（关键！无需反序列化）
    // 4.1 准备createSchema的参数（属性名数组）
    char **attrNames = (char **)malloc(mgmt->tableInfo.schemaNumAttr * sizeof(char *));
    if (attrNames == NULL) {  // 检查malloc结果
        closePageFile(&mgmt->fileHandle);
        free(mgmt);
        return RC_OUT_OF_MEMORY;
    }
    // 复制属性名到堆内存（不再直接指向mgmt的栈上数组）
    for (int i = 0; i < mgmt->tableInfo.schemaNumAttr; i++) {
        attrNames[i] = (char *)malloc(strlen(mgmt->tableInfo.schemaAttrNames[i]) + 1);
        strcpy(attrNames[i], mgmt->tableInfo.schemaAttrNames[i]);
    }
    // 4.2 调用createSchema重建
    mgmt->schema = createSchema(
        mgmt->tableInfo.schemaNumAttr,
        attrNames,
        mgmt->tableInfo.schemaDataTypes,
        mgmt->tableInfo.schemaTypeLength,
        mgmt->tableInfo.schemaKeySize,
        mgmt->tableInfo.schemaKeyAttrs
    );
    // 释放attrNames（内部字符串已被createSchema复制，无需保留）
    for (int i = 0; i < mgmt->tableInfo.schemaNumAttr; i++) {
        free(attrNames[i]);
    }
    free(attrNames);

    if (mgmt->schema == NULL) {
        closePageFile(&mgmt->fileHandle);
        free(mgmt);
        return RC_OUT_OF_MEMORY;
    }

    // 5. 初始化缓冲池
    rc = initBufferPool(&mgmt->bufferPool, name, DEFAULT_BUFFER_POOL_SIZE, RS_FIFO, NULL);
    if (rc != RC_OK) {
        freeSchema(mgmt->schema);
        closePageFile(&mgmt->fileHandle);
        free(mgmt);
        return rc;
    }

    // 6. 设置其他字段
    mgmt->numReadIO = 0;
    mgmt->numWriteIO = 0;

    // 7. 设置rel
    rel->name = name;
    rel->schema = mgmt->schema;
    rel->mgmtData = mgmt;

    return RC_OK;
}
/**
 * @brief close a table, release buffer pool
 * 
 * @param rel, pointer to the table data
 * @return RC_OK
 */
RC closeTable(RM_TableData *rel) {
    if (rel == NULL || rel->mgmtData == NULL) return RC_INVALID_PARAMS;

    // 先保存需要的值，因为mgmt可能在shutdownBufferPool中被释放
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData;

    // 1. 关闭缓冲池（此时frame 0已非脏页，无需刷写）
    RC rc = shutdownBufferPool(&mgmt->bufferPool);
    if (rc != RC_OK) {
        printf("Warning: shutdown buffer pool failed\n");
    }

    // 2. 关闭文件句柄
    rc = closePageFile(&mgmt->fileHandle);
    if (rc != RC_OK) {
        printf("Warning: close page file failed\n");
    }

    // 3. 释放Schema和管理结构体
    rc = freeSchema(mgmt->schema);
    if (rc != RC_OK) {
        printf("Warning: free schema failed\n");
    }

    // 4. 释放管理结构体
    free(mgmt);

    // 5. 清空rel中的指针
    rel->mgmtData = NULL;
    rel->schema = NULL;

    return RC_OK;
}
/**
 * @brief delete a table, release buffer pool
 * 
 * @param name, name of the table
 * @return RC_OK
 */
RC deleteTable (char *name)
{
    if (name == NULL) return RC_INVALID_PARAMS;
    return destroyPageFile(name);
}

/**
 * @brief get total number of pages in a table
 * 
 * @param rel, pointer to the table data
 * @return total number of pages
 */
int getTableTotalPages(RM_TableData *rel) {
    if (rel == NULL || rel->mgmtData == NULL) return -1;
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData;
    return mgmt->tableInfo.totalPages;
}

/**
 * @brief get record size in a table
 * 
 * @param rel, pointer to the table data
 * @return record size
 */
int getTableRecordSize(RM_TableData *rel) {
    if (rel == NULL || rel->mgmtData == NULL) return -1;
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData;
    return mgmt->tableInfo.recordSize;
}

/**
 * @brief get table name in a table
 * 
 * @param rel, pointer to the table data
 * @return table name, a copy of the table name need release after use
 */
char* getTableName(RM_TableData *rel) {
    if (rel == NULL || rel->mgmtData == NULL) return NULL;
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData;
    char *name = (char *)malloc(strlen(mgmt->tableInfo.tableName) + 1); // please free after use
    if (name == NULL) return NULL;
    strcpy(name, mgmt->tableInfo.tableName);
    return name;
}
/**
 * @brief get number of tuples in a table
 * 
 * @param rel, pointer to the table data
 * @return number of tuples
 */
int getNumTuples(RM_TableData *rel) {
    if (rel == NULL || rel->mgmtData == NULL) return -1;
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData;
    return mgmt->tableInfo.numTuples;
}

// handling records in a table
/**
 * @brief insert a record into a table
 * 
 * @param rel, input parameter, pointer to the table data
 * @param record, input parameter, pointer to the record
 * @return RC_OK
 */
RC insertRecord(RM_TableData *rel, Record *record) {
    if (rel == NULL || record == NULL || rel->mgmtData == NULL) 
        return RC_INVALID_PARAMS;
    
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData;
    BM_BufferPool *bp = &mgmt->bufferPool;
    int recordSize = mgmt->tableInfo.recordSize;
    RC rc = RC_OK;
    BM_PageHandle ph;
    
    // 1. 查找可用页面（优先使用空闲页链表中的页面）
    int pageNum = -1;
    if (mgmt->tableInfo.freePageListHead != -1) {
        // 使用空闲页链表中的页面
        pageNum = mgmt->tableInfo.freePageListHead;
        
        // 获取该页面
        rc = getPageFromBuffer(bp, &ph, pageNum);
        if (rc != RC_OK) {
            DEBUG_PRINT("insertRecord: Failed to get free page %d\n", pageNum);
            return rc;
        }
        
        // 更新空闲页链表头
        PageHeader *header = (PageHeader *)ph.data;
        mgmt->tableInfo.freePageListHead = header->nextFreePage;
        
        // 释放页面
        releasePageToBuffer(bp, &ph, TRUE);
    } else {
        // 创建新页面
        pageNum = mgmt->tableInfo.totalPages;
        mgmt->tableInfo.totalPages++;
    }
    
    // 2. 获取目标页面
    rc = getPageFromBuffer(bp, &ph, pageNum);
    if (rc != RC_OK) {
        DEBUG_PRINT("insertRecord: Failed to get page %d\n", pageNum);
        return rc;
    }
    
    // 3. 确保页面已初始化
    PageHeader *header = (PageHeader *)ph.data;
    if (header->slotCount == 0) {
        initDataPage(bp, &ph);
    }
    
    // 4. 查找空闲槽位
    int slotNum = -1;
    rc = findFreeSlotInPage(bp, &ph, &slotNum);
    if (rc != RC_OK) {
        // 没有空闲槽位，需要创建新页面
        releasePageToBuffer(bp, &ph, FALSE);
        pageNum = mgmt->tableInfo.totalPages;
        mgmt->tableInfo.totalPages++;
        
        rc = getPageFromBuffer(bp, &ph, pageNum);
        if (rc != RC_OK) {
            DEBUG_PRINT("insertRecord: Failed to get new page %d\n", pageNum);
            return rc;
        }
        
        initDataPage(bp,&ph);
        rc = findFreeSlotInPage(bp, &ph, &slotNum);
        if (rc != RC_OK) {
            releasePageToBuffer(bp, &ph, FALSE);
            DEBUG_PRINT("insertRecord: No free slot found in new page\n");
            return RC_RM_NO_MORE_TUPLES;
        }
    }
    
    // 5. 插入记录到槽位
    rc = insertRecordIntoSlot(bp, &ph, slotNum, record, recordSize);
    if (rc != RC_OK) {
        releasePageToBuffer(bp, &ph, FALSE);
        return rc;
    }
    
    // 6. 更新页面头信息
    header = (PageHeader *)ph.data;
    header->freeSlotCount--;
    
    // 7. 更新记录ID
    record->id.page = pageNum;
    record->id.slot = slotNum;
    
    // 8. 更新表信息
    mgmt->tableInfo.numTuples++;
    
    // 9. 更新第0页的表信息
    BM_PageHandle infoPage;
    rc = pinPage(bp, &infoPage, 0);
    if (rc == RC_OK) {
        memcpy(infoPage.data, &mgmt->tableInfo, sizeof(TableInfo));
        markDirty(bp, &infoPage);
        unpinPage(bp, &infoPage);
    }
    
    // 10. 释放页面
    releasePageToBuffer(bp, &ph, TRUE);
    
    return RC_OK;
}
/**
 * @brief get a record from a table
 * 
 * @param rel, input parameter, pointer to the table data
 * @param id, input parameter, RID of the record
 * @param record, output parameter, pointer to the record
 * @return RC_OK
 */
RC getRecord(RM_TableData *rel, RID id, Record *record) {
    if (rel == NULL || record == NULL || rel->mgmtData == NULL || 
        id.page < 1 || id.page >= ((RM_TableMgmt *)rel->mgmtData)->tableInfo.totalPages) 
        return RC_INVALID_PARAMS;
    
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData;
    BM_BufferPool *bp = &mgmt->bufferPool;
    BM_PageHandle ph;
    RC rc = RC_OK;
    int recordSize = mgmt->tableInfo.recordSize;
    
    // 1. 确保记录数据缓冲区已分配
    if (record->data == NULL) {
        record->data = malloc(recordSize);
        if (record->data == NULL) return RC_OUT_OF_MEMORY;
    }
    
    // 2. 从缓冲区获取页面
    rc = getPageFromBuffer(bp, &ph, id.page);
    if (rc != RC_OK) return rc;
    
    // 3. 检查页面结构
    PageHeader *header = (PageHeader *)ph.data;
    SlotDirEntry *slotDir = (SlotDirEntry *)(ph.data + header->slotDirOffset);
    
    // 4. 检查槽位有效性
    if (id.slot < 0 || id.slot >= header->slotCount || !slotDir[id.slot].isValid) {
        releasePageToBuffer(bp, &ph, FALSE);
        return RC_RM_NO_MORE_TUPLES;
    }
    
    // 5. 复制记录数据
    memcpy(record->data, ph.data + slotDir[id.slot].offset, recordSize);
    
    // 6. 设置记录的RID
    record->id = id;
    
    // 7. 释放页面
    releasePageToBuffer(bp, &ph, FALSE);
    
    return RC_OK;
}
/**
 * @brief delete a record from a table
 * 
 * @param rel, input parameter, pointer to the table data
 * @param id, input parameter, RID of the record
 * @return RC_OK
 */
RC deleteRecord(RM_TableData *rel, RID id) {
    if (rel == NULL || rel->mgmtData == NULL || 
        id.page < 1 || id.page >= ((RM_TableMgmt *)rel->mgmtData)->tableInfo.totalPages) 
        return RC_INVALID_PARAMS;
    
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData;
    BM_BufferPool *bp = &mgmt->bufferPool;
    BM_PageHandle ph;
    RC rc = RC_OK;
    
    // 1. 从缓冲区获取页面
    rc = getPageFromBuffer(bp, &ph, id.page);
    if (rc != RC_OK) return rc;
    
    // 2. 检查页面结构
    PageHeader *header = (PageHeader *)ph.data;
    SlotDirEntry *slotDir = (SlotDirEntry *)(ph.data + header->slotDirOffset);
    
    // 3. 检查槽位有效性
    if (id.slot < 0 || id.slot >= header->slotCount || !slotDir[id.slot].isValid) {
        releasePageToBuffer(bp, &ph, FALSE);
        return RC_RM_NO_MORE_TUPLES;
    }
    
    // 4. 标记槽位为无效
    slotDir[id.slot].isValid = FALSE;
    header->freeSlotCount++;
    
    // 5. 更新记录计数
    mgmt->tableInfo.numTuples--;
    
    // 6. 更新第0页的表信息
    BM_PageHandle infoPage;
    rc = pinPage(bp, &infoPage, 0);
    if (rc == RC_OK) {
        memcpy(infoPage.data, &mgmt->tableInfo, sizeof(TableInfo));
        markDirty(bp, &infoPage);
        unpinPage(bp, &infoPage);
    }
    
    // 7. 释放页面
    releasePageToBuffer(bp, &ph, TRUE);
    
    return RC_OK;
}
/**
 * @brief update a record in a table
 * 
 * @param rel, input parameter, pointer to the table data
 * @param record, input parameter, pointer to the record
 * @return RC_OK
 */
RC updateRecord(RM_TableData *rel, Record *record) {
    if (rel == NULL || record == NULL || rel->mgmtData == NULL || 
        record->id.page < 1 || record->id.page >= ((RM_TableMgmt *)rel->mgmtData)->tableInfo.totalPages) 
        return RC_INVALID_PARAMS;
    
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData;
    BM_BufferPool *bp = &mgmt->bufferPool;
    BM_PageHandle ph;
    RC rc = RC_OK;
    int recordSize = mgmt->tableInfo.recordSize;
    
    // 1. 从缓冲区获取页面
    rc = getPageFromBuffer(bp, &ph, record->id.page);
    if (rc != RC_OK) {
        DEBUG_PRINT("updateRecord: Failed to get page %d\n", record->id.page);
        return rc;
    }
    
    // 2. 检查页面结构
    PageHeader *header = (PageHeader *)ph.data;
    SlotDirEntry *slotDir = (SlotDirEntry *)(ph.data + header->slotDirOffset);
    
    // 3. 检查槽位有效性
    if (record->id.slot < 0 || record->id.slot >= header->slotCount || !slotDir[record->id.slot].isValid) {
        releasePageToBuffer(bp, &ph, FALSE);
        DEBUG_PRINT("updateRecord: Invalid slot %d on page %d\n", record->id.slot, record->id.page);
        return RC_RM_NO_MORE_TUPLES;
    }
    
    // 4. 更新记录数据
    memcpy(ph.data + slotDir[record->id.slot].offset, record->data, recordSize);
    
    // 5. 释放页面
    rc = releasePageToBuffer(bp, &ph, TRUE);
    if (rc != RC_OK) {
        DEBUG_PRINT("updateRecord: Failed to release page %d\n", record->id.page);
        // 即使释放失败也返回成功，避免中断操作流
    }
    
    return RC_OK;
}
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    return RC_OK;
}
RC next (RM_ScanHandle *scan, Record *record)
{
    return RC_OK;
}
RC closeScan (RM_ScanHandle *scan)
{
    return RC_OK;
}

// dealing with schemas

/**
 * @brief create a record
 * 
 * @param record, output, pointer to the record
 * @param schema, input, pointer to the schema
 * @return RC_OK
 */
RC createRecord (Record **record, Schema *schema)
{
    if (record == NULL || schema == NULL) return RC_INVALID_PARAMS;
    // 创建一个记录的头
    *record = (Record *)malloc(sizeof(Record));
    if (*record == NULL) return RC_OUT_OF_MEMORY;

    // 分配记录数据内存（大小由schema决定）
    int recSize = getRecordSize(schema);
    (*record)->data = malloc(recSize);
    if ((*record)->data == NULL) {
        free(*record);
        return RC_OUT_OF_MEMORY;
    }
    // 初始化RID（默认无效）
    (*record)->id.page = -1;
    (*record)->id.slot = -1;
    return RC_OK;
}
/**
 * @brief free a record
 * 
 * @param record, input, pointer to the record
 * @return RC_OK
 */
RC freeRecord (Record *record)
{
    if (record == NULL) return RC_OK;
    
    free(record->data); // free record data
    free(record); // free record header

    return RC_OK;
}

/**
 * @brief get attribute offset in a record
 * 
 * @param schema, input, pointer to the schema
 * @param attrNum, input, attribute number
 * @param result, output, pointer to the offset
 * @return RC_OK
 */
RC attrOffset(Schema *schema, int attrNum, int *result) {
    if (schema == NULL || result == NULL || attrNum < 0 || attrNum >= schema->numAttr)
        return RC_INVALID_PARAMS;
    
    int offset = 0;
    for (int i = 0; i < attrNum; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT: offset += sizeof(int); break;
            case DT_FLOAT: offset += sizeof(float); break;
            case DT_STRING: offset += schema->typeLength[i]; break;
            case DT_BOOL: offset += sizeof(bool); break;
            default: return RC_RM_UNKNOWN_DATATYPE;
        }
    }
    *result = offset;
    return RC_OK;
}
/**
 * @brief get attribute value (simple version)
 * 
 * @param record, input, pointer to the record
 * @param schema, input, pointer to the schema
 * @param attrNum, input, attribute number
 * @param value, output, pointer to the value
 * @return RC_OK
 */
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    if (record == NULL || schema == NULL || attrNum < 0 || attrNum >= schema->numAttr || value == NULL)
        return RC_INVALID_PARAMS;

    *value = (Value *)malloc(sizeof(Value));
    if (*value == NULL) return RC_OUT_OF_MEMORY;

    int offset;
    attrOffset(schema, attrNum, &offset); // 使用现有偏移量计算函数
    char *data = record->data + offset;

    // 根据数据类型解析值
    switch (schema->dataTypes[attrNum]) {
        case DT_INT:
            (*value)->dt = DT_INT;
            memcpy(&(*value)->v.intV, data, sizeof(int));
            break;
        case DT_FLOAT:
            (*value)->dt = DT_FLOAT;
            memcpy(&(*value)->v.floatV, data, sizeof(float));
            break;
        case DT_STRING:
            (*value)->dt = DT_STRING;
            int len = schema->typeLength[attrNum];
            (*value)->v.stringV = malloc(len + 1);
            strncpy((*value)->v.stringV, data, len);
            (*value)->v.stringV[len] = '\0';
            break;
        case DT_BOOL:
            (*value)->dt = DT_BOOL;
            memcpy(&(*value)->v.boolV, data, sizeof(bool));
            break;
        default:
            free(*value);
            return RC_RM_UNKNOWN_DATATYPE;
    }
    return RC_OK;
}
/**
 * @brief set attribute value (simple version)
 * 
 * @param record, input, pointer to the record
 * @param schema, input, pointer to the schema
 * @param attrNum, input, attribute number
 * @param value, input, pointer to the value
 * @return RC_OK
 */
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
if (record == NULL || schema == NULL || value == NULL || attrNum < 0 || attrNum >= schema->numAttr)
        return RC_INVALID_PARAMS;
    if (schema->dataTypes[attrNum] != value->dt) return RC_INVALID_PARAMS;

    int offset;
    attrOffset(schema, attrNum, &offset); // 使用现有偏移量计算函数
    char *data = record->data + offset;

    // 根据数据类型写入值
    switch (value->dt) {
        case DT_INT:
            memcpy(data, &value->v.intV, sizeof(int));
            break;
        case DT_FLOAT:
            memcpy(data, &value->v.floatV, sizeof(float));
            break;
        case DT_STRING:
            int len = schema->typeLength[attrNum];
            strncpy(data, value->v.stringV, len); // 截断或补全到指定长度
            break;
        case DT_BOOL:
            memcpy(data, &value->v.boolV, sizeof(bool));
            break;
        default:
            return RC_RM_UNKNOWN_DATATYPE;
    }

    return RC_OK;
}