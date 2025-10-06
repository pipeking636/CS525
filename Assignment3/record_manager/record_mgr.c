#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "tables.h"
#include "dberror.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

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
// static RC initDataPage(BM_BufferPool *bp, BM_PageHandle *ph) {
//     PageHeader *header = (PageHeader *)ph->data;
//     header->slotDirOffset = sizeof(PageHeader); // 槽位目录紧跟页头
//     header->slotCount = 0;
//     header->freeSlotCount = 0;
//     header->nextFreePage = -1;
//     markDirty(bp, ph);
//     return RC_OK;
// }

static RC getPageFromBuffer(BM_BufferPool *bp, BM_PageHandle *ph, PageNumber pageNum) {
    RC rc = pinPage(bp, ph, pageNum);
    if (rc != RC_OK) return rc;
    ((RM_TableMgmt *)bp->mgmtData)->numReadIO++; // 统计读IO
    return RC_OK;
}

static RC releasePageToBuffer(BM_BufferPool *bp, BM_PageHandle *ph, bool isDirty) {
    if (isDirty) markDirty(bp, ph);
    if (isDirty) ((RM_TableMgmt *)bp->mgmtData)->numWriteIO++; // 统计写IO
    return unpinPage(bp, ph);
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
    initBufferPool(&mgmt->bufferPool, name, 10, RS_FIFO, NULL);
    mgmt->bufferPool.mgmtData = mgmt;
    mgmt->numReadIO = 0;
    mgmt->numWriteIO = 0;

    // 6. 设置rel
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
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData;
    RC rc = RC_OK;

    // 1. 关闭缓冲池（此时frame 0已非脏页，无需刷写）
    rc = shutdownBufferPool(&mgmt->bufferPool);
    if (rc != RC_OK) {
        fprintf(stderr, "Warning: shutdown buffer pool failed\n");
    }

    // 2. 关闭文件句柄
    rc = closePageFile(&mgmt->fileHandle);
    if (rc != RC_OK) {
        fprintf(stderr, "Warning: close page file failed\n");
    }

    // 3. 释放Schema和管理结构体
    rc = freeSchema(mgmt->schema);
    if (rc != RC_OK) {
        fprintf(stderr, "Warning: free schema failed\n");
    }
    free(mgmt);
    rel->mgmtData = NULL;

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
    return destroyPageFile(name);
}

// 1. 获取表总页数
int getTableTotalPages(RM_TableData *rel) {
    if (rel == NULL || rel->mgmtData == NULL) return -1;
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData;
    return mgmt->tableInfo.totalPages;
}

// 2. 获取记录大小
int getTableRecordSize(RM_TableData *rel) {
    if (rel == NULL || rel->mgmtData == NULL) return -1;
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData;
    return mgmt->tableInfo.recordSize;
}

// 3. 获取表名（返回拷贝，需外部释放）
char* getTableName(RM_TableData *rel) {
    if (rel == NULL || rel->mgmtData == NULL) return NULL;
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData;
    char *name = (char *)malloc(strlen(mgmt->tableInfo.tableName) + 1);
    strcpy(name, mgmt->tableInfo.tableName);
    return name;
}
/**
 * @brief get number of tuples in a table
 * 
 * @param rel, pointer to the table data
 * @return number of tuples
 */
// 暂时实现空函数（避免未定义错误）
int getNumTuples(RM_TableData *rel) {
    if (rel == NULL || rel->mgmtData == NULL) return -1;
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData;
    return mgmt->tableInfo.numTuples;
}

// handling records in a table
/**
 * @brief insert a record into a table
 * 
 * @param rel, pointer to the table data
 * @param record, pointer to the record
 * @return RC_OK
 */
RC insertRecord (RM_TableData *rel, Record *record)
{
    return RC_OK;
}
/**
 * @brief get a record from a table
 * 
 * @param rel, pointer to the table data
 * @param id, RID of the record
 * @param record, pointer to the record
 * @return RC_OK
 */
RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    return RC_OK;
}
RC deleteRecord (RM_TableData *rel, RID id)
{
    return RC_OK;
}
RC updateRecord (RM_TableData *rel, Record *record)
{
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
 * @param record, pointer to the record
 * @param schema, pointer to the schema
 * @return RC_OK
 */
RC createRecord (Record **record, Schema *schema)
{
    return RC_OK;
}
/**
 * @brief free a record
 * 
 * @param record, pointer to the record
 * @return RC_OK
 */
RC freeRecord (Record *record)
{
    return RC_OK;
}
/**
 * @brief get attribute value (simple version)
 * 
 * @param record, pointer to the record
 * @param schema, pointer to the schema
 * @param attrNum, attribute number
 * @param value, pointer to the value
 * @return RC_OK
 */
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    return RC_OK;
}
/**
 * @brief set attribute value (simple version)
 * 
 * @param record, pointer to the record
 * @param schema, pointer to the schema
 * @param attrNum, attribute number
 * @param value, pointer to the value
 * @return RC_OK
 */
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    return RC_OK;
}