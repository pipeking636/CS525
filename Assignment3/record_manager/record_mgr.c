#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "tables.h"
#include "dberror.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// 页头元数据（每个数据页的页头）
typedef struct PageHeader {
    int freeSpaceOffset;       // 空闲空间起始偏移量（从页尾向页头增长）
    int slotCount;             // 已使用的槽位数量
    int nextFreePage;          // 下一个空闲页的页号（-1表示无）
    bool isTableInfoPage;      // 是否为表信息页
} PageHeader;

// 表信息页的内容（存储在第0页）
typedef struct TableInfo {
    char tableName[100];       // 表名
    Schema schema;             // 表的schema（需注意动态内存分配）
    int numTuples;             // 记录总数
    int totalPages;            // 表的总页数
    int freePageListHead;      // 空闲页链表头（-1表示无空闲页）
    int recordSize;            // 每条记录的固定大小（由schema计算）
} TableInfo;

// RM_TableData的mgmtData实际类型
typedef struct RM_TableMgmt {
    BM_BufferPool bufferPool;  // 该表对应的缓冲池
    TableInfo tableInfo;       // 从表信息页读取的元数据
    SM_FileHandle fileHandle;  // 该表对应的文件句柄
    int numReadIO;             // 统计用（可选，参考缓冲管理器）
    int numWriteIO;            // 统计用（可选）
} RM_TableMgmt;

// table and manager
/**
 * @brief record manager initialization
 * 
 * @param mgmtData, pointer to the record manager management data, not used in this function
 * @return RC_OK
 */
RC initRecordManager (void *mgmtData)
{
    initStorageManager(); // 初始化存储管理器
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
    // 1. create page file
    if(createPageFile(name) != RC_OK)
        return RC_FILE_HANDLE_NOT_INIT;
    // 2. initialize table info (page 0)
    SM_FileHandle fh;
    if(openPageFile(name, &fh) != RC_OK)
        return RC_FILE_HANDLE_NOT_INIT;
    // calculate record size
    int recordSize = getRecordSize(schema);
    if(recordSize <= 0)
    {
        closePageFile(&fh);
        return RC_UNVALID_HANDLE;
    }
    // 3. initialize table info
    TableInfo tableInfo;
    strcpy(tableInfo.tableName, name); //table name keep normal string length and "\0" ending
    tableInfo.schema = *schema; // copy schema, please note dynamic memory in schema
    tableInfo.numTuples = 0; // no tuple in the table yet
    tableInfo.totalPages = 1; // at least one page for table info
    tableInfo.freePageListHead = -1; // no free page
    tableInfo.recordSize = recordSize; // record size is fixed
    // 4. write table info to page 0
    SM_PageHandle page = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
    // 5. set page 0 header
    PageHeader phHeader;
    phHeader.freeSpaceOffset = PAGE_SIZE - sizeof(PageHeader); // free space start from page tail
    phHeader.slotCount = 0; // no slot used yet
    phHeader.nextFreePage = -1; // no next free page
    phHeader.isTableInfoPage = true; // mark as table info page
    memcpy(page, &phHeader, sizeof(PageHeader));
    memcpy(page + sizeof(PageHeader), &tableInfo, sizeof(TableInfo));
    if (writeBlock(0, &fh, page) != RC_OK)
    {
        free(page);
        closePageFile(&fh);
        return RC_WRITE_FAILED;
    }
    free(page);
    closePageFile(&fh);
    return RC_OK;
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
    RC ret = RC_OK;

    if(rel == NULL || name == NULL)
        return RC_UNVALID_HANDLE; 

    // 1. initialize RM_TableData
    rel->name = strdup(name); // copy table name, please note dynamic memory in string
    rel->schema = (Schema *)malloc(sizeof(Schema)); // copy schema, please note dynamic memory in schema
    rel->mgmtData =(RM_TableMgmt *)malloc(sizeof(RM_TableMgmt)); // copy mgmtData, please note dynamic memory in mgmtData
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData; // cast mgmtData to RM_TableMgmt
    ret=openPageFile(name, &mgmt->fileHandle);
    if(ret != RC_OK)
    {
        free(rel->name);
        free(rel->schema);
        free(rel->mgmtData);
        return ret;        
    }
    // 2. initialize buffer pool (use FIFO policy)
    ret = initBufferPool(&mgmt->bufferPool, name, 10, RS_FIFO, NULL);
    if ( ret != RC_OK)
    {
        free(rel->name);
        free(rel->schema);
        free(rel->mgmtData);
        return ret;
    }
    // 3. read table info from page 0
    BM_PageHandle ph;
    ret = pinPage(&mgmt->bufferPool,&ph,0);
    if( ret != RC_OK)
    {
        free(rel->name);
        free(rel->schema);
        free(rel->mgmtData);
        return ret;
    }
    // copy table info to mgmt
    TableInfo *tableInfo = (TableInfo *)ph.data;
    mgmt->tableInfo = *tableInfo; // copy table info, please note dynamic memory in schema
    *rel->schema = mgmt->tableInfo.schema; // copy schema, please note dynamic memory in schema
    // 4. unpin page 0
    ret = unpinPage(&mgmt->bufferPool, &ph);
    if( ret != RC_OK)
    {
        free(rel->name);
        free(rel->schema);
        free(rel->mgmtData);
        return ret;
    }
    return RC_OK;
}
/**
 * @brief close a table, release buffer pool
 * 
 * @param rel, pointer to the table data
 * @return RC_OK
 */
RC closeTable (RM_TableData *rel)
{
    RC ret = RC_OK;
    if(rel == NULL || rel->mgmtData == NULL)
        return RC_UNVALID_HANDLE;
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData; // cast mgmtData to RM_TableMgmt
    // 1. close buffer pool
    ret = shutdownBufferPool(&mgmt->bufferPool);
    if( ret != RC_OK)
    {
        /*maybe need more error handling*/
        return ret;
    }
    ret = closePageFile(&mgmt->fileHandle);
    if( ret != RC_OK) return ret;
    
    // 2. release table info
    free(rel->name);
    free(rel->schema);
    free(rel->mgmtData);
    rel->name = NULL;
    rel->schema = NULL;
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
    if(name == NULL)
        return RC_UNVALID_HANDLE;
    return destroyPageFile(name);
}
/**
 * @brief get number of tuples in a table
 * 
 * @param rel, pointer to the table data
 * @return number of tuples
 */
int getNumTuples (RM_TableData *rel)
{
    if(rel == NULL || rel->mgmtData == NULL)
        return RC_UNVALID_HANDLE;
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData; // cast mgmtData to RM_TableMgmt
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
    RC ret = RC_OK;

    if(rel == NULL || rel->mgmtData == NULL || record == NULL)
        return RC_UNVALID_HANDLE;

    // get record size
    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData; // cast mgmtData to RM_TableMgmt
    int recordSize = getRecordSize(rel->schema);
    if(recordSize<=0) return RC_RM_INVALID_RECORD_SIZE;

    // 1. find valid page(from free list first)
    int targetPageNum =-1;
    PageHeader phHeader;
    BM_PageHandle ph;

    // traverse free list to find enough free space
    int currPage = mgmt->tableInfo.freePageListHead;
    while(currPage != -1)
    {
        // pin current free page, read page header
        ret = pinPage(&mgmt->bufferPool, &ph, currPage);
        if( ret != RC_OK) 
        {
            currPage = -1;
            break;
        }
        // parse page header
        memcpy(&phHeader, ph.data, sizeof(PageHeader));
        // check free space (page size-header size-slot size-record size)
        int freeSpace = phHeader.freeSpaceOffset - sizeof(PageHeader) - phHeader.slotCount * recordSize;
        if (freeSpace >= recordSize) {
            targetPageNum = currPage;
            break;  // find free page with enough space
        } else {
            // this page no enough space, unpin and move to next free page
            unpinPage(&mgmt->bufferPool, &ph);
            currPage = phHeader.nextFreePage;
        }
    }
    // 2. if can not find free page, create new page
    if(targetPageNum == -1)
    {
        ret = appendEmptyBlock(&mgmt->fileHandle);
        if( ret != RC_OK) return ret;
        targetPageNum = mgmt->fileHandle.totalNumPages - 1; // new page number
        // pin new page, and initialize page header
        ret = pinPage(&mgmt->bufferPool, &ph, targetPageNum);
        if( ret != RC_OK) return ret;
        // initialize page header
        phHeader.freeSpaceOffset = PAGE_SIZE; // free space offset is end of page
        phHeader.slotCount = 0;
        phHeader.nextFreePage = mgmt->tableInfo.freePageListHead; // link to free list
        mgmt->tableInfo.freePageListHead = targetPageNum; // update free list head
        memcpy(ph.data, &phHeader, sizeof(PageHeader));
        markDirty(&mgmt->bufferPool, &ph);// mark page as dirty
    }
    // 3. insert record to page
    // calculate record offset(page header size + slot count * record size)
    int recordOffset = sizeof(PageHeader) + phHeader.slotCount * recordSize;
    // copy record data to page
    memcpy(ph.data + recordOffset, record->data, recordSize);
    // update page header
    phHeader.slotCount++;
    phHeader.freeSpaceOffset -= recordSize; // update free space offset
    memcpy(ph.data, &phHeader, sizeof(PageHeader));
    // mark page as dirty
    markDirty(&mgmt->bufferPool, &ph);// mark page as dirty
    unpinPage(&mgmt->bufferPool, &ph); // unpin page

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
    RC ret;
    if(rel == NULL || rel->mgmtData == NULL || record == NULL)
        return RC_UNVALID_HANDLE;

    RM_TableMgmt *mgmt = (RM_TableMgmt *)rel->mgmtData; // cast mgmtData to RM_TableMgmt
    int recordSize = mgmt->tableInfo.recordSize;
    // 1. pin page
    BM_PageHandle ph;
    ret = pinPage(&mgmt->bufferPool, &ph, id.page);
    if( ret != RC_OK) return ret;
    // 2. calculate data offset
    PageHeader *pageHeader = (PageHeader *)ph.data;
    int dataOffset = sizeof(PageHeader) + id.slot * recordSize;
    if(dataOffset + recordSize > pageHeader->freeSpaceOffset)
    {
        unpinPage(&mgmt->bufferPool, &ph);
        return RC_RM_NO_MORE_TUPLES;
    }
    // 3. copy record to record
    memcpy(record->data, ph.data + dataOffset, recordSize);
    // 4. set RID
    record->id = id;
    ret = unpinPage(&mgmt->bufferPool, &ph);
    if( ret != RC_OK) return ret;   
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
 * @brief get record size in bytes
 * 
 * @param schema, pointer to the schema
 * @return record size in bytes
 */
int getRecordSize (Schema *schema)
{
    if(schema == NULL)
        return RC_UNVALID_HANDLE;
    int size = 0;
    for(int i = 0; i < schema->numAttr; i++)
    {
        switch(schema->dataTypes[i])
        {
            case DT_INT: size += sizeof(int); break;
            case DT_STRING: size += schema->typeLength[i]; break;
            case DT_FLOAT: size += sizeof(float); break;
            case DT_BOOL: size += sizeof(bool); break;
            default: return RC_UNVALID_HANDLE;
        }
    }
    return size;
}
/**
 * @brief create a schema
 * 
 * @param numAttr, number of attributes
 * @param attrNames, array of attribute names
 * @param dataTypes, array of data types
 * @param typeLength, array of type lengths
 * @param keySize, number of key attributes
 * @param keys, array of key attribute indices
 * @return pointer to the schema
 */
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    if(numAttr <= 0 || attrNames == NULL || dataTypes == NULL || typeLength == NULL || keySize < 0 || keys == NULL)
        return NULL;
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    if(schema == NULL)
        return NULL;
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;
    return schema;
}
/**
 * @brief free a schema
 * 
 * @param schema, pointer to the schema
 * @return RC_OK
 */
RC freeSchema(Schema *schema) {
    if (schema == NULL)
        return RC_UNVALID_HANDLE;
    // 释放动态分配的成员（假设attrNames等是动态分配的）
    if(schema->attrNames != NULL)
        free(schema->attrNames);
    if(schema->dataTypes != NULL)
        free(schema->dataTypes);
    if(schema->typeLength != NULL)
        free(schema->typeLength);
    if(schema->keyAttrs != NULL)
        free(schema->keyAttrs);
    // 释放schema结构体
    free(schema);
    return RC_OK;
}
/**
 * @brief create a record
 * 
 * @param record, pointer to the record
 * @param schema, pointer to the schema
 * @return RC_OK
 */
RC createRecord (Record **record, Schema *schema)
{
    if(record == NULL || schema == NULL)
        return RC_UNVALID_HANDLE;
    
    *record = (Record *)malloc(sizeof(Record));
    if(*record == NULL)
        return RC_MEMORY_ALLOC_FAILED;
    (*record)->data = (char *)malloc(getRecordSize(schema));
    if((*record)->data == NULL)
    {
        free(*record);
        return RC_MEMORY_ALLOC_FAILED;
    }

    (*record)->id.page = -1;
    (*record)->id.slot = -1;

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
    if (record)
    {
        free(record->data);
        free(record);
        return RC_OK;
    }
    else
        return RC_UNVALID_HANDLE;
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
    if(record == NULL || schema == NULL || attrNum < 0 || attrNum >= schema->numAttr || value == NULL)
        return RC_UNVALID_HANDLE;
    
    *value = (Value *)malloc(sizeof(Value));
    if(*value == NULL)
        return RC_MEMORY_ALLOC_FAILED;
    (*value)->dt = schema->dataTypes[attrNum];
    char *dataPtr = record->data;
    int offset = 0;
    // calculate all attribute offsets
    for(int i = 0; i < attrNum; i++)
    {
        switch(schema->dataTypes[i])
        {
            case DT_INT: offset += sizeof(int); break;
            case DT_STRING: offset += schema->typeLength[i]; break;
            case DT_FLOAT: offset += sizeof(float); break;
            case DT_BOOL: offset += sizeof(bool); break;
            default: free(*value); return RC_INVALID_PARAMS;
        }
    }
    // copy the attribute value
    switch(schema->dataTypes[attrNum])
    {
        case DT_INT: (*value)->v.intV = *((int *)(dataPtr + offset)); break;
        case DT_STRING: 
            (*value)->v.stringV = (char *)malloc(schema->typeLength[attrNum]);
            if (((*value)->v.stringV) == NULL)
                {
                    free(*value);
                    return RC_MEMORY_ALLOC_FAILED;
                }
            memcpy((*value)->v.stringV, (char *)(dataPtr + offset), schema->typeLength[attrNum]); 
            break; // 根据文档要求，记录中的字符串不包含结尾的"\0"~!
        case DT_FLOAT: (*value)->v.floatV = *((float *)(dataPtr + offset)); break;
        case DT_BOOL: (*value)->v.boolV = *((bool *)(dataPtr + offset)); break;
        default: free(*value); return RC_INVALID_PARAMS;
    }
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
    if(record == NULL || schema == NULL || attrNum < 0 || attrNum >= schema->numAttr || value == NULL)
        return RC_UNVALID_HANDLE;

    char *dataPtr = record->data;
    int offset = 0;
    // calculate all attribute offsets
    for(int i = 0; i < attrNum; i++)
    {
        switch(schema->dataTypes[i])
        {
            case DT_INT: offset += sizeof(int); break;
            case DT_STRING: offset += schema->typeLength[i]; break;
            case DT_FLOAT: offset += sizeof(float); break;
            case DT_BOOL: offset += sizeof(bool); break;
            default: free(value); return RC_INVALID_PARAMS;
        }
    }
    // set the attribute value
    switch(schema->dataTypes[attrNum])
    {
        case DT_INT: *((int *)(dataPtr + offset)) = value->v.intV; break;
        case DT_STRING: 
            memcpy((char *)(dataPtr + offset), value->v.stringV, schema->typeLength[attrNum]); 
            break; // 根据文档要求，记录中的字符串不包含结尾的"\0"~!
        case DT_FLOAT: *((float *)(dataPtr + offset)) = value->v.floatV; break;
        case DT_BOOL: *((bool *)(dataPtr + offset)) = value->v.boolV; break;
        default: free(value); return RC_INVALID_PARAMS;
    }

    return RC_OK;
}
