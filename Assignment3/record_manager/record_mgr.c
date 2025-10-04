#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
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
    int numReadIO;             // 统计用（可选，参考缓冲管理器）
    int numWriteIO;            // 统计用（可选）
} RM_TableMgmt;

// table and manager
RC initRecordManager (void *mgmtData)
{
    return RC_OK;
}
RC shutdownRecordManager ()
{
    return RC_OK;
}
RC createTable (char *name, Schema *schema)
{
    return RC_OK;
}
RC openTable (RM_TableData *rel, char *name)
{
    return RC_OK;
}
RC closeTable (RM_TableData *rel)
{
    return RC_OK;
}
RC deleteTable (char *name)
{
    return RC_OK;
}
int getNumTuples (RM_TableData *rel)
{
    return -1;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record)
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
RC getRecord (RM_TableData *rel, RID id, Record *record)
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
int getRecordSize (Schema *schema)
{
    return -1;
}
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    return -1;
}
RC freeSchema (Schema *schema)
{
    if (schema)
    {
        free(schema);
        return RC_OK;
    }
    else
        return RC_UNVALID_HANDLE;
}
RC createRecord (Record **record, Schema *schema)
{
    return RC_OK;
}
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
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    return RC_OK;
}
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    return RC_OK;
}
