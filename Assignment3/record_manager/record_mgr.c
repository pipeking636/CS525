#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "tables.h"
#include "dberror.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// 页头元数据（每个数据页的页头）, 固定空间的结构体。
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
    int recordSize;            // 每条记录的固定大小（从schema计算）
} TableInfo;

// RM_TableData的mgmtData实际类型
typedef struct RM_TableMgmt {
    BM_BufferPool bufferPool;  // 该表对应的缓冲池
    TableInfo tableInfo;       // 从表信息页读取的元数据
    SM_FileHandle fileHandle;  // 该表对应的文件句柄
    int numReadIO;             // 统计用（可选，参考缓冲管理器）
    int numWriteIO;            // 统计用（可选）
} RM_TableMgmt;

// debug functions

// table and manager
/**
 * @brief record manager initialization
 * 
 * @param mgmtData, pointer to the record manager management data, not used in this function
 * @return RC_OK
 */
RC initRecordManager (void *mgmtData)
{
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
    return RC_OK;
}
/**
 * @brief get number of tuples in a table
 * 
 * @param rel, pointer to the table data
 * @return number of tuples
 */
int getNumTuples (RM_TableData *rel)
{
    return 0;
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
 * @brief get record size in bytes
 * 
 * @param schema, pointer to the schema
 * @return record size in bytes
 */
int getRecordSize (Schema *schema)
{
    return 0;
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
    return NULL;
}
/**
 * @brief free a schema
 * 
 * @param schema, pointer to the schema
 * @return RC_OK
 */
RC freeSchema(Schema *schema) {
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
