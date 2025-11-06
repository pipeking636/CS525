/*===================================================================================================================
注意：
- 请将bptree.c中的代码merge到btree_mgr.c中，但不要修改当前的函数接口，因为这些接口被测试程序test_assign4_1.c和test_expr.c调用。
- merge代码时，可以修改bptree.c中的代码以适应当前定义好的函数，但是不要随便修改本来的逻辑，因为bptree.c已经测试过了，相对应的逻辑是正确的。
- 再次强调我在bptree.c中的经验，节点插入时，一定要先插入键，再判断是否要分裂，否则很容易出现问题。
=====================================================================================================================*/

#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "btree_mgr.h"
#include "dberror.h"
#include "dt.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>

#ifdef DEBUG // define this macro from makefile to enable debug print
    #define DEBUG_PRINT(format, ...) printf(format, ##__VA_ARGS__)
#else
    #define DEBUG_PRINT(format, ...)
#endif

// init and shutdown index manager
RC initIndexManager (void *mgmtData)
{
    return RC_OK;
}
RC shutdownIndexManager ()
{
    return RC_OK;
}

// create, destroy, open, and close an btree index
RC createBtree (char *idxId, DataType keyType, int n)
{
    return RC_OK;
}
RC openBtree (BTreeHandle **tree, char *idxId)
{
    return RC_OK;
}
RC closeBtree (BTreeHandle *tree)
{
    return RC_OK;
}
RC deleteBtree (char *idxId)
{
    return RC_OK;
}

// access information about a b-tree
RC getNumNodes (BTreeHandle *tree, int *result)
{
    return RC_OK;
}
RC getNumEntries (BTreeHandle *tree, int *result)
{
    return RC_OK;
}
RC getKeyType (BTreeHandle *tree, DataType *result)
{
    return RC_OK;
}   

// index access
RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
    return RC_OK;
}
RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
    return RC_OK;
}
RC deleteKey (BTreeHandle *tree, Value *key)
{
    return RC_OK;
}
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
    return RC_OK;
}
RC nextEntry (BT_ScanHandle *handle, RID *result)
{
    return RC_OK;
}
RC closeTreeScan (BT_ScanHandle *handle)
{
    return RC_OK;
}

// debug and test functions
char *printTree (BTreeHandle *tree)
{
    return NULL;
}