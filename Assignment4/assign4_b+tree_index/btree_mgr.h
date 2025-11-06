#ifndef BTREE_MGR_H
#define BTREE_MGR_H

#include "dberror.h"
#include "tables.h"

// 不再重新定义错误码，使用dberror.h中已有的定义

// B+树节点结构体
typedef struct BPlusNode {
    int is_leaf;                  // 1=叶节点, 0=非叶节点
    int key_num;                  // 当前键数量
    Value *keys;                  // 键数组
    void **ptrs;                  // 指针数组：叶节点指向RID，非叶节点指向子节点
    struct BPlusNode *next;       // 叶节点链表指针
    struct BPlusNode *parent;     // 父节点指针
} BPlusNode;

// B+树结构体
typedef struct BPlusTree {
    BPlusNode *root;              // 根节点
    DataType keyType;             // 键类型
    int order;                    // B+树的阶
    int nodeCount;                // 节点数量
    int entryCount;               // 条目数量
} BPlusTree;

// 扫描管理数据
typedef struct BT_ScanMgmt {
    BPlusNode *currentNode;       // 当前扫描的节点
    int currentPos;               // 当前位置
} BT_ScanMgmt;

// 结构用于访问B树
typedef struct BTreeHandle {
    DataType keyType;
    char *idxId;
    void *mgmtData;               // 实际指向BPlusTree
} BTreeHandle;

// 扫描句柄结构
typedef struct BT_ScanHandle {
    BTreeHandle *tree;
    void *mgmtData;               // 实际指向BT_ScanMgmt
} BT_ScanHandle;

// 初始化和关闭索引管理器
extern RC initIndexManager (void *mgmtData);
extern RC shutdownIndexManager ();

// 创建、销毁、打开和关闭B树索引
extern RC createBtree (char *idxId, DataType keyType, int n);
extern RC openBtree (BTreeHandle **tree, char *idxId);
extern RC closeBtree (BTreeHandle *tree);
extern RC deleteBtree (char *idxId);

// 获取B树信息
extern RC getNumNodes (BTreeHandle *tree, int *result);
extern RC getNumEntries (BTreeHandle *tree, int *result);
extern RC getKeyType (BTreeHandle *tree, DataType *result);

// 索引操作
extern RC findKey (BTreeHandle *tree, Value *key, RID *result);
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid);
extern RC deleteKey (BTreeHandle *tree, Value *key);

// 扫描操作
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
extern RC nextEntry (BT_ScanHandle *handle, RID *result);
extern RC closeTreeScan (BT_ScanHandle *handle);

// 调试和测试函数
extern char *printTree (BTreeHandle *tree);

// 辅助函数声明
// static BPlusNode* create_node(BPlusTree *tree, int is_leaf);
// static int compare_values(DataType type, Value *a, Value *b);
// static void split_node(BPlusTree *tree, BPlusNode *parent, int index);
// static void insert_non_full(BPlusTree *tree, BPlusNode *node, Value *key, RID rid);
// static BPlusNode* find_leaf_node(BPlusTree *tree, Value *key);
// static int find_key_index(BPlusNode *node, DataType keyType, Value *key);
// static void handle_underflow(BPlusTree *tree, BPlusNode *node);

extern bool indexManagerInitialized;

#endif // BTREE_MGR_H