/*
注意：
- 请将bptree.c中的代码merge到btree_mgr.c中，但不要修改当前的函数接口，因为这些接口被测试程序test_assign4_1.c和test_expr.c调用。
- 可以修改bptree.c中的代码以适应当前定义好的函数，但是不要随便修改本来的逻辑，因为bptree.c已经测试过了，相对应的逻辑是正确的。
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
#include <stdarg.h>

#ifdef DEBUG // define this macro from makefile to enable debug print
    #define DEBUG_PRINT(format, ...) printf(format, ##__VA_ARGS__)
#else
    #define DEBUG_PRINT(format, ...)
#endif

// 全局变量用于记录节点中最大键数量
int GLOBAL_MAX_KEYS = 0;

// 日志文件指针
FILE *log_file = NULL;

// 定义全局索引信息结构体
typedef struct IndexInfo {
    char *idxId;
    DataType keyType;
    int maxKeys;
} IndexInfo;

// 存储索引信息的全局变量
IndexInfo *currentIndexInfo = NULL;

// B+树节点结构
typedef struct BPlusNode {
    bool is_leaf;
    int key_count;
    Value **keys;
    RID *rids;
    struct BPlusNode **ptrs;
    struct BPlusNode *next;
    struct BPlusNode *prev;
    struct BPlusNode *parent;
    int max_keys;
} BPlusNode;

// B+树结构
typedef struct BPlusTree {
    BPlusNode *root;
    int node_count;
    int entry_count;
    DataType key_type;
    FILE *log_file;
} BPlusTree;

// 日志函数
void log_message(char *format, ...)
{
    va_list args;
    va_start(args, format);
    
    if (log_file != NULL)
    {
        vfprintf(log_file, format, args);
        fflush(log_file);
    }
    
    va_end(args);
}

void console_log(char *format, ...)
{
    va_list args;
    va_start(args, format);
    
    DEBUG_PRINT(format, args);
    
    va_end(args);
}

// 修复create_node函数，确保正确初始化所有字段
BPlusNode* create_node(bool is_leaf, int max_keys)
{
    BPlusNode *node = (BPlusNode*)malloc(sizeof(BPlusNode));
    if (node == NULL)
        return NULL;
    
    // 正确初始化所有字段
    node->is_leaf = is_leaf;
    node->key_count = 0;  // 重要：初始化为0
    node->next = NULL;
    node->prev = NULL;
    node->parent = NULL;
    node->max_keys = max_keys;
    
    node->keys = (Value**)malloc(sizeof(Value*) * max_keys);
    if (node->keys == NULL)
    {
        free(node);
        return NULL;
    }
    
    // 初始化所有键指针为NULL
    for (int i = 0; i < max_keys; i++)
    {
        node->keys[i] = NULL;
    }
    
    if (is_leaf)
    {
        node->rids = (RID*)malloc(sizeof(RID) * max_keys);
        if (node->rids == NULL)
        {
            free(node->keys);
            free(node);
            return NULL;
        }
        // 初始化RID结构体
        for (int i = 0; i < max_keys; i++)
        {
            node->rids[i].page = 0;
            node->rids[i].slot = 0;
        }
        node->ptrs = NULL;
    }
    else
    {
        node->ptrs = (BPlusNode**)malloc(sizeof(BPlusNode*) * (max_keys + 1));
        if (node->ptrs == NULL)
        {
            free(node->keys);
            free(node);
            return NULL;
        }
        // 初始化所有子节点指针为NULL
        for (int i = 0; i < max_keys + 1; i++)
        {
            node->ptrs[i] = NULL;
        }
        node->rids = NULL;
    }
    
    return node;
}

// 修复init_bplus_tree函数，确保正确初始化所有字段
BPlusTree* init_bplus_tree(DataType key_type, int max_keys)
{
    BPlusTree *tree = (BPlusTree*)malloc(sizeof(BPlusTree));
    if (tree == NULL)
    {
        return NULL;
    }
    
    // 初始化所有字段
    tree->root = create_node(true, max_keys);
    if (tree->root == NULL)
    {
        free(tree);
        return NULL;
    }
    
    tree->node_count = 1;
    tree->entry_count = 0;
    tree->key_type = key_type;
    tree->log_file = log_file;
    
    return tree;
}

// 释放B+树
void free_bplus_tree(BPlusNode *node)
{
    if (node == NULL)
        return;
    
    if (node->is_leaf)
    {
        for (int i = 0; i < node->key_count; i++)
        {
            free(node->keys[i]);
        }
        free(node->rids);
    }
    else
    {
        for (int i = 0; i < node->key_count + 1; i++)
        {
            free_bplus_tree(node->ptrs[i]);
        }
        free(node->ptrs);
    }
    
    free(node->keys);
    free(node);
}

// 比较两个Value值
int compareValues(Value *v1, Value *v2)
{
    if (v1 == NULL || v2 == NULL || v1->dt != v2->dt)
        return -2; // 类型不同或参数无效
    
    switch(v1->dt)
    {
        case DT_INT:
            return (v1->v.intV < v2->v.intV) ? -1 : (v1->v.intV > v2->v.intV) ? 1 : 0;
        case DT_STRING:
            return strcmp(v1->v.stringV, v2->v.stringV);
        case DT_FLOAT:
            return (v1->v.floatV < v2->v.floatV) ? -1 : (v1->v.floatV > v2->v.floatV) ? 1 : 0;
        case DT_BOOL:
            return (v1->v.boolV == v2->v.boolV) ? 0 : (v1->v.boolV < v2->v.boolV) ? -1 : 1;
        default:
            return -2; // 未知类型
    }
}

// 查找叶节点
BPlusNode* find_leaf_node(BPlusNode *node, Value *key, bool *found)
{
    if (node == NULL)
    {
        if (found != NULL)
            *found = false;
        return NULL;
    }
    
    if (node->is_leaf)
    {
        if (found != NULL)
        {
            *found = false;
            for (int i = 0; i < node->key_count; i++)
            {
                if (compareValues(node->keys[i], key) == 0)
                {
                    *found = true;
                    break;
                }
            }
        }
        return node;
    }
    
    int i = 0;
    while (i < node->key_count && compareValues(node->keys[i], key) < 0)
        i++;
    
    return find_leaf_node(node->ptrs[i], key, found);
}

// 在叶节点中搜索键
int search_in_leaf(BPlusNode *node, Value *key)
{
    if (node == NULL || !node->is_leaf)
        return -1;
    
    for (int i = 0; i < node->key_count; i++)
    {
        if (compareValues(node->keys[i], key) == 0)
            return i;
    }
    
    return -1;
}

// 查找键的索引
int find_key_index(BPlusNode *node, Value *key)
{
    if (node == NULL)
        return -1;
    
    int i = 0;
    while (i < node->key_count && compareValues(node->keys[i], key) < 0)
        i++;
    
    return i;
}

// 复制Value值
Value* copyValue(Value *val)
{
    if (val == NULL)
        return NULL;
    
    Value *new_val = (Value*)malloc(sizeof(Value));
    if (new_val == NULL)
        return NULL;
    
    new_val->dt = val->dt;
    
    switch(val->dt)
    {
        case DT_INT:
            new_val->v.intV = val->v.intV;
            break;
        case DT_STRING:
            new_val->v.stringV = (char*)malloc(strlen(val->v.stringV) + 1);
            if (new_val->v.stringV == NULL)
            {
                free(new_val);
                return NULL;
            }
            strcpy(new_val->v.stringV, val->v.stringV);
            break;
        case DT_FLOAT:
            new_val->v.floatV = val->v.floatV;
            break;
        case DT_BOOL:
            new_val->v.boolV = val->v.boolV;
            break;
        default:
            free(new_val);
            return NULL;
    }
    
    return new_val;
}

// 修复split_leaf函数，确保节点计数正确
void split_leaf(BPlusNode *node, BPlusTree *tree)
{
    if (node == NULL || !node->is_leaf)
        return;
    
    int mid = node->key_count / 2;
    
    BPlusNode *new_node = create_node(true, node->max_keys);
    if (new_node == NULL)
        return;
    
    tree->node_count++;
    
    // 复制后半部分的键和RID到新节点
    for (int i = mid; i < node->key_count; i++)
    {
        new_node->keys[new_node->key_count] = node->keys[i];
        new_node->rids[new_node->key_count] = node->rids[i];
        new_node->key_count++;
        
        // 将原节点中的键设置为NULL（避免重复释放）
        node->keys[i] = NULL;
    }
    
    node->key_count = mid;
    
    // 设置叶节点链表指针
    new_node->next = node->next;
    new_node->prev = node;
    if (node->next != NULL)
        node->next->prev = new_node;
    node->next = new_node;
    
    // 如果是根节点，需要创建新的根节点
    if (node->parent == NULL)
    {
        BPlusNode *new_root = create_node(false, node->max_keys);
        if (new_root == NULL)
        {
            // 释放新节点
            for (int i = 0; i < new_node->key_count; i++)
                free(new_node->keys[i]);
            free(new_node->rids);
            free(new_node->keys);
            free(new_node);
            tree->node_count--;
            return;
        }
        
        tree->root = new_root;
        tree->node_count++;
        
        new_root->ptrs[0] = node;
        new_root->ptrs[1] = new_node;
        new_root->keys[0] = new_node->keys[0];
        new_root->key_count++;
        
        node->parent = new_root;
        new_node->parent = new_root;
        
        return;
    }
    
    // 否则，更新父节点
    int index = 0;
    while (index <= node->parent->key_count && node->parent->ptrs[index] != node)
        index++;
    
    // 在父节点中插入分隔键和指向新节点的指针
    for (int i = node->parent->key_count; i > index; i--)
    {
        node->parent->keys[i] = node->parent->keys[i - 1];
        node->parent->ptrs[i + 1] = node->parent->ptrs[i];
    }
    
    node->parent->keys[index] = copyValue(new_node->keys[0]);
    node->parent->ptrs[index + 1] = new_node;
    node->parent->key_count++;
    new_node->parent = node->parent;
}

// 修复split_non_leaf函数，确保节点计数正确
void split_non_leaf(BPlusNode *node, BPlusTree *tree)
{
    if (node == NULL || node->is_leaf)
        return;
    
    int mid = node->key_count / 2;
    Value *mid_key = node->keys[mid];
    
    BPlusNode *new_node = create_node(false, node->max_keys);
    if (new_node == NULL)
        return;
    
    tree->node_count++;
    
    // 复制后半部分的键和指针到新节点
    int j = 0;
    for (int i = mid + 1; i < node->key_count; i++)
    {
        new_node->keys[j] = node->keys[i];
        new_node->ptrs[j] = node->ptrs[i];
        new_node->ptrs[j]->parent = new_node;
        new_node->key_count++;
        j++;
    }
    
    // 复制最后一个指针
    new_node->ptrs[j] = node->ptrs[node->key_count];
    new_node->ptrs[j]->parent = new_node;
    
    node->key_count = mid;
    
    // 如果是根节点，需要创建新的根节点
    if (node->parent == NULL)
    {
        BPlusNode *new_root = create_node(false, node->max_keys);
        if (new_root == NULL)
        {
            // 释放新节点
            for (int i = 0; i < new_node->key_count; i++)
                free(new_node->keys[i]);
            free(new_node->ptrs);
            free(new_node->keys);
            free(new_node);
            tree->node_count--;
            return;
        }
        
        tree->root = new_root;
        tree->node_count++;
        
        new_root->ptrs[0] = node;
        new_root->ptrs[1] = new_node;
        new_root->keys[0] = mid_key;
        new_root->key_count++;
        
        node->parent = new_root;
        new_node->parent = new_root;
        
        return;
    }
    
    // 否则，更新父节点
    int index = 0;
    while (index <= node->parent->key_count && node->parent->ptrs[index] != node)
        index++;
    
    // 在父节点中插入分隔键和指向新节点的指针
    for (int i = node->parent->key_count; i > index; i--)
    {
        node->parent->keys[i] = node->parent->keys[i - 1];
        node->parent->ptrs[i + 1] = node->parent->ptrs[i];
    }
    
    node->parent->keys[index] = mid_key;
    node->parent->ptrs[index + 1] = new_node;
    node->parent->key_count++;
    new_node->parent = node->parent;
}

// 修复insert_non_full函数，确保条目计数正确更新
void insert_non_full(BPlusNode *node, Value *key, RID rid, BPlusTree *tree)
{
    if (node == NULL || key == NULL || tree == NULL)
        return;
    
    // 检查键是否已存在
    for (int i = 0; i < node->key_count; i++)
    {
        if (compareValues(node->keys[i], key) == 0)
        {
            // 键已存在，不执行插入
            free(key);
            return;
        }
    }
    
    if (node->is_leaf)
    {
        // 在叶节点中插入
        int pos = node->key_count;
        while (pos > 0 && compareValues(node->keys[pos - 1], key) > 0)
        {
            node->keys[pos] = node->keys[pos - 1];
            node->rids[pos] = node->rids[pos - 1];
            pos--;
        }
        
        node->keys[pos] = key;
        node->rids[pos] = rid;
        node->key_count++;
        tree->entry_count++;
        
        // 检查是否需要分裂
        if (node->key_count >= node->max_keys)
            split_leaf(node, tree);
    }
    else
    {
        // 在非叶节点中找到合适的子节点
        int pos = node->key_count;
        while (pos > 0 && compareValues(node->keys[pos - 1], key) > 0)
            pos--;
        
        // 递归插入到子节点
        insert_non_full(node->ptrs[pos], key, rid, tree);
        
        // 检查子节点插入后是否导致父节点需要分裂
        if (node->key_count >= node->max_keys)
            split_non_leaf(node, tree);
    }
}

// 搜索键
bool search(BPlusNode *root, Value *key, RID *rid)
{
    if (root == NULL || key == NULL)
        return false;
    
    BPlusNode *leaf = find_leaf_node(root, key, NULL);
    if (leaf == NULL)
        return false;
    
    int index = search_in_leaf(leaf, key);
    if (index == -1)
        return false;
    
    if (rid != NULL)
        *rid = leaf->rids[index];
    
    return true;
}

// 获取有效键数量
int get_valid_key_count(BPlusNode *node)
{
    if (node == NULL)
        return 0;
    
    return node->key_count;
}

// 获取最小键数
int get_min_keys(int max_keys, bool is_root)
{
    if (is_root)
        return 1;
    
    return (max_keys + 1) / 2;
}

// 从左侧兄弟借键（叶节点）
void borrow_from_left_sibling_leaf(BPlusNode *node, BPlusNode *left_sibling)
{
    if (node == NULL || left_sibling == NULL || !node->is_leaf || !left_sibling->is_leaf)
        return;
    
    // 将左侧兄弟的最后一个键移动到当前节点的第一个位置
    for (int i = node->key_count; i > 0; i--)
    {
        node->keys[i] = node->keys[i - 1];
        node->rids[i] = node->rids[i - 1];
    }
    
    node->keys[0] = left_sibling->keys[left_sibling->key_count - 1];
    node->rids[0] = left_sibling->rids[left_sibling->key_count - 1];
    node->key_count++;
    
    left_sibling->key_count--;
    
    // 更新父节点中的分隔键
    if (node->parent != NULL)
    {
        int index = 0;
        while (index < node->parent->key_count && node->parent->ptrs[index + 1] != node)
            index++;
        
        if (index < node->parent->key_count)
        {
            free(node->parent->keys[index]);
            node->parent->keys[index] = copyValue(node->keys[0]);
        }
    }
}

// 从右侧兄弟借键（叶节点）
void borrow_from_right_sibling_leaf(BPlusNode *node, BPlusNode *right_sibling)
{
    if (node == NULL || right_sibling == NULL || !node->is_leaf || !right_sibling->is_leaf)
        return;
    
    // 将右侧兄弟的第一个键移动到当前节点的最后一个位置
    node->keys[node->key_count] = right_sibling->keys[0];
    node->rids[node->key_count] = right_sibling->rids[0];
    node->key_count++;
    
    // 从右侧兄弟中移除第一个键
    for (int i = 0; i < right_sibling->key_count - 1; i++)
    {
        right_sibling->keys[i] = right_sibling->keys[i + 1];
        right_sibling->rids[i] = right_sibling->rids[i + 1];
    }
    
    right_sibling->key_count--;
    
    // 更新父节点中的分隔键
    if (node->parent != NULL)
    {
        int index = 0;
        while (index < node->parent->key_count && node->parent->ptrs[index] != node)
            index++;
        
        if (index < node->parent->key_count)
        {
            free(node->parent->keys[index]);
            node->parent->keys[index] = copyValue(right_sibling->keys[0]);
        }
    }
}

// 从左侧兄弟借键（非叶节点）
void borrow_from_left_sibling_non_leaf(BPlusNode *node, BPlusNode *left_sibling)
{
    if (node == NULL || left_sibling == NULL || node->is_leaf || left_sibling->is_leaf)
        return;
    
    // 获取父节点中分隔两个兄弟节点的键的索引
    int separator_index = 0;
    while (separator_index < node->parent->key_count && node->parent->ptrs[separator_index + 1] != node)
        separator_index++;
    
    // 将分隔键下移到当前节点
    for (int i = node->key_count; i > 0; i--)
        node->keys[i] = node->keys[i - 1];
    
    for (int i = node->key_count + 1; i > 0; i--)
        node->ptrs[i] = node->ptrs[i - 1];
    
    node->keys[0] = node->parent->keys[separator_index];
    node->ptrs[0] = left_sibling->ptrs[left_sibling->key_count];
    node->ptrs[0]->parent = node;
    node->key_count++;
    
    // 将左侧兄弟的最后一个键上移到父节点作为分隔键
    free(node->parent->keys[separator_index]);
    node->parent->keys[separator_index] = left_sibling->keys[left_sibling->key_count - 1];
    
    left_sibling->key_count--;
}

// 从右侧兄弟借键（非叶节点）
void borrow_from_right_sibling_non_leaf(BPlusNode *node, BPlusNode *right_sibling)
{
    if (node == NULL || right_sibling == NULL || node->is_leaf || right_sibling->is_leaf)
        return;
    
    // 获取父节点中分隔两个兄弟节点的键的索引
    int separator_index = 0;
    while (separator_index < node->parent->key_count && node->parent->ptrs[separator_index] != node)
        separator_index++;
    
    // 将分隔键下移到当前节点
    node->keys[node->key_count] = node->parent->keys[separator_index];
    node->ptrs[node->key_count + 1] = right_sibling->ptrs[0];
    node->ptrs[node->key_count + 1]->parent = node;
    node->key_count++;
    
    // 将右侧兄弟的第一个键上移到父节点作为分隔键
    free(node->parent->keys[separator_index]);
    node->parent->keys[separator_index] = right_sibling->keys[0];
    
    // 从右侧兄弟中移除第一个键
    for (int i = 0; i < right_sibling->key_count - 1; i++)
        right_sibling->keys[i] = right_sibling->keys[i + 1];
    
    for (int i = 0; i < right_sibling->key_count; i++)
        right_sibling->ptrs[i] = right_sibling->ptrs[i + 1];
    
    right_sibling->key_count--;
}

// 合并叶节点
void merge_leaf_nodes(BPlusNode *left, BPlusNode *right)
{
    if (left == NULL || right == NULL || !left->is_leaf || !right->is_leaf)
        return;
    
    // 获取父节点中分隔两个节点的键的索引
    int separator_index = 0;
    if (left->parent != NULL)
    {
        while (separator_index < left->parent->key_count && left->parent->ptrs[separator_index + 1] != right)
            separator_index++;
        
        // 释放分隔键
        free(left->parent->keys[separator_index]);
    }
    
    // 合并键和RID
    for (int i = 0; i < right->key_count; i++)
    {
        left->keys[left->key_count] = right->keys[i];
        left->rids[left->key_count] = right->rids[i];
        left->key_count++;
    }
    
    // 更新链表指针
    left->next = right->next;
    if (left->next != NULL)
        left->next->prev = left;
    
    // 释放右侧节点
    free(right->keys);
    free(right->rids);
    free(right);
}

// 合并非叶节点
void merge_non_leaf_nodes(BPlusNode *left, BPlusNode *right, Value *separator_key)
{
    if (left == NULL || right == NULL || left->is_leaf || right->is_leaf)
        return;
    
    // 将分隔键下移到左侧节点
    left->keys[left->key_count] = separator_key;
    left->key_count++;
    
    // 合并键和指针
    for (int i = 0; i < right->key_count; i++)
    {
        left->keys[left->key_count] = right->keys[i];
        left->ptrs[left->key_count] = right->ptrs[i];
        left->ptrs[left->key_count]->parent = left;
        left->key_count++;
    }
    
    // 合并最后一个指针
    left->ptrs[left->key_count] = right->ptrs[right->key_count];
    left->ptrs[left->key_count]->parent = left;
    
    // 释放右侧节点
    free(right->keys);
    free(right->ptrs);
    free(right);
}

// 处理节点下溢
void handle_underflow(BPlusNode *node, BPlusTree *tree)
{
    if (node == NULL || node == tree->root || node->key_count >= get_min_keys(node->max_keys, node == tree->root))
        return;
    
    int index = 0;
    BPlusNode *parent = node->parent;
    
    // 找到当前节点在父节点中的索引
    while (index <= parent->key_count && parent->ptrs[index] != node)
        index++;
    
    // 检查左侧兄弟
    if (index > 0)
    {
        BPlusNode *left_sibling = parent->ptrs[index - 1];
        if (left_sibling->key_count > get_min_keys(left_sibling->max_keys, left_sibling == tree->root))
        {
            if (node->is_leaf)
                borrow_from_left_sibling_leaf(node, left_sibling);
            else
                borrow_from_left_sibling_non_leaf(node, left_sibling);
            
            return;
        }
    }
    
    // 检查右侧兄弟
    if (index < parent->key_count)
    {
        BPlusNode *right_sibling = parent->ptrs[index + 1];
        if (right_sibling->key_count > get_min_keys(right_sibling->max_keys, right_sibling == tree->root))
        {
            if (node->is_leaf)
                borrow_from_right_sibling_leaf(node, right_sibling);
            else
                borrow_from_right_sibling_non_leaf(node, right_sibling);
            
            return;
        }
    }
    
    // 需要合并节点
    if (index > 0)
    {
        // 与左侧兄弟合并
        BPlusNode *left_sibling = parent->ptrs[index - 1];
        Value *separator_key = parent->keys[index - 1];
        
        if (node->is_leaf)
            merge_leaf_nodes(left_sibling, node);
        else
            merge_non_leaf_nodes(left_sibling, node, separator_key);
        
        // 从父节点中移除指向当前节点的指针和分隔键
        for (int i = index; i < parent->key_count; i++)
        {
            parent->keys[i - 1] = parent->keys[i];
            parent->ptrs[i] = parent->ptrs[i + 1];
        }
        
        parent->key_count--;
        tree->node_count--;
        
        // 处理父节点可能的下溢
        handle_underflow(parent, tree);
    }
    else if (index < parent->key_count)
    {
        // 与右侧兄弟合并
        BPlusNode *right_sibling = parent->ptrs[index + 1];
        Value *separator_key = parent->keys[index];
        
        if (node->is_leaf)
            merge_leaf_nodes(node, right_sibling);
        else
            merge_non_leaf_nodes(node, right_sibling, separator_key);
        
        // 从父节点中移除指向右侧兄弟的指针和分隔键
        for (int i = index + 1; i < parent->key_count; i++)
        {
            parent->keys[i - 1] = parent->keys[i];
            parent->ptrs[i] = parent->ptrs[i + 1];
        }
        
        parent->key_count--;
        tree->node_count--;
        
        // 处理父节点可能的下溢
        handle_underflow(parent, tree);
    }
}

// 删除节点中的键
void delete_node(BPlusNode *node, Value *key, BPlusTree *tree)
{
    if (node == NULL || key == NULL)
        return;
    
    if (node->is_leaf)
    {
        // 在叶节点中找到键的索引
        int index = search_in_leaf(node, key);
        if (index == -1)
            return;
        
        // 删除键和对应的RID
        free(node->keys[index]);
        
        for (int i = index; i < node->key_count - 1; i++)
        {
            node->keys[i] = node->keys[i + 1];
            node->rids[i] = node->rids[i + 1];
        }
        
        node->key_count--;
        tree->entry_count--;
        
        // 处理可能的下溢
        handle_underflow(node, tree);
    }
    else
    {
        // 找到键应该在的子节点
        int index = 0;
        while (index < node->key_count && compareValues(node->keys[index], key) < 0)
            index++;
        
        if (index < node->key_count && compareValues(node->keys[index], key) == 0)
        {
            // 键在当前节点中，找到前驱键
            BPlusNode *predecessor = node->ptrs[index];
            while (!predecessor->is_leaf)
                predecessor = predecessor->ptrs[predecessor->key_count];
            
            // 用前驱键替换当前键
            free(node->keys[index]);
            node->keys[index] = copyValue(predecessor->keys[predecessor->key_count - 1]);
            
            // 递归删除前驱键
            delete_node(node->ptrs[index], predecessor->keys[predecessor->key_count - 1], tree);
        }
        else
        {
            // 递归删除子节点中的键
            delete_node(node->ptrs[index], key, tree);
        }
        
        // 处理可能的下溢
        handle_underflow(node, tree);
    }
    
    // 如果根节点的键数为0，且有子节点，更新根节点
    if (node == tree->root && node->key_count == 0 && !node->is_leaf)
    {
        BPlusNode *new_root = node->ptrs[0];
        new_root->parent = NULL;
        
        free(node->keys);
        free(node->ptrs);
        free(node);
        
        tree->root = new_root;
        tree->node_count--;
    }
}

// 修复printValue函数，确保正确打印键值
void printValue(char *buf, Value *val)
{
    if (buf == NULL || val == NULL)
        return;
    
    switch(val->dt)
    {
        case DT_INT:
            sprintf(buf, "%d", val->v.intV);
            break;
        case DT_STRING:
            sprintf(buf, "%s", val->v.stringV ? val->v.stringV : "(null)");
            break;
        case DT_FLOAT:
            sprintf(buf, "%f", val->v.floatV);
            break;
        case DT_BOOL:
            sprintf(buf, "%s", val->v.boolV ? "true" : "false");
            break;
        default:
            sprintf(buf, "<unknown type %d>", val->dt);
            break;
    }
}

// 删除键
void delete_key(BPlusNode *root, Value *key, BPlusTree *tree)
{
    if (root == NULL || key == NULL)
        return;
    
    // 检查键是否存在
    bool found = false;
    find_leaf_node(root, key, &found);
    
    if (found)
    {
        // 记录删除操作
        char key_str[100];
        printValue(key_str, key);
        log_message("Deleting key: %s\n", key_str);
        console_log("Deleting key: %s\n", key_str);
        
        // 执行删除
        delete_node(root, key, tree);
        
        // 记录删除后的树结构
        log_message("Delete operation completed.\n");
        console_log("Delete operation completed.\n");
    }
}

// 扫描操作相关结构
typedef struct ScanState {
    BPlusNode *current_node;
    int current_index;
} ScanState;

// 修复initIndexManager函数，创建日志文件
RC initIndexManager (void *mgmtData)
{
    // 打开日志文件
    log_file = fopen("btree_log.txt", "w");
    if (log_file == NULL)
    {
        // 无法创建日志文件，但继续执行
        fprintf(stderr, "Warning: Could not open log file 'btree_log.txt'\n");
    }
    
    // 初始化全局最大键数量为默认值
    GLOBAL_MAX_KEYS = 2; // 默认值，可被createBtree覆盖
    
    return RC_OK;
}

// 修复shutdownIndexManager函数，清理索引信息
RC shutdownIndexManager ()
{
    if (log_file != NULL)
    {
        fclose(log_file);
        log_file = NULL;
    }
    
    // 释放索引信息
    if (currentIndexInfo != NULL)
    {
        free(currentIndexInfo->idxId);
        free(currentIndexInfo);
        currentIndexInfo = NULL;
    }
    
    return RC_OK;
}

// 修复createBtree函数，确保正确保存索引信息
RC createBtree (char *idxId, DataType keyType, int n)
{
    // 验证参数
    if (idxId == NULL || n <= 0)
        return RC_INVALID_PARAMS;
    
    // 释放旧的索引信息
    if (currentIndexInfo != NULL)
    {
        free(currentIndexInfo->idxId);
        free(currentIndexInfo);
        currentIndexInfo = NULL;
    }
    
    // 创建新的索引信息
    currentIndexInfo = (IndexInfo*)malloc(sizeof(IndexInfo));
    if (currentIndexInfo == NULL)
        return RC_OUT_OF_MEMORY;
    
    currentIndexInfo->idxId = strdup(idxId);
    if (currentIndexInfo->idxId == NULL)
    {
        free(currentIndexInfo);
        currentIndexInfo = NULL;
        return RC_OUT_OF_MEMORY;
    }
    
    currentIndexInfo->keyType = keyType;
    currentIndexInfo->maxKeys = n;
    
    // 记录操作
    log_message("Creating B+ tree: %s, key type: %d, max keys: %d\n", 
                idxId, keyType, n);
    console_log("Creating B+ tree: %s, key type: %d, max keys: %d\n", 
                idxId, keyType, n);
    
    // 设置全局最大键数量
    GLOBAL_MAX_KEYS = n;
    
    return RC_OK;
}

// 修复openBtree函数，确保正确恢复索引信息
RC openBtree (BTreeHandle **tree, char *idxId)
{
    // 验证参数
    if (tree == NULL || idxId == NULL)
        return RC_INVALID_PARAMS;
    
    // 创建BTreeHandle
    *tree = (BTreeHandle*)malloc(sizeof(BTreeHandle));
    if (*tree == NULL)
        return RC_OUT_OF_MEMORY;
    
    // 初始化BTreeHandle
    if (currentIndexInfo != NULL && strcmp(currentIndexInfo->idxId, idxId) == 0)
    {
        (*tree)->keyType = currentIndexInfo->keyType;
        GLOBAL_MAX_KEYS = currentIndexInfo->maxKeys;
    }
    else
    {
        (*tree)->keyType = DT_INT; // 默认类型
        GLOBAL_MAX_KEYS = 2; // 默认最大键数
    }
    
    (*tree)->idxId = strdup(idxId);
    if ((*tree)->idxId == NULL)
    {
        free(*tree);
        return RC_OUT_OF_MEMORY;
    }
    
    // 记录操作
    log_message("Opening B+ tree: %s, key type: %d, max keys: %d\n", 
                idxId, (*tree)->keyType, GLOBAL_MAX_KEYS);
    console_log("Opening B+ tree: %s, key type: %d, max keys: %d\n", 
                idxId, (*tree)->keyType, GLOBAL_MAX_KEYS);
    
    // 创建B+树
    BPlusTree *bptree = init_bplus_tree((*tree)->keyType, GLOBAL_MAX_KEYS);
    if (bptree == NULL)
    {
        free((*tree)->idxId);
        free(*tree);
        return RC_OUT_OF_MEMORY;
    }
    
    (*tree)->mgmtData = bptree;
    
    return RC_OK;
}

RC closeBtree (BTreeHandle *tree)
{
    // 验证参数
    if (tree == NULL)
        return RC_INVALID_PARAMS;
    
    // 释放B+树
    if (tree->mgmtData != NULL)
    {
        BPlusTree *bptree = (BPlusTree*)tree->mgmtData;
        free_bplus_tree(bptree->root);
        free(bptree);
        tree->mgmtData = NULL;
    }
    
    // 释放BTreeHandle
    free(tree->idxId);
    free(tree);
    
    // 记录操作
    log_message("Closing B+ tree.\n");
    console_log("Closing B+ tree.\n");
    
    return RC_OK;
}

RC deleteBtree (char *idxId)
{
    // 验证参数
    if (idxId == NULL)
        return RC_INVALID_PARAMS;
    
    // 记录操作
    log_message("Deleting B+ tree: %s\n", idxId);
    console_log("Deleting B+ tree: %s\n", idxId);
    
    return RC_OK;
}

// 修复getNumNodes函数，确保返回正确的节点计数
RC getNumNodes (BTreeHandle *tree, int *result)
{
    if (tree == NULL || tree->mgmtData == NULL || result == NULL)
        return RC_INVALID_PARAMS;
    
    BPlusTree *bptree = (BPlusTree*)tree->mgmtData;
    *result = bptree->node_count;
    
    return RC_OK;
}

RC getNumEntries (BTreeHandle *tree, int *result)
{
    // 验证参数
    if (tree == NULL || tree->mgmtData == NULL || result == NULL)
        return RC_INVALID_PARAMS;
    
    BPlusTree *bptree = (BPlusTree*)tree->mgmtData;
    *result = bptree->entry_count;
    
    return RC_OK;
}

RC getKeyType (BTreeHandle *tree, DataType *result)
{
    // 验证参数
    if (tree == NULL || result == NULL)
        return RC_INVALID_PARAMS;
    
    *result = tree->keyType;
    
    return RC_OK;
}

// 修复findKey函数，确保正确处理键搜索
RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
    // 验证参数
    if (tree == NULL || tree->mgmtData == NULL || key == NULL || result == NULL)
        return RC_INVALID_PARAMS;
    
    BPlusTree *bptree = (BPlusTree*)tree->mgmtData;
    
    // 记录操作
    char key_str[100];
    printValue(key_str, key);
    log_message("Finding key: %s\n", key_str);
    console_log("Finding key: %s\n", key_str);
    
    // 初始化结果RID
    result->page = 0;
    result->slot = 0;
    
    // 搜索键
    if (search(bptree->root, key, result))
    {
        // 记录找到结果
        log_message("Key found: page=%d, slot=%d\n", result->page, result->slot);
        console_log("Key found: page=%d, slot=%d\n", result->page, result->slot);
        
        return RC_OK;
    }
    
    // 键不存在
    log_message("Key not found: %s\n", key_str);
    console_log("Key not found: %s\n", key_str);
    
    return RC_IM_KEY_NOT_FOUND;
}

// 修复insert函数，确保节点计数正确更新
void insert(BPlusNode *root, Value *key, RID rid, BPlusTree *tree)
{
    if (root == NULL || key == NULL || tree == NULL)
        return;
    
    // 检查根节点是否已满
    if (root->key_count >= root->max_keys)
    {
        // 创建新的根节点
        BPlusNode *new_root = create_node(false, root->max_keys);
        if (new_root == NULL)
            return;
        
        new_root->ptrs[0] = root;
        root->parent = new_root;
        
        tree->root = new_root;
        tree->node_count++;
        
        // 分裂原根节点
        split_non_leaf(root, tree);
        
        // 确保正确插入键
        if (compareValues(key, tree->root->keys[0]) < 0)
        {
            insert_non_full(tree->root->ptrs[0], key, rid, tree);
        }
        else
        {
            insert_non_full(tree->root->ptrs[1], key, rid, tree);
        }
    }
    else
    {
        // 在当前根节点中插入键
        insert_non_full(root, key, rid, tree);
    }
}

// 修复insertKey函数，确保正确处理键类型和RID
RC insertKey (BTreeHandle *handle, Value *key, RID rid)
{
    // 验证参数
    if (handle == NULL || handle->mgmtData == NULL || key == NULL)
        return RC_INVALID_PARAMS;
    
    BPlusTree *bptree = (BPlusTree*)handle->mgmtData;
    
    // 检查键类型是否匹配
    if (key->dt != handle->keyType)
        return RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE;
    
    // 检查键是否已存在
    bool found = false;
    find_leaf_node(bptree->root, key, &found);
    if (found)
        return RC_IM_KEY_ALREADY_EXISTS;
    
    // 记录操作
    char key_str[100];
    printValue(key_str, key);
    log_message("Inserting key: %s, RID: (%d,%d)\n", key_str, rid.page, rid.slot);
    console_log("Inserting key: %s, RID: (%d,%d)\n", key_str, rid.page, rid.slot);
    
    // 创建键的副本
    Value *key_copy = copyValue(key);
    if (key_copy == NULL)
        return RC_OUT_OF_MEMORY;
    
    // 调用内部的insert函数执行实际插入
    insert(bptree->root, key_copy, rid, bptree);
    
    // 记录插入完成后的状态
    log_message("Insertion completed. Current node count: %d, entry count: %d\n", 
                bptree->node_count, bptree->entry_count);
    console_log("Insertion completed. Current node count: %d, entry count: %d\n", 
                bptree->node_count, bptree->entry_count);
    
    return RC_OK;
}

RC deleteKey (BTreeHandle *tree, Value *key)
{
    // 验证参数
    if (tree == NULL || tree->mgmtData == NULL || key == NULL)
        return RC_INVALID_PARAMS;
    
    BPlusTree *bptree = (BPlusTree*)tree->mgmtData;
    
    // 检查键是否存在
    bool found = false;
    find_leaf_node(bptree->root, key, &found);
    if (!found)
        return RC_IM_KEY_NOT_FOUND;
    
    // 删除键
    delete_key(bptree->root, key, bptree);
    
    return RC_OK;
}

RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
    // 验证参数
    if (tree == NULL || tree->mgmtData == NULL || handle == NULL)
        return RC_INVALID_PARAMS;
    
    BPlusTree *bptree = (BPlusTree*)tree->mgmtData;
    
    // 创建扫描句柄
    *handle = (BT_ScanHandle*)malloc(sizeof(BT_ScanHandle));
    if (*handle == NULL)
        return RC_OUT_OF_MEMORY;
    
    // 初始化扫描状态
    ScanState *scan_state = (ScanState*)malloc(sizeof(ScanState));
    if (scan_state == NULL)
    {
        free(*handle);
        return RC_OUT_OF_MEMORY;
    }
    
    // 找到第一个叶节点
    BPlusNode *current_node = bptree->root;
    while (current_node != NULL && !current_node->is_leaf)
        current_node = current_node->ptrs[0];
    
    scan_state->current_node = current_node;
    scan_state->current_index = 0;
    
    (*handle)->tree = tree;
    (*handle)->mgmtData = scan_state;
    
    // 记录操作
    log_message("Opening tree scan.\n");
    console_log("Opening tree scan.\n");
    
    return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result)
{
    // 验证参数
    if (handle == NULL || handle->mgmtData == NULL || result == NULL)
        return RC_INVALID_PARAMS;
    
    ScanState *scan_state = (ScanState*)handle->mgmtData;
    
    // 检查是否还有更多条目
    if (scan_state->current_node == NULL || scan_state->current_index >= scan_state->current_node->key_count)
    {
        // 查找下一个叶节点
        while (scan_state->current_node != NULL && scan_state->current_index >= scan_state->current_node->key_count)
        {
            scan_state->current_node = scan_state->current_node->next;
            scan_state->current_index = 0;
        }
        
        // 没有更多条目
        if (scan_state->current_node == NULL)
            return RC_IM_NO_MORE_ENTRIES;
    }
    
    // 返回当前条目
    *result = scan_state->current_node->rids[scan_state->current_index];
    scan_state->current_index++;
    
    // 记录操作
    char key_str[100];
    printValue(key_str, scan_state->current_node->keys[scan_state->current_index - 1]);
    log_message("Next entry: key=%s, RID=(%d,%d)\n", 
                key_str, result->page, result->slot);
    console_log("Next entry: key=%s, RID=(%d,%d)\n", 
                key_str, result->page, result->slot);
    
    return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle)
{
    // 验证参数
    if (handle == NULL)
        return RC_INVALID_PARAMS;
    
    // 释放扫描状态
    if (handle->mgmtData != NULL)
    {
        free(handle->mgmtData);
        handle->mgmtData = NULL;
    }
    
    // 释放扫描句柄
    free(handle);
    
    // 记录操作
    log_message("Closing tree scan.\n");
    console_log("Closing tree scan.\n");
    
    return RC_OK;
}

// debug and test functions
char *printTree (BTreeHandle *tree)
{
    // 此函数可以根据需要实现，返回树的字符串表示
    return "B+ Tree Structure";
}