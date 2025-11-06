#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "btree_mgr.h"
#include "dberror.h"
#include "dt.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>

#ifdef DEBUG
    #define DEBUG_PRINT(format, ...) printf(format, ##__VA_ARGS__)
#else
    #define DEBUG_PRINT(format, ...)
#endif

// 全局变量：索引管理器状态
bool indexManagerInitialized = false;

// 修复compare_values函数，添加类型检查
static int compare_values(DataType type, Value *a, Value *b) {
    if (a == NULL || b == NULL)
        return 0;
    
    // 确保值的类型与B树定义的类型匹配
    if (a->dt != type || b->dt != type)
        return 0; // 类型不匹配时返回0
    
    switch (type) {
        case DT_INT:
            return a->v.intV - b->v.intV;
        case DT_FLOAT:
            if (a->v.floatV < b->v.floatV) return -1;
            if (a->v.floatV > b->v.floatV) return 1;
            return 0;
        case DT_STRING:
            return strcmp(a->v.stringV, b->v.stringV);
        case DT_BOOL:
            if (a->v.boolV == b->v.boolV) return 0;
            return a->v.boolV ? 1 : -1;
        default:
            return 0;
    }
}

// 修复create_node函数，确保正确初始化keys和ptrs数组
static BPlusNode* create_node(BPlusTree *tree, int is_leaf) {
    BPlusNode *node = (BPlusNode*)malloc(sizeof(BPlusNode));
    if (!node) return NULL;
    
    node->is_leaf = is_leaf;
    node->key_num = 0;
    node->parent = NULL;
    node->next = NULL;
    
    // 为键和指针分配内存
    node->keys = (Value*)malloc(sizeof(Value) * (tree->order - 1));
    node->ptrs = (void**)malloc(sizeof(void*) * tree->order);
    
    if (!node->keys || !node->ptrs) {
        free(node->keys);
        free(node->ptrs);
        free(node);
        return NULL;
    }
    
    // 初始化指针为NULL
    for (int i = 0; i < tree->order; i++) {
        node->ptrs[i] = NULL;
    }
    
    return node;
}

// 查找键在节点中的位置
static int find_key_index(BPlusNode *node, DataType keyType, Value *key) {
    int idx = 0;
    while (idx < node->key_num && compare_values(keyType, &node->keys[idx], key) < 0) {
        idx++;
    }
    return idx;
}

// 修复后的split_node函数
static void split_node(BPlusTree *tree, BPlusNode *parent, int index) {
    BPlusNode *child = (BPlusNode*)parent->ptrs[index];
    BPlusNode *new_node = create_node(tree, child->is_leaf);
    tree->nodeCount++;
    
    int mid = (tree->order - 1) / 2; // 计算中间位置
    
    // 对于order=2，mid=0，意味着我们需要特殊处理
    int keys_to_move = tree->order - 1 - mid - 1;
    if (keys_to_move < 0) keys_to_move = 0;
    
    // 复制后半部分键到新节点
    for (int i = 0; i < keys_to_move; i++) {
        new_node->keys[i] = child->keys[mid + 1 + i];
        new_node->ptrs[i] = child->ptrs[mid + 1 + i];
        
        // 如果是非叶节点，更新子节点的父指针
        if (!child->is_leaf) {
            ((BPlusNode*)new_node->ptrs[i])->parent = new_node;
        }
    }
    
    // 复制最后一个指针
    if (keys_to_move >= 0) {
        new_node->ptrs[keys_to_move] = child->ptrs[mid + 1 + keys_to_move];
        
        // 如果是非叶节点，更新子节点的父指针
        if (!child->is_leaf && new_node->ptrs[keys_to_move] != NULL) {
            ((BPlusNode*)new_node->ptrs[keys_to_move])->parent = new_node;
        }
    }
    
    new_node->key_num = keys_to_move;
    new_node->parent = parent;
    
    // 更新子节点的键数量
    child->key_num = mid + 1;
    
    // 移动父节点的指针
    for (int i = parent->key_num; i > index; i--) {
        parent->ptrs[i + 1] = parent->ptrs[i];
    }
    parent->ptrs[index + 1] = new_node;
    
    // 移动父节点的键
    for (int i = parent->key_num - 1; i >= index; i--) {
        parent->keys[i + 1] = parent->keys[i];
    }
    
    // 根据child是否为叶节点来决定使用哪个键作为分隔键
    if (child->is_leaf) {
        // 叶节点：使用中间键作为分隔键
        parent->keys[index] = child->keys[mid];
    } else {
        // 非叶节点：使用中间键作为分隔键，并保留该键在子节点中
        parent->keys[index] = child->keys[mid];
    }
    parent->key_num++;
    
    // 如果是叶节点，更新链表指针
    if (child->is_leaf) {
        new_node->next = child->next;
        child->next = new_node;
    }
}

// 修复insert_non_full函数，确保正确实现"先插入后判断"原则
static void insert_non_full(BPlusTree *tree, BPlusNode *node, Value *key, RID rid) {
    int i = node->key_num - 1;
    
    if (node->is_leaf) {
        // 叶节点：找到插入位置并移动键
        while (i >= 0 && compare_values(tree->keyType, key, &node->keys[i]) < 0) {
            node->keys[i + 1] = node->keys[i];
            node->ptrs[i + 1] = node->ptrs[i];
            i--;
        }
        
        // 插入新键和RID
        node->keys[i + 1] = *key;
        RID *newRid = (RID*)malloc(sizeof(RID));
        *newRid = rid;
        node->ptrs[i + 1] = newRid;
        node->key_num++;
        tree->entryCount++;
    } else {
        // 非叶节点：找到子节点并递归插入
        while (i >= 0 && compare_values(tree->keyType, key, &node->keys[i]) < 0) {
            i--;
        }
        i++;
        
        BPlusNode *child = (BPlusNode*)node->ptrs[i];
        
        // 检查子节点是否已满，如果已满则先分裂
        if (child->key_num == tree->order - 1) {
            split_node(tree, node, i);
            
            // 确定应该插入到哪个子节点
            if (compare_values(tree->keyType, key, &node->keys[i]) > 0) {
                i++;
            }
        }
        
        // 递归插入到适当的子节点
        insert_non_full(tree, (BPlusNode*)node->ptrs[i], key, rid);
    }
}

// 修复find_leaf_node函数
static BPlusNode* find_leaf_node(BPlusTree *tree, Value *key) {
    BPlusNode *current = tree->root;
    while (!current->is_leaf) {
        int i = 0;
        while (i < current->key_num && compare_values(tree->keyType, key, &current->keys[i]) > 0) {
            i++;
        }
        current = (BPlusNode*)current->ptrs[i];
    }
    return current;
}

// 修复handle_underflow函数
static void handle_underflow(BPlusTree *tree, BPlusNode *node) {
    if (node == tree->root) {
        // 根节点下溢处理
        if (node->key_num == 0 && !node->is_leaf) {
            // 根节点变为唯一的子节点
            BPlusNode *new_root = (BPlusNode*)node->ptrs[0];
            new_root->parent = NULL;
            tree->root = new_root;
            free(node->keys);
            free(node->ptrs);
            free(node);
            tree->nodeCount--;
        }
        return;
    }
    
    BPlusNode *parent = node->parent;
    int idx = 0;
    
    // 找到当前节点在父节点中的索引
    while (idx <= parent->key_num && parent->ptrs[idx] != node) {
        idx++;
    }
    
    // 尝试从左兄弟借键
    if (idx > 0) {
        BPlusNode *left_sibling = (BPlusNode*)parent->ptrs[idx - 1];
        if (left_sibling->key_num > (tree->order - 1) / 2) {
            // 左兄弟有多余的键可以借出
            if (node->is_leaf) {
                // 叶节点：从左兄弟借最后一个键
                for (int i = node->key_num - 1; i >= 0; i--) {
                    node->keys[i + 1] = node->keys[i];
                    node->ptrs[i + 1] = node->ptrs[i];
                    i--;
                }
                node->keys[0] = left_sibling->keys[left_sibling->key_num - 1];
                node->ptrs[0] = left_sibling->ptrs[left_sibling->key_num - 1];
                node->key_num++;
                
                // 更新父节点的分隔键
                parent->keys[idx - 1] = left_sibling->keys[left_sibling->key_num - 1];
                
                left_sibling->key_num--;
            } else {
                // 非叶节点：从左兄弟借键
                // 为新键和指针腾出空间
                for (int i = node->key_num; i > 0; i--) {
                    node->keys[i] = node->keys[i - 1];
                }
                for (int i = node->key_num + 1; i > 0; i--) {
                    node->ptrs[i] = node->ptrs[i - 1];
                }
                
                // 将父节点中的分隔键下移到当前节点
                node->keys[0] = parent->keys[idx - 1];
                
                // 将左侧兄弟节点的最大键上移到父节点
                parent->keys[idx - 1] = left_sibling->keys[left_sibling->key_num - 1];
                
                // 将左侧兄弟节点的最右子节点移到当前节点
                node->ptrs[0] = left_sibling->ptrs[left_sibling->key_num];
                ((BPlusNode*)node->ptrs[0])->parent = node; // 更新父指针
                
                // 更新键数量
                left_sibling->key_num--;
                node->key_num++;
            }
            return;
        }
    }
    
    // 尝试从右兄弟借键
    if (idx < parent->key_num) {
        BPlusNode *right_sibling = (BPlusNode*)parent->ptrs[idx + 1];
        if (right_sibling->key_num > (tree->order - 1) / 2) {
            // 右兄弟有多余的键可以借出
            if (node->is_leaf) {
                // 叶节点：从右兄弟借第一个键
                node->keys[node->key_num] = right_sibling->keys[0];
                node->ptrs[node->key_num] = right_sibling->ptrs[0];
                node->key_num++;
                
                // 更新父节点的分隔键
                parent->keys[idx] = right_sibling->keys[1];
                
                // 移动右兄弟的键
                for (int i = 0; i < right_sibling->key_num - 1; i++) {
                    right_sibling->keys[i] = right_sibling->keys[i + 1];
                    right_sibling->ptrs[i] = right_sibling->ptrs[i + 1];
                }
                right_sibling->key_num--;
            } else {
                // 非叶节点：从右兄弟借键
                // 将父节点中的分隔键下移到当前节点
                node->keys[node->key_num] = parent->keys[idx];
                
                // 将右侧兄弟节点的最小键上移到父节点
                parent->keys[idx] = right_sibling->keys[0];
                
                // 将右侧兄弟节点的最左子节点移到当前节点
                node->ptrs[node->key_num + 1] = right_sibling->ptrs[0];
                ((BPlusNode*)node->ptrs[node->key_num + 1])->parent = node; // 更新父指针
                
                // 移动右侧兄弟节点的剩余键和指针
                for (int i = 0; i < right_sibling->key_num - 1; i++) {
                    right_sibling->keys[i] = right_sibling->keys[i + 1];
                }
                for (int i = 0; i < right_sibling->key_num; i++) {
                    right_sibling->ptrs[i] = right_sibling->ptrs[i + 1];
                }
                right_sibling->key_num--;
            }
            return;
        }
    }
    
    // 无法借键，合并节点
    if (idx > 0) {
        // 与左兄弟合并
        BPlusNode *left_sibling = (BPlusNode*)parent->ptrs[idx - 1];
        
        if (node->is_leaf) {
            // 合并叶节点
            for (int i = 0; i < node->key_num; i++) {
                left_sibling->keys[left_sibling->key_num + i] = node->keys[i];
                left_sibling->ptrs[left_sibling->key_num + i] = node->ptrs[i];
            }
            left_sibling->key_num += node->key_num;
            left_sibling->next = node->next;
            
            // 释放节点
            free(node->keys);
            free(node->ptrs);
            free(node);
            tree->nodeCount--;
        } else {
            // 合并非叶节点
            left_sibling->keys[left_sibling->key_num] = parent->keys[idx - 1];
            
            for (int i = 0; i < node->key_num; i++) {
                left_sibling->keys[left_sibling->key_num + 1 + i] = node->keys[i];
                left_sibling->ptrs[left_sibling->key_num + 1 + i] = node->ptrs[i];
                if (node->ptrs[i]) {
                    ((BPlusNode*)node->ptrs[i])->parent = left_sibling;
                }
            }
            left_sibling->ptrs[left_sibling->key_num + 1 + node->key_num] = node->ptrs[node->key_num];
            if (node->ptrs[node->key_num]) {
                ((BPlusNode*)node->ptrs[node->key_num])->parent = left_sibling;
            }
            left_sibling->key_num += node->key_num + 1;
            
            // 释放节点
            free(node->keys);
            free(node->ptrs);
            free(node);
            tree->nodeCount--;
        }
        
        // 更新父节点
        for (int i = idx; i < parent->key_num; i++) {
            parent->keys[i - 1] = parent->keys[i];
            parent->ptrs[i] = parent->ptrs[i + 1];
        }
        parent->key_num--;
    } else {
        // 与右兄弟合并
        BPlusNode *right_sibling = (BPlusNode*)parent->ptrs[idx + 1];
        
        if (node->is_leaf) {
            // 合并叶节点
            for (int i = 0; i < right_sibling->key_num; i++) {
                node->keys[node->key_num + i] = right_sibling->keys[i];
                node->ptrs[node->key_num + i] = right_sibling->ptrs[i];
            }
            node->key_num += right_sibling->key_num;
            node->next = right_sibling->next;
            
            // 释放右兄弟
            free(right_sibling->keys);
            free(right_sibling->ptrs);
            free(right_sibling);
            tree->nodeCount--;
        } else {
            // 合并非叶节点
            node->keys[node->key_num] = parent->keys[idx];
            
            for (int i = 0; i < right_sibling->key_num; i++) {
                node->keys[node->key_num + 1 + i] = right_sibling->keys[i];
                node->ptrs[node->key_num + 1 + i] = right_sibling->ptrs[i];
                if (right_sibling->ptrs[i]) {
                    ((BPlusNode*)right_sibling->ptrs[i])->parent = node;
                }
            }
            node->ptrs[node->key_num + 1 + right_sibling->key_num] = right_sibling->ptrs[right_sibling->key_num];
            if (right_sibling->ptrs[right_sibling->key_num]) {
                ((BPlusNode*)right_sibling->ptrs[right_sibling->key_num])->parent = node;
            }
            node->key_num += right_sibling->key_num + 1;
            
            // 释放右兄弟
            free(right_sibling->keys);
            free(right_sibling->ptrs);
            free(right_sibling);
            tree->nodeCount--;
        }
        
        // 更新父节点
        for (int i = idx + 1; i < parent->key_num; i++) {
            parent->keys[i - 1] = parent->keys[i];
            parent->ptrs[i] = parent->ptrs[i + 1];
        }
        parent->key_num--;
    }
    
    // 检查父节点是否下溢
    if (parent->key_num < (tree->order - 1) / 2) {
        handle_underflow(tree, parent);
    }
}

// 初始化索引管理器
RC initIndexManager (void *mgmtData)
{
    if (indexManagerInitialized)
        return RC_OK;
    
    // 初始化存储管理器
    initStorageManager();
    
    indexManagerInitialized = true;
    return RC_OK;
}

// 关闭索引管理器
RC shutdownIndexManager ()
{
    if (!indexManagerInitialized)
        return RC_OK;
    
    indexManagerInitialized = false;
    return RC_OK;
}

// 创建B树索引
RC createBtree (char *idxId, DataType keyType, int n)
{
    // 添加详细调试信息
    if (!indexManagerInitialized || idxId == NULL || n < 2)
        return RC_INVALID_PARAMS;
    
    // 创建索引文件
    RC rc = createPageFile(idxId);
    if (rc != RC_OK)
        return rc;
    
    // 打开文件写入B树元数据
    SM_FileHandle fh;
    rc = openPageFile(idxId, &fh);
    if (rc != RC_OK)
        return rc;
    
    // 写入B树元数据（键类型和阶数）
    PageNumber pageNum = 0;
    SM_PageHandle ph = (SM_PageHandle)malloc(PAGE_SIZE);
    if (ph == NULL) {  // 内存分配失败
        closePageFile(&fh);
        return RC_MEMORY_ALLOC_FAILED;
    }
    
    // 修正ensureCapacity参数顺序
    rc = ensureCapacity(pageNum + 1, &fh);
    if (rc != RC_OK) {
        free(ph);
        closePageFile(&fh);
        return rc;
    }
    
    rc = readBlock(pageNum, &fh, ph);
    if (rc != RC_OK) {
        free(ph);
        closePageFile(&fh);
        return rc;
    }
    
    // 存储元数据：键类型和阶数
    memcpy(ph, &keyType, sizeof(DataType));
    memcpy(ph + sizeof(DataType), &n, sizeof(int));
    
    rc = writeBlock(pageNum, &fh, ph);
    if (rc != RC_OK) {
        free(ph);
        closePageFile(&fh);
        return rc;
    }
    
    free(ph);
    closePageFile(&fh);
    return RC_OK;
}

// 打开B树索引
RC openBtree (BTreeHandle **tree, char *idxId)
{
    // 添加详细调试信息
    printf("[DEBUG] openBtree called. indexManagerInitialized=%d, tree=%p, idxId=%p\n", 
           indexManagerInitialized, (void*)tree, (void*)idxId);
    
    if (tree != NULL) {
        printf("[DEBUG] *tree value before: %p\n", (void*)*tree);
    }
    
    if (idxId != NULL) {
        printf("[DEBUG] idxId value: %s\n", idxId);
    }
    
    if (!indexManagerInitialized) {
        printf("[DEBUG] ERROR: indexManagerInitialized is false\n");
        return RC_INVALID_PARAMS;
    }
    
    if (tree == NULL) {
        printf("[DEBUG] ERROR: tree pointer is NULL\n");
        return RC_INVALID_PARAMS;
    }
    
    if (idxId == NULL) {
        printf("[DEBUG] ERROR: idxId is NULL\n");
        return RC_INVALID_PARAMS;
    }
    
    // 分配BTreeHandle
    *tree = (BTreeHandle*)malloc(sizeof(BTreeHandle));
    if (*tree == NULL) {
        printf("[DEBUG] ERROR: malloc for BTreeHandle failed\n");
        return RC_MEMORY_ALLOC_FAILED;
    }
    
    printf("[DEBUG] BTreeHandle allocated successfully at %p\n", (void*)*tree);

    // 初始化BTreeHandle
    (*tree)->idxId = strdup(idxId);
    (*tree)->mgmtData = malloc(sizeof(BPlusTree));
    if ((*tree)->mgmtData == NULL) {
        free((*tree)->idxId);
        free(*tree);
        printf("[DEBUG] ERROR: malloc for BPlusTree failed\n");
        return RC_MEMORY_ALLOC_FAILED; 
    }
    printf("[DEBUG] BPlusTree allocated successfully\n");

    BPlusTree *btree = (BPlusTree*)(*tree)->mgmtData;
    
    // 打开索引文件读取元数据
    SM_FileHandle fh;
    RC rc = openPageFile(idxId, &fh);
    if (rc != RC_OK) {
        free((*tree)->idxId);
        free((*tree)->mgmtData);
        free(*tree);
        return rc;
    }
    
    // 读取元数据
    PageNumber pageNum = 0;
    SM_PageHandle ph = (SM_PageHandle)malloc(PAGE_SIZE);
    if (ph == NULL) {
        closePageFile(&fh);
        free((*tree)->idxId);
        free((*tree)->mgmtData);
        free(*tree);
        return RC_MEMORY_ALLOC_FAILED;
    }
    
    rc = readBlock(pageNum, &fh, ph);
    if (rc != RC_OK) {
        free(ph);
        closePageFile(&fh);
        free((*tree)->idxId);
        free((*tree)->mgmtData);
        free(*tree);
        return rc;
    }
    
    // 解析元数据
    DataType keyType;
    int order;
    memcpy(&keyType, ph, sizeof(DataType));
    memcpy(&order, ph + sizeof(DataType), sizeof(int));
    
    (*tree)->keyType = keyType;
    btree->keyType = keyType;
    btree->order = order;
    btree->nodeCount = 1;  // 初始只有根节点
    btree->entryCount = 0;
    
    // 创建初始根节点
    btree->root = create_node(btree, 1);  // 初始根节点是叶节点
    if (btree->root == NULL) {
        free(ph);
        closePageFile(&fh);
        free((*tree)->idxId);
        free((*tree)->mgmtData);
        free(*tree);
        return RC_RM_CREATE_NODE_FAILED;  // 替换RC_ERROR为RC_FAIL
    }
    
    free(ph);
    closePageFile(&fh);
    return RC_OK;
}

// 关闭B树索引
RC closeBtree (BTreeHandle *tree)
{
    if (!indexManagerInitialized || tree == NULL)
        return RC_INVALID_PARAMS;
    
    BPlusTree *btree = (BPlusTree*)tree->mgmtData;
    
    // 递归释放所有节点
    BPlusNode **queue = (BPlusNode**)malloc(sizeof(BPlusNode*) * btree->nodeCount);
    int front = 0, rear = 0;
    
    if (btree->root != NULL) {
        queue[rear++] = btree->root;
    }
    
    while (front < rear) {
        BPlusNode *node = queue[front++];
        
        // 如果是非叶节点，将子节点加入队列
        if (!node->is_leaf) {
            for (int i = 0; i <= node->key_num; i++) {
                if (node->ptrs[i] != NULL) {
                    queue[rear++] = (BPlusNode*)node->ptrs[i];
                }
            }
        } else {
            // 叶节点释放RID指针
            for (int i = 0; i < node->key_num; i++) {
                free(node->ptrs[i]);
            }
        }
        
        // 释放键和指针数组
        free(node->keys);
        free(node->ptrs);
        free(node);
    }
    
    free(queue);
    
    // 释放BTreeHandle
    free(tree->idxId);
    free(tree->mgmtData);
    free(tree);
    
    return RC_OK;
}

// 删除B树索引
RC deleteBtree (char *idxId)
{
    if (!indexManagerInitialized || idxId == NULL)
        return RC_INVALID_PARAMS;
    
    // 删除索引文件
    return destroyPageFile(idxId);
}

// 获取节点数量
RC getNumNodes (BTreeHandle *tree, int *result)
{
    if (!indexManagerInitialized || tree == NULL || result == NULL)
        return RC_INVALID_PARAMS;
    
    BPlusTree *btree = (BPlusTree*)tree->mgmtData;
    *result = btree->nodeCount;
    return RC_OK;
}

// 获取条目数量
RC getNumEntries (BTreeHandle *tree, int *result)
{
    if (!indexManagerInitialized || tree == NULL || result == NULL)
        return RC_INVALID_PARAMS;
    
    BPlusTree *btree = (BPlusTree*)tree->mgmtData;
    *result = btree->entryCount;
    return RC_OK;
}

// 获取键类型
RC getKeyType (BTreeHandle *tree, DataType *result)
{
    if (!indexManagerInitialized || tree == NULL || result == NULL)
        return RC_INVALID_PARAMS;
    
    *result = tree->keyType;
    return RC_OK;
}

// 查找键
RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
    if (!indexManagerInitialized || tree == NULL || key == NULL || result == NULL)
        return RC_INVALID_PARAMS;
    
    BPlusTree *btree = (BPlusTree*)tree->mgmtData;
    BPlusNode *leaf = find_leaf_node(btree, key);
    
    // 在叶节点中查找键
    int idx = find_key_index(leaf, btree->keyType, key);
    if (idx < leaf->key_num && compare_values(btree->keyType, &leaf->keys[idx], key) == 0) {
        *result = *(RID*)leaf->ptrs[idx];
        return RC_OK;
    }
    
    return RC_IM_KEY_NOT_FOUND;
}

// 修复后的insertKey函数，采用标准B+树插入流程
RC insertKey (BTreeHandle *tree, Value *key, RID rid) {
    printf("[DEBUG] insertKey: 函数被调用\n");
    
    if (!indexManagerInitialized) {
        printf("[DEBUG] insertKey: 索引管理器未初始化\n");
        return RC_INVALID_PARAMS;
    }
    
    if (tree == NULL) {
        printf("[DEBUG] insertKey: tree为NULL\n");
        return RC_INVALID_PARAMS;
    }
    
    if (key == NULL) {
        printf("[DEBUG] insertKey: key为NULL\n");
        return RC_INVALID_PARAMS;
    }
    
    // 添加详细的调试信息
    printf("[DEBUG] insertKey: tree->keyType = %d\n", tree->keyType);
    printf("[DEBUG] insertKey: key->dt = %d\n", key->dt);
    
    // 检查键类型匹配
    if (key->dt != tree->keyType) {
        printf("[DEBUG] insertKey: 类型不匹配！期望 %d, 实际 %d\n", tree->keyType, key->dt);
        
        // 打印更详细的键信息，帮助诊断问题
        if (key->dt == DT_INT) {
            printf("[DEBUG] insertKey: 键是整数类型，值为 %d\n", key->v.intV);
        } else if (key->dt == DT_STRING) {
            printf("[DEBUG] insertKey: 键是字符串类型，值为 %s\n", key->v.stringV);
        } else if (key->dt == DT_FLOAT) {
            printf("[DEBUG] insertKey: 键是浮点数类型，值为 %f\n", key->v.floatV);
        } else if (key->dt == DT_BOOL) {
            printf("[DEBUG] insertKey: 键是布尔类型，值为 %s\n", key->v.boolV ? "true" : "false");
        }
        
        return RC_INVALID_PARAMS;
    }
    
    // 如果类型匹配，继续执行插入操作
    BPlusTree *btree = (BPlusTree*)tree->mgmtData;
    printf("[DEBUG] insertKey: btree->keyType = %d\n", btree->keyType);
    printf("[DEBUG] insertKey: btree->order = %d\n", btree->order);
    
    // 其余代码保持不变
    BPlusNode *root = btree->root;
    printf("[DEBUG] insertKey: 根节点存在? %s\n", (root ? "是" : "否"));
    
    // 检查键是否已存在
    BPlusNode *leaf = find_leaf_node(btree, key);
    int idx = find_key_index(leaf, btree->keyType, key);
    if (idx < leaf->key_num && compare_values(btree->keyType, &leaf->keys[idx], key) == 0) {
        printf("[DEBUG] insertKey: 键已存在\n");
        return RC_IM_KEY_ALREADY_EXISTS;
    }
    
    // 标准B+树插入流程：先检查根节点是否已满，如果已满则先分裂
    if (root->key_num == btree->order - 1) {
        printf("[DEBUG] insertKey: 根节点已满，创建新根节点并分裂\n");
        BPlusNode *new_root = create_node(btree, 0);
        btree->nodeCount++;
        btree->root = new_root;
        new_root->ptrs[0] = root;
        root->parent = new_root;
        split_node(btree, new_root, 0);
        
        // 调用insert_non_full插入键
        insert_non_full(btree, new_root, key, rid);
    } else {
        // 根节点未满，直接调用insert_non_full插入键
        insert_non_full(btree, root, key, rid);
    }
    
    printf("[DEBUG] insertKey: 成功\n");
    return RC_OK;
}

// 修复deleteKey函数
RC deleteKey (BTreeHandle *tree, Value *key) {
    if (!indexManagerInitialized || tree == NULL || key == NULL)
        return RC_INVALID_PARAMS;
    
    BPlusTree *btree = (BPlusTree*)tree->mgmtData;
    
    // 查找包含该键的叶子节点
    BPlusNode *leaf = find_leaf_node(btree, key);
    
    // 查找键
    int idx = find_key_index(leaf, btree->keyType, key);
    if (idx >= leaf->key_num || compare_values(btree->keyType, &leaf->keys[idx], key) != 0) {
        return RC_IM_KEY_NOT_FOUND;
    }
    
    // 如果是叶节点，直接删除
    if (leaf->is_leaf) {
        // 释放RID
        free(leaf->ptrs[idx]);
        
        // 移动键和指针
        for (int i = idx; i < leaf->key_num - 1; i++) {
            leaf->keys[i] = leaf->keys[i + 1];
            leaf->ptrs[i] = leaf->ptrs[i + 1];
        }
        leaf->key_num--;
        btree->entryCount--;
        
        // 检查是否下溢
        if (leaf != btree->root && leaf->key_num < (btree->order - 1) / 2) {
            handle_underflow(btree, leaf);
        }
    } else {
        // 非叶节点，需要用后继键替换
        BPlusNode* successor_node = (BPlusNode*)leaf->ptrs[idx + 1];
        while (!successor_node->is_leaf) {
            successor_node = (BPlusNode*)successor_node->ptrs[0];
        }
        
        // 保存后继键和对应的数据指针
        Value successor_key = successor_node->keys[0];
        void* successor_ptr = successor_node->ptrs[0];
        
        // 用后继键替换当前键
        leaf->keys[idx] = successor_key;
        leaf->ptrs[idx] = successor_ptr;
        
        // 删除后继节点中的键
        for (int i = 0; i < successor_node->key_num - 1; i++) {
            successor_node->keys[i] = successor_node->keys[i + 1];
            successor_node->ptrs[i] = successor_node->ptrs[i + 1];
        }
        successor_node->key_num--;
        btree->entryCount--;
        
        // 检查后继节点是否下溢
        if (successor_node != btree->root && successor_node->key_num < (btree->order - 1) / 2) {
            handle_underflow(btree, successor_node);
        }
    }
    
    // 如果根节点没有键但有一个子节点，更新根节点
    if (btree->root->key_num == 0 && !btree->root->is_leaf) {
        BPlusNode* new_root = (BPlusNode*)btree->root->ptrs[0];
        new_root->parent = NULL;
        free(btree->root->keys);
        free(btree->root->ptrs);
        free(btree->root);
        btree->root = new_root;
        btree->nodeCount--;
    }
    
    return RC_OK;
}

// 打开树扫描
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
    if (!indexManagerInitialized || tree == NULL || handle == NULL)
        return RC_INVALID_PARAMS;
    
    // 分配扫描句柄
    *handle = (BT_ScanHandle*)malloc(sizeof(BT_ScanHandle));
    if (*handle == NULL)
        return RC_MEMORY_ALLOC_FAILED;
    
    // 初始化扫描句柄
    (*handle)->tree = tree;
    (*handle)->mgmtData = malloc(sizeof(BT_ScanMgmt));
    if ((*handle)->mgmtData == NULL) {
        free(*handle);
        return RC_INVALID_PARAMS;  // 替换RC_ERROR为RC_FAIL
    }
    BT_ScanMgmt *scanMgmt = (BT_ScanMgmt*)(*handle)->mgmtData;
    
    BPlusTree *btree = (BPlusTree*)tree->mgmtData;
    
    // 找到最左的叶节点
    BPlusNode *current = btree->root;
    while (!current->is_leaf) {
        current = (BPlusNode*)current->ptrs[0];
    }
    
    scanMgmt->currentNode = current;
    scanMgmt->currentPos = 0;
    
    return RC_OK;
}

// 获取下一个条目
RC nextEntry (BT_ScanHandle *handle, RID *result)
{
    if (!indexManagerInitialized || handle == NULL || result == NULL)
        return RC_INVALID_PARAMS;
    
    BT_ScanMgmt *scanMgmt = (BT_ScanMgmt*)handle->mgmtData;
    
    // 检查是否还有条目
    if (scanMgmt->currentNode == NULL || scanMgmt->currentPos >= scanMgmt->currentNode->key_num) {
        // 移动到下一个叶节点
        scanMgmt->currentNode = scanMgmt->currentNode->next;
        scanMgmt->currentPos = 0;
        
        // 检查是否所有节点都已扫描
        if (scanMgmt->currentNode == NULL) {
            return RC_IM_NO_MORE_ENTRIES;
        }
    }
    
    // 返回当前条目
    *result = *(RID*)scanMgmt->currentNode->ptrs[scanMgmt->currentPos];
    scanMgmt->currentPos++;
    
    return RC_OK;
}

// 关闭树扫描
RC closeTreeScan (BT_ScanHandle *handle)
{
    if (!indexManagerInitialized || handle == NULL)
        return RC_INVALID_PARAMS;
    
    // 释放扫描管理数据
    free(handle->mgmtData);
    free(handle);
    
    return RC_OK;
}

// 修复printTree函数，确保正确输出树结构
char *printTree (BTreeHandle *tree) {
    if (!indexManagerInitialized || tree == NULL)
        return NULL;
    
    BPlusTree *btree = (BPlusTree*)tree->mgmtData;
    static char buffer[1024 * 1024];  // 静态缓冲区存储打印结果
    buffer[0] = '\0';
    
    // 使用队列进行广度优先遍历
    BPlusNode **queue = (BPlusNode**)malloc(sizeof(BPlusNode*) * btree->nodeCount);
    int front = 0, rear = 0;
    
    if (btree->root != NULL) {
        queue[rear++] = btree->root;
    }
    
    strcat(buffer, "B+ Tree Structure:\n");
    
    int level = 0;
    while (front < rear) {
        int levelSize = rear - front;
        sprintf(buffer + strlen(buffer), "Level: %d\n", level++);
        
        for (int i = 0; i < levelSize; i++) {
            BPlusNode *node = queue[front++];
            
            sprintf(buffer + strlen(buffer), "  Node (%s): keys=[", node->is_leaf ? "leaf" : "internal");
            
            for (int j = 0; j < node->key_num; j++) {
                char keyStr[50];
                switch (btree->keyType) {
                    case DT_INT:
                        sprintf(keyStr, "%d", node->keys[j].v.intV);
                        break;
                    case DT_FLOAT:
                        sprintf(keyStr, "%.2f", node->keys[j].v.floatV);
                        break;
                    case DT_STRING:
                        sprintf(keyStr, "%s", node->keys[j].v.stringV);
                        break;
                    case DT_BOOL:
                        sprintf(keyStr, "%s", node->keys[j].v.boolV ? "true" : "false");
                        break;
                    default:
                        strcpy(keyStr, "unknown");
                }
                strcat(buffer, keyStr);
                if (j < node->key_num - 1) {
                    strcat(buffer, ", ");
                }
            }
            
            strcat(buffer, "]\n");
            
            // 非叶节点的子节点加入队列
            if (!node->is_leaf) {
                for (int j = 0; j <= node->key_num; j++) {
                    if (node->ptrs[j] != NULL) {
                        queue[rear++] = (BPlusNode*)node->ptrs[j];
                    }
                }
            }
        }
    }
    
    free(queue);
    return buffer;
}