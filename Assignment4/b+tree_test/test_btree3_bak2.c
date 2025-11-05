#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// 配置参数（节点最大键数）
#define MAX_KEYS 2  // 支持奇数/偶数测试
#define DELETED_KEY -1  // 已删除键的标记值

// B+树节点结构体
typedef struct BPlusNode {
    int is_leaf;                  // 1=叶节点, 0=非叶节点
    int key_num;                  // 当前键数量
    int keys[MAX_KEYS + 1];       // 预留1个位置，因为我们要先插入，再判断是否分裂~！！！！
    void* ptrs[MAX_KEYS + 2];     // 指针数组：叶节点含next指针
    struct BPlusNode* parent;     // 父节点指针
} BPlusNode;

// B+树结构体
typedef struct BPlusTree {
    BPlusNode* root;              // 根节点
} BPlusTree;

// 函数声明
BPlusNode* create_node(int is_leaf);
void init_bplus_tree(BPlusTree* tree);
void free_bplus_tree(BPlusNode* node);
int find_key_index(BPlusNode* node, int key);
void split_leaf(BPlusNode* parent, int index);
void split_non_leaf(BPlusNode* parent, int index);
void insert_non_full(BPlusNode* node, int key, void* data);
void insert(BPlusTree* tree, int key, void* data);
void* search(BPlusNode* node, int key);

// void redistribute_leaf(BPlusNode* parent, int curr_idx);
// void redistribute_non_leaf(BPlusNode* parent, int curr_idx);
// void redistribute_leaf_from_right(BPlusNode* parent, int curr_idx);

void delete_key(BPlusTree* tree, int key);
void print_bplus_tree(BPlusTree* tree, const char* operation_desc, FILE* log_file);
void log_message(const char* msg, FILE* log_file);
int get_tree_height(BPlusNode* node);
void print_level_nodes(BPlusNode* node, int current_level, int target_level, FILE* log_file);
int get_node_index_in_parent(BPlusNode* node);

int get_min_keys(BPlusNode* node);
void delete_leaf_key(BPlusTree* tree, BPlusNode* node, int key);
void delete_non_leaf_key(BPlusTree* tree, BPlusNode* node, int key);
void handle_underflow(BPlusTree* tree, BPlusNode* node);
void redistribute_leaf_from_left(BPlusNode *node, BPlusNode *left_sibling, BPlusNode *parent, int index);
void redistribute_non_leaf_from_left(BPlusNode *node, BPlusNode *left_sibling, BPlusNode *parent, int index);
void redistribute_non_leaf_from_right(BPlusNode *node, BPlusNode *right_sibling, BPlusNode *parent, int index);
void merge_leaf(BPlusNode *left, BPlusNode *right, BPlusNode *parent, int index);
void merge_non_leaf(BPlusNode *left, BPlusNode *right, BPlusNode *parent, int index);
void delete_node(BPlusTree *tree, BPlusNode *node, int key);
void validate_parent_pointers(BPlusTree *tree);
void validate_min_keys_constraint(BPlusTree *tree);


// 日志输出（同时输出到控制台和文件）
void log_message(const char* msg, FILE* log_file) {
    printf("%s", msg);
    fprintf(log_file, "%s", msg);
}

// 获取节点在父节点中的索引（辅助打印）
// 修复后的get_node_index_in_parent函数 - 加强安全检查
int get_node_index_in_parent(BPlusNode* node) {
    // 基础安全检查
    if (!node || !node->parent) return -1;
    
    BPlusNode* parent = node->parent;
    
    // 额外安全检查：确保父节点的key_num有效
    if (parent->key_num < 0 || parent->key_num > MAX_KEYS) {
        return -1; // 父节点状态异常
    }
    
    // 限制循环范围在MAX_KEYS+1以内，防止数组越界
    int max_ptr_index = parent->key_num;
    if (max_ptr_index > MAX_KEYS) {
        max_ptr_index = MAX_KEYS; // 防止访问超出数组范围
    }
    
    // 只在安全的范围内查找
    for (int i = 0; i <= max_ptr_index; i++) {
        // 每次访问ptrs[i]前都进行NULL检查
        if (parent->ptrs[i] == node) {
            return i;
        }
    }
    
    // 未找到节点在父节点中的索引，这通常表示树结构已损坏
    return -1;
}
// 获取树的高度（根节点为第1层）
int get_tree_height(BPlusNode* node) {
    if (!node) return 0;
    if (node->is_leaf) return 1;
    
    // 确保ptrs[0]有效再递归
    if (node->ptrs[0] == NULL) return 1;
    
    return 1 + get_tree_height((BPlusNode*)node->ptrs[0]);  // 非叶节点高度=1+子节点高度
}
// 辅助函数：打印特定层级的所有节点
void print_level_nodes(BPlusNode* node, int current_level, int target_level, FILE* log_file) {
    if (!node) return;
    
    // 到达目标层，打印节点信息
    if (current_level == target_level) {
        char msg[1024];
        snprintf(msg, sizeof(msg), "  Level %d | %s | Parent Index: %d | Key Count: %d | Keys: [ ",
                current_level, node->is_leaf ? "Leaf Node" : "Non-Leaf Node",
                node->parent ? get_node_index_in_parent(node) : -1, node->key_num);
        log_message(msg, log_file);
        
        // 打印键信息
        int valid_key_count = 0;
        for (int i = 0; i < node->key_num; i++) {
            // 只显示数据不为NULL的有效键
            if (node->ptrs[i] != NULL) {
                if (valid_key_count > 0) {
                    log_message(", ", log_file);
                }
                snprintf(msg, sizeof(msg), "%d", node->keys[i]);
                log_message(msg, log_file);
                valid_key_count++;
            }
        }
        log_message(" ] | Pointers: [ ", log_file);
        
        // 打印指针信息
        int valid_ptr_count = 0;
        if (node->is_leaf) {
            // 叶节点指针是数据指针和下一个叶节点
            for (int i = 0; i < node->key_num; i++) {
                if (node->ptrs[i] != NULL) {
                    if (valid_ptr_count > 0) {
                        log_message(", ", log_file);
                    }
                    snprintf(msg, sizeof(msg), "data=%d", *((int*)node->ptrs[i]));
                    log_message(msg, log_file);
                    valid_ptr_count++;
                }
            }
            // 打印next_leaf指针 - 关键点：加强安全检查
            if (MAX_KEYS + 1 < sizeof(node->ptrs)/sizeof(node->ptrs[0])) {
                if (node->ptrs[MAX_KEYS + 1] != NULL) {
                    if (valid_ptr_count > 0) {
                        log_message(", ", log_file);
                    }
                    snprintf(msg, sizeof(msg), "next_leaf=%p", node->ptrs[MAX_KEYS + 1]);
                    log_message(msg, log_file);
                    valid_ptr_count++;
                }
            }
        } else {
            // 非叶节点指针都是子节点指针
            for (int i = 0; i <= node->key_num; i++) {
                if (node->ptrs[i] != NULL) {
                    if (valid_ptr_count > 0) {
                        log_message(", ", log_file);
                    }
                    snprintf(msg, sizeof(msg), "child=%p", node->ptrs[i]);
                    log_message(msg, log_file);
                    valid_ptr_count++;
                }
            }
        }
        log_message(" ]\n", log_file);
    } else if (current_level < target_level) {
        // 非目标层，继续递归 - 增强检查，确保安全访问
        if (!node->is_leaf) {
            for (int i = 0; i <= node->key_num; i++) {
                if (node->ptrs[i] != NULL) { // 添加NULL检查，防止段错误
                    print_level_nodes((BPlusNode*)node->ptrs[i], current_level + 1, target_level, log_file);
                }
            }
        }
    }
}
// 创建新节点
BPlusNode* create_node(int is_leaf) {
    BPlusNode* node = (BPlusNode*)malloc(sizeof(BPlusNode));
    if (!node) {
        perror("内存分配失败");
        exit(EXIT_FAILURE);
    }
    node->is_leaf = is_leaf;
    node->key_num = 0;
    node->parent = NULL;
    memset(node->keys, 0, sizeof(node->keys));
    memset(node->ptrs, 0, sizeof(node->ptrs));
    return node;
}
// 初始化B+树
void init_bplus_tree(BPlusTree* tree) {
    tree->root = create_node(1);  // 空树的根为叶节点
}
// 释放B+树内存
void free_bplus_tree(BPlusNode* node) {
    if (!node) return;
    
    // 递归释放所有子节点
    if (!node->is_leaf) {
        for (int i = 0; i <= node->key_num; i++) {
            if (node->ptrs[i]) {
                free_bplus_tree((BPlusNode*)node->ptrs[i]);
            }
        }
    }
    
    free(node);
}
// 辅助函数：查找包含目标键的叶节点
// 修复后的find_leaf_node函数，确保在删除操作后仍能正确定位叶子节点
BPlusNode* find_leaf_node(BPlusNode* node, int key) {
    if (!node) return NULL;
    
    if (node->is_leaf) {
        return node;
    }
    
    int idx = 0;
    // 跳过已删除的键，确保导航正确
    while (idx < node->key_num) {
        if (key >= node->keys[idx]) {
            idx++;
        } else {
            break;
        }
    }
    
    // 确保ptr[idx]有效
    if (idx > node->key_num || !node->ptrs[idx]) {
        idx = node->key_num;
    }
    
    return find_leaf_node((BPlusNode*)node->ptrs[idx], key);
}
// 辅助函数： 在叶节点内查找键。
// 修复后的search_in_leaf函数，确保在删除操作后仍能正确搜索
void* search_in_leaf(BPlusNode* leaf, int key) {
    if (!leaf || !leaf->is_leaf) return NULL;
    
    // 首先找到链表的第一个节点，确保搜索范围覆盖所有可能的节点
    BPlusNode* first_leaf = leaf;
    while (first_leaf && first_leaf->parent) {
        // 这是一个简化的实现，实际需要根据父节点找到链表的第一个节点
        // 在实际应用中，可能需要额外的字段来跟踪链表的头节点
        break;
    }
    
    // 从第一个叶节点开始，沿着链表遍历直到找到目标键或确定不存在
    BPlusNode* current = first_leaf;
    while (current) {
        // 检查当前叶节点中的所有键
        for (int i = 0; i < current->key_num; i++) {
            // 跳过已删除的键（key=-1或ptr=NULL）
            if (current->keys[i] == -1 || current->ptrs[i] == NULL) {
                continue;
            }
            
            if (current->keys[i] == key) {
                return current->ptrs[i]; // 找到目标键
            }
            // 如果当前键已经大于目标键，可以提前退出，因为叶节点中的键是有序的
            if (current->keys[i] > key) {
                return NULL; // 由于键是有序的，后面不会再找到
            }
        }
        
        // 继续在链表中下一个叶节点查找
        current = (BPlusNode*)current->ptrs[MAX_KEYS + 1];
    }
    
    return NULL; // 遍历完整个链表都没找到
}
// 修复后的find_key_index函数，处理删除标记
int find_key_index(BPlusNode* node, int key) {
    int idx = 0;
    while (idx < node->key_num) {
        // 跳过已删除的键（key=-1或ptr=NULL）
        if ((node->is_leaf && (node->keys[idx] == -1 || node->ptrs[idx] == NULL))) {
            idx++;
            continue;
        }
        
        // 找到第一个不小于key的位置
        if (node->keys[idx] >= key) {
            break;
        }
        idx++;
    }
    return idx;
}
// 修复split_leaf函数，正确处理叶节点分裂和分隔键设置
// 修复split_leaf函数，考虑删除标记
void split_leaf(BPlusNode* parent, int index) {
    BPlusNode* curr_leaf = (BPlusNode*)parent->ptrs[index];
    BPlusNode* new_leaf = create_node(1);
    new_leaf->parent = parent;

    // 先压缩当前节点，移除所有已删除的键
    int valid_pos = 0;
    for (int i = 0; i < curr_leaf->key_num; i++) {
        if (curr_leaf->keys[i] != -1 && curr_leaf->ptrs[i] != NULL) {
            if (valid_pos != i) {
                curr_leaf->keys[valid_pos] = curr_leaf->keys[i];
                curr_leaf->ptrs[valid_pos] = curr_leaf->ptrs[i];
                // 将原位置标记为已删除
                curr_leaf->keys[i] = -1;
                curr_leaf->ptrs[i] = NULL;
            }
            valid_pos++;
        }
    }
    curr_leaf->key_num = valid_pos;

    // 计算左节点键数（奇数n均分，偶数n左多1）
    int left_key_cnt = (MAX_KEYS % 2 == 1) ? 
        (MAX_KEYS + 1) / 2 : 
        (MAX_KEYS / 2) + 1;

    // 复制右半部分键和数据到新叶节点
    new_leaf->key_num = (MAX_KEYS + 1) - left_key_cnt;
    for (int i = 0; i < new_leaf->key_num; i++) {
        int src_idx = left_key_cnt + i;
        if (src_idx >= MAX_KEYS + 1) break; // 防御性检查
        new_leaf->keys[i] = curr_leaf->keys[src_idx];
        new_leaf->ptrs[i] = curr_leaf->ptrs[src_idx];
    }

    // 更新叶节点链表指针
    new_leaf->ptrs[MAX_KEYS + 1] = curr_leaf->ptrs[MAX_KEYS + 1];
    curr_leaf->ptrs[MAX_KEYS + 1] = new_leaf;
    curr_leaf->key_num = left_key_cnt; // 更新原叶节点的键数量

    // 更新父节点（插入新键和新叶节点指针）
    // 根据MAX_KEYS奇偶性选择正确的分隔键
    int separator_key;
    if (MAX_KEYS % 2 == 1) {
        // 奇数情况：使用左节点最后一个键
        separator_key = curr_leaf->keys[curr_leaf->key_num - 1];
    } else {
        // 偶数情况：使用右节点第一个键
        separator_key = new_leaf->keys[0];
    }
    
    for (int i = parent->key_num; i > index; i--) {
        parent->keys[i] = parent->keys[i - 1];
        parent->ptrs[i + 1] = parent->ptrs[i];
    }
    parent->keys[index] = separator_key;  // 父节点存入正确的分隔键
    parent->ptrs[index + 1] = new_leaf;
    parent->key_num++;
}
// 分裂非叶节点
void split_non_leaf(BPlusNode* parent, int index) {
    BPlusNode* curr_non_leaf = (BPlusNode*)parent->ptrs[index];
    BPlusNode* new_non_leaf = create_node(0);
    new_non_leaf->parent = parent;

    // 计算中间键索引
    int mid = (MAX_KEYS % 2 == 1) ? (MAX_KEYS + 1) / 2 : MAX_KEYS / 2;
    int middle_key = curr_non_leaf->keys[mid];

    // 复制右半部分键和子节点到新非叶节点（避免越界）
    new_non_leaf->key_num = MAX_KEYS - mid;
    for (int i = 0; i < new_non_leaf->key_num; i++) {
        int src_idx = mid + 1 + i;
        if (src_idx >= MAX_KEYS + 1) break; // 防御性检查
        new_non_leaf->keys[i] = curr_non_leaf->keys[src_idx];
        new_non_leaf->ptrs[i] = curr_non_leaf->ptrs[src_idx];
        if (new_non_leaf->ptrs[i]) {
            ((BPlusNode*)new_non_leaf->ptrs[i])->parent = new_non_leaf;
        }
    }
    // 复制最后一个子节点指针
    int last_src_idx = mid + 1 + new_non_leaf->key_num;
    if (last_src_idx <= MAX_KEYS + 1) { // 防御性检查
        new_non_leaf->ptrs[new_non_leaf->key_num] = curr_non_leaf->ptrs[last_src_idx];
        if (new_non_leaf->ptrs[new_non_leaf->key_num]) {
            ((BPlusNode*)new_non_leaf->ptrs[new_non_leaf->key_num])->parent = new_non_leaf;
        }
    }

    curr_non_leaf->key_num = mid;  // 左节点保留前mid个键

    // 更新父节点（插入中间键和新子节点指针）
    for (int i = parent->key_num; i > index; i--) {
        parent->keys[i] = parent->keys[i - 1];
        parent->ptrs[i + 1] = parent->ptrs[i];
    }
    parent->keys[index] = middle_key;
    parent->ptrs[index + 1] = new_non_leaf;
    parent->key_num++;
}
// 修复insert_non_full函数，确保正确实现"先插入后判断"原则
void insert_non_full(BPlusNode* node, int key, void* data) {
    int idx = node->key_num - 1;

    // 检查键是否已存在（避免重复插入）
    int exist_idx = find_key_index(node, key);
    if (exist_idx < node->key_num && node->keys[exist_idx] == key) {
        char msg[64];
        snprintf(msg, sizeof(msg), "警告：键 %d 已存在，跳过插入\n", key);
        log_message(msg, stdout); // 输出警告
        return;
    }

    if (node->is_leaf) {
        // 叶节点：移动键和指针，插入新键
        while (idx >= 0 && key < node->keys[idx]) {
            node->keys[idx + 1] = node->keys[idx];
            node->ptrs[idx + 1] = node->ptrs[idx];
            idx--;
        }
        node->keys[idx + 1] = key;
        node->ptrs[idx + 1] = data;
        node->key_num++;
    } else {
        // 非叶节点：找到子节点并递归插入
        while (idx >= 0 && key < node->keys[idx]) {
            idx--;
        }
        idx++;
        BPlusNode* child = (BPlusNode*)node->ptrs[idx];
        
        // 先递归插入，然后处理可能的分裂（这才是真正的"先插入后判断"）
        insert_non_full(child, key, data);
        
        // 子节点插入后若溢出，分裂子节点
        if (child->key_num > MAX_KEYS) {
            if (child->is_leaf) {
                split_leaf(node, idx);
            } else {
                split_non_leaf(node, idx);
            }
        }
    }
}
// 修复insert函数，确保根节点处理正确
void insert(BPlusTree* tree, int key, void* data) {
    BPlusNode* root = tree->root;

    // 先执行插入操作
    insert_non_full(root, key, data);

    // 插入后，若根节点溢出，分裂根节点
    if (root->key_num > MAX_KEYS) {
        BPlusNode* new_root = create_node(0);
        tree->root = new_root;
        new_root->ptrs[0] = root;
        root->parent = new_root;

        if (root->is_leaf) {
            split_leaf(new_root, 0);
        } else {
            split_non_leaf(new_root, 0);
        }
    }
}

// 充分利用叶节点链表的优化搜索函数
void* search(BPlusNode* node, int key) {
    if (!node) return NULL;

    // 第一步：找到包含目标键的叶节点（传统B+树搜索方式）
    BPlusNode* leaf_node = find_leaf_node(node, key);
    if (!leaf_node) return NULL;
    
    // 第二步：在叶节点内查找目标键
    return search_in_leaf(leaf_node, key);
}

// 辅助函数：计算节点中有效的键数量（不包含已删除的键）
int get_valid_key_count(BPlusNode* node) {
    if (!node) return 0;
    
    int count = 0;
    for (int i = 0; i < node->key_num; i++) {
        // 叶节点需要跳过已删除的键
        if (node->is_leaf) {
            if (node->keys[i] != -1 && node->ptrs[i] != NULL) {
                count++;
            }
        } else {
            // 非叶节点不使用删除标记
            count++;
        }
    }
    return count;
}

// 获取节点的最小键数
int get_min_keys(BPlusNode  *node) {
    if (node->is_leaf) {
        // 叶子节点的最小键数 = floor((MAX_KEYS + 1) / 2)
        return (MAX_KEYS + 1) / 2;
    } else {
        // 非叶子节点的最小键数 = ceil((MAX_KEYS + 1) / 2) - 1
        if ((MAX_KEYS + 1) % 2 == 0) {
            return (MAX_KEYS + 1) / 2 - 1;
        } else {
            return (MAX_KEYS + 1 + 1) / 2 - 1;
        }
    }
}

// 修改delete_leaf_key函数，增强对空叶节点的处理
// 删除叶子节点中的键
void delete_leaf_key(BPlusTree *tree, BPlusNode *node, int key) {
    int i, j;
    int key_index = -1;
    
    // 查找键的位置
    for (i = 0; i < node->key_num; i++) {
        if (node->keys[i] == key) {
            key_index = i;
            break;
        }
    }
    
    // 如果找不到键，直接返回
    if (key_index == -1) {
        return;
    }
    
    // 移动键和指针，覆盖被删除的键
    for (j = key_index; j < node->key_num - 1; j++) {
        node->keys[j] = node->keys[j + 1];
        node->ptrs[j] = node->ptrs[j + 1];
    }
    
    // 更新键数量
    node->key_num--;
    node->ptrs[node->key_num] = NULL; // 清空最后一个指针
    
    // 添加调试信息
    char msg[128];
    snprintf(msg, sizeof(msg), "删除叶子节点键 %d 成功，剩余键数量: %d\n", key, node->key_num);
    log_message(msg, stdout); // 临时输出到控制台用于调试
    
    // 检查是否需要处理下溢
    if (node != tree->root && node->key_num < get_min_keys(node)) {
        log_message("叶子节点发生下溢，准备处理\n", stdout); // 临时输出到控制台用于调试
        handle_underflow(tree, node);
    }
}

// 删除非叶子节点中的键
void delete_non_leaf_key(BPlusTree *tree, BPlusNode *node, int key) {
    int i;
    int key_index = -1;
    
    // 查找键的位置
    for (i = 0; i < node->key_num; i++) {
        if (node->keys[i] == key) {
            key_index = i;
            break;
        }
    }
    
    // 如果找不到键，直接返回
    if (key_index == -1) {
        return;
    }
    
    // 对于非叶子节点，我们需要用右子树中的最小键替换被删除的键
    BPlusNode* right_child = (BPlusNode*)node->ptrs[key_index + 1];
    while (!right_child->is_leaf) {
        right_child = (BPlusNode*)right_child->ptrs[0];
    }
    
    // 用右子树的最小键替换当前键
    int successor_key = right_child->keys[0];
    node->keys[key_index] = successor_key;
    
    // 递归删除右子树中的最小键
    delete_leaf_key(tree, right_child, successor_key);
}

// 从左侧兄弟节点重分配键到当前叶节点
void redistribute_leaf_from_left(BPlusNode *node, BPlusNode *left_sibling, BPlusNode *parent, int index) {
    // 安全检查
    if (!node || !left_sibling || !parent) {
        return;
    }
    
    // 从左侧兄弟节点的最右边移动一个键和数据到当前节点的最左边
    // 1. 移动键和数据
    for (int i = node->key_num; i > 0; i--) {
        node->keys[i] = node->keys[i - 1];
        node->ptrs[i] = node->ptrs[i - 1];
    }
    
    // 2. 从左侧兄弟节点获取最右边的键和数据
    node->keys[0] = left_sibling->keys[left_sibling->key_num - 1];
    node->ptrs[0] = left_sibling->ptrs[left_sibling->key_num - 1];
    
    // 3. 更新父节点中的分隔键
    parent->keys[index - 1] = node->keys[0];
    
    // 4. 更新键数量
    left_sibling->key_num--;
    node->key_num++;
    
    // 5. 清空左侧兄弟节点最后一个指针
    left_sibling->ptrs[left_sibling->key_num] = NULL;
    
    char msg[128];
    snprintf(msg, sizeof(msg), "从左侧兄弟节点重分配键到叶节点，当前节点键数量: %d\n", node->key_num);
    log_message(msg, stdout); // 临时输出到控制台用于调试
}

// 从右侧兄弟节点重分配键到当前叶节点
void redistribute_leaf_from_right(BPlusNode *node, BPlusNode *right_sibling, BPlusNode *parent, int index) {
    // 安全检查
    if (!node || !right_sibling || !parent) {
        return;
    }
    
    // 从右侧兄弟节点的最左边移动一个键和数据到当前节点的最右边
    // 1. 获取右侧兄弟节点的第一个键和数据
    node->keys[node->key_num] = right_sibling->keys[0];
    node->ptrs[node->key_num] = right_sibling->ptrs[0];
    
    // 2. 更新父节点中的分隔键
    parent->keys[index] = right_sibling->keys[1]; // 因为右侧兄弟节点的第一个键已经被移动
    
    // 3. 移动右侧兄弟节点的剩余键和指针
    for (int i = 0; i < right_sibling->key_num - 1; i++) {
        right_sibling->keys[i] = right_sibling->keys[i + 1];
        right_sibling->ptrs[i] = right_sibling->ptrs[i + 1];
    }
    
    // 4. 更新键数量
    node->key_num++;
    right_sibling->key_num--;
    
    // 5. 清空右侧兄弟节点最后一个指针
    right_sibling->ptrs[right_sibling->key_num] = NULL;
    
    char msg[128];
    snprintf(msg, sizeof(msg), "从右侧兄弟节点重分配键到叶节点，当前节点键数量: %d\n", node->key_num);
    log_message(msg, stdout); // 临时输出到控制台用于调试
}

// 从左侧兄弟节点重分配键到当前非叶节点
void redistribute_non_leaf_from_left(BPlusNode *node, BPlusNode *left_sibling, BPlusNode *parent, int index) {
    // 安全检查
    if (!node || !left_sibling || !parent) {
        return;
    }
    
    // 1. 为新键和指针腾出空间
    for (int i = node->key_num; i > 0; i--) {
        node->keys[i] = node->keys[i - 1];
    }
    for (int i = node->key_num + 1; i > 0; i--) {
        node->ptrs[i] = node->ptrs[i - 1];
    }
    
    // 2. 将父节点中的分隔键下移到当前节点
    node->keys[0] = parent->keys[index - 1];
    
    // 3. 将左侧兄弟节点的最大键上移到父节点
    parent->keys[index - 1] = left_sibling->keys[left_sibling->key_num - 1];
    
    // 4. 将左侧兄弟节点的最右子节点移到当前节点
    node->ptrs[0] = left_sibling->ptrs[left_sibling->key_num];
    ((BPlusNode*)node->ptrs[0])->parent = node; // 更新父指针
    
    // 5. 更新键数量
    left_sibling->key_num--;
    node->key_num++;
    
    char msg[128];
    snprintf(msg, sizeof(msg), "从左侧兄弟节点重分配键到非叶节点，当前节点键数量: %d\n", node->key_num);
    log_message(msg, stdout); // 临时输出到控制台用于调试
}

// 从右侧兄弟节点重分配键到当前非叶节点
void redistribute_non_leaf_from_right(BPlusNode *node, BPlusNode *right_sibling, BPlusNode *parent, int index) {
    // 安全检查
    if (!node || !right_sibling || !parent) {
        return;
    }
    
    // 1. 将父节点中的分隔键下移到当前节点
    node->keys[node->key_num] = parent->keys[index];
    
    // 2. 将右侧兄弟节点的最小键上移到父节点
    parent->keys[index] = right_sibling->keys[0];
    
    // 3. 将右侧兄弟节点的最左子节点移到当前节点
    node->ptrs[node->key_num + 1] = right_sibling->ptrs[0];
    ((BPlusNode*)node->ptrs[node->key_num + 1])->parent = node; // 更新父指针
    
    // 4. 移动右侧兄弟节点的剩余键和指针
    for (int i = 0; i < right_sibling->key_num - 1; i++) {
        right_sibling->keys[i] = right_sibling->keys[i + 1];
    }
    for (int i = 0; i < right_sibling->key_num; i++) {
        right_sibling->ptrs[i] = right_sibling->ptrs[i + 1];
    }
    
    // 5. 更新键数量
    node->key_num++;
    right_sibling->key_num--;
    
    char msg[128];
    snprintf(msg, sizeof(msg), "从右侧兄弟节点重分配键到非叶节点，当前节点键数量: %d\n", node->key_num);
    log_message(msg, stdout); // 临时输出到控制台用于调试
}

// 合并两个叶节点
void merge_leaf(BPlusNode *left, BPlusNode *right, BPlusNode *parent, int index) {
    // 安全检查
    if (!left || !right || !parent) {
        return;
    }
    
    // 1. 将右侧节点的所有键和数据合并到左侧节点
    for (int i = 0; i < right->key_num; i++) {
        left->keys[left->key_num + i] = right->keys[i];
        left->ptrs[left->key_num + i] = right->ptrs[i];
    }
    
    // 2. 更新左侧节点的next指针（指向右侧节点的next指针）
    left->ptrs[left->key_num + right->key_num] = right->ptrs[right->key_num];
    
    // 3. 更新键数量
    left->key_num += right->key_num;
    
    // 4. 更新父节点中的键和指针
    for (int i = index; i < parent->key_num - 1; i++) {
        parent->keys[i] = parent->keys[i + 1];
    }
    for (int i = index + 1; i < parent->key_num; i++) {
        parent->ptrs[i] = parent->ptrs[i + 1];
    }
    
    // 5. 更新父节点的键数量
    parent->key_num--;
    
    // 6. 标记右侧节点为已删除（在实际释放前）
    char msg[128];
    snprintf(msg, sizeof(msg), "合并叶节点，左侧节点键数量: %d\n", left->key_num);
    log_message(msg, stdout); // 临时输出到控制台用于调试
}

// 合并两个非叶节点
void merge_non_leaf(BPlusNode *left, BPlusNode *right, BPlusNode *parent, int index) {
    // 安全检查
    if (!left || !right || !parent) {
        return;
    }
    
    // 1. 将父节点中的分隔键下移到左侧节点
    left->keys[left->key_num] = parent->keys[index];
    
    // 2. 将右侧节点的所有键和子节点合并到左侧节点
    for (int i = 0; i < right->key_num; i++) {
        left->keys[left->key_num + 1 + i] = right->keys[i];
    }
    for (int i = 0; i <= right->key_num; i++) {
        left->ptrs[left->key_num + 1 + i] = right->ptrs[i];
        // 更新子节点的父指针
        if (right->ptrs[i]) {
            ((BPlusNode*)right->ptrs[i])->parent = left;
        }
    }
    
    // 3. 更新键数量
    left->key_num += right->key_num + 1;
    
    // 4. 更新父节点中的键和指针
    for (int i = index; i < parent->key_num - 1; i++) {
        parent->keys[i] = parent->keys[i + 1];
    }
    for (int i = index + 1; i < parent->key_num; i++) {
        parent->ptrs[i] = parent->ptrs[i + 1];
    }
    
    // 5. 更新父节点的键数量
    parent->key_num--;
    
    char msg[128];
    snprintf(msg, sizeof(msg), "合并非叶节点，左侧节点键数量: %d\n", left->key_num);
    log_message(msg, stdout); // 临时输出到控制台用于调试
}

// 处理节点下溢
void handle_underflow(BPlusTree *tree, BPlusNode *node) {
    // 安全检查
    if (!node || node == tree->root) {
        return;
    }
    
    BPlusNode* parent = node->parent;
    int node_index = get_node_index_in_parent(node);
    
    // 安全检查
    if (node_index == -1) {
        return;
    }
    
    // 1. 尝试从左侧兄弟节点借键
    if (node_index > 0) {
        BPlusNode* left_sibling = (BPlusNode*)parent->ptrs[node_index - 1];
        if (left_sibling->key_num > get_min_keys(left_sibling)) {
            // 左侧兄弟节点有足够的键可以借
            if (node->is_leaf) {
                redistribute_leaf_from_left(node, left_sibling, parent, node_index);
            } else {
                redistribute_non_leaf_from_left(node, left_sibling, parent, node_index);
            }
            return;
        }
    }
    
    // 2. 尝试从右侧兄弟节点借键
    if (node_index < parent->key_num) {
        BPlusNode* right_sibling = (BPlusNode*)parent->ptrs[node_index + 1];
        if (right_sibling->key_num > get_min_keys(right_sibling)) {
            // 右侧兄弟节点有足够的键可以借
            if (node->is_leaf) {
                redistribute_leaf_from_right(node, right_sibling, parent, node_index);
            } else {
                redistribute_non_leaf_from_right(node, right_sibling, parent, node_index);
            }
            return;
        }
    }
    
    // 3. 无法借键，需要合并节点
    if (node_index > 0) {
        // 与左侧兄弟节点合并
        BPlusNode* left_sibling = (BPlusNode*)parent->ptrs[node_index - 1];
        if (node->is_leaf) {
            merge_leaf(left_sibling, node, parent, node_index - 1);
        } else {
            merge_non_leaf(left_sibling, node, parent, node_index - 1);
        }
        // 释放被合并的节点
        free(node);
    } else if (node_index < parent->key_num) {
        // 与右侧兄弟节点合并
        BPlusNode* right_sibling = (BPlusNode*)parent->ptrs[node_index + 1];
        if (node->is_leaf) {
            merge_leaf(node, right_sibling, parent, node_index);
        } else {
            merge_non_leaf(node, right_sibling, parent, node_index);
        }
        // 释放被合并的节点
        free(right_sibling);
    }
    
    // 4. 检查父节点是否发生下溢
    if (parent != tree->root && parent->key_num < get_min_keys(parent)) {
        handle_underflow(tree, parent);
    }
    
    // 5. 如果父节点是根节点且键数量为0，更新根节点
    if (parent == tree->root && parent->key_num == 0) {
        tree->root = (BPlusNode*)parent->ptrs[0];
        if (tree->root) {
            tree->root->parent = NULL;
        }
        free(parent);
    }
}

// 删除节点中的键
void delete_node(BPlusTree *tree, BPlusNode *node, int key) {
    // 安全检查
    if (!node) {
        return;
    }
    
    if (node->is_leaf) {
        // 叶节点直接删除键
        delete_leaf_key(tree, node, key);
    } else {
        // 非叶节点的处理
        delete_non_leaf_key(tree, node, key);
    }
}

// 辅助函数：递归验证父指针的一致性
void validate_parent_pointers_recursive(BPlusTree *tree, BPlusNode *node, BPlusNode *expected_parent) {
    // 安全检查
    if (!node) {
        return;
    }
    
    // 检查当前节点的父指针是否正确
    if (node != tree->root && node->parent != expected_parent) {
        char msg[128];
        snprintf(msg, sizeof(msg), "父指针不一致: 节点 %p 的父节点应为 %p，但实际是 %p\n", 
                node, expected_parent, node->parent);
        log_message(msg, stdout);
    }
    
    // 递归检查所有子节点
    if (!node->is_leaf) {
        for (int i = 0; i <= node->key_num; i++) {
            if (node->ptrs[i]) {
                validate_parent_pointers_recursive(tree, (BPlusNode*)node->ptrs[i], node);
            }
        }
    }
}

// 验证父指针的一致性
void validate_parent_pointers(BPlusTree *tree) {
    validate_parent_pointers_recursive(tree, tree->root, NULL);
}

// 辅助函数：递归验证最小键数约束
void validate_min_keys_constraint_recursive(BPlusTree *tree, BPlusNode *node) {
    // 安全检查
    if (!node) {
        return;
    }
    
    // 根节点可以有任意数量的键（至少0个）
    if (node == tree->root) {
        if (node->key_num < 0) {
            char msg[128];
            snprintf(msg, sizeof(msg), "根节点键数量为负: %d\n", node->key_num);
            log_message(msg, stdout);
        }
    } else {
        // 非根节点需要满足最小键数约束
        int min_keys = get_min_keys(node);
        if (node->key_num < min_keys) {
            char msg[128];
            snprintf(msg, sizeof(msg), "节点 %p 键数量不足: 当前 %d，最小 %d\n", 
                    node, node->key_num, min_keys);
            log_message(msg, stdout);
        }
    }
    
    // 递归检查所有子节点
    if (!node->is_leaf) {
        for (int i = 0; i <= node->key_num; i++) {
            if (node->ptrs[i]) {
                validate_min_keys_constraint_recursive(tree, (BPlusNode*)node->ptrs[i]);
            }
        }
    }
}

// 验证最小键数约束
void validate_min_keys_constraint(BPlusTree *tree) {
    validate_min_keys_constraint_recursive(tree, tree->root);
}


// 删除键的入口函数
void delete_key(BPlusTree *tree, int key) {
    char msg[128];
    snprintf(msg, sizeof(msg), "=== 操作: 删除 key=%d ===\n", key);
    log_message(msg, stdout);
    
    // 查找包含该键的叶子节点
    BPlusNode *leaf_node = (BPlusNode*)search(tree->root, key);
    
    if (!leaf_node) {
        snprintf(msg, sizeof(msg), "键 %d 不存在，删除失败\n", key);
        log_message(msg, stdout);
        return;
    }
    
    // 在叶子节点中删除该键
    delete_leaf_key(tree, (BPlusNode*)tree->root, key);
    
    // 打印删除后的树结构
    print_bplus_tree(tree, "删除后的树结构", stdout);
    
    // 验证删除结果
    leaf_node = (BPlusNode*)search(tree->root, key);
    if (!leaf_node) {
        snprintf(msg, sizeof(msg), "=== 验证删除 key=%d === 已删除（正常）\n", key);
    } else {
        snprintf(msg, sizeof(msg), "=== 验证删除 key=%d === 删除失败（异常）\n", key);
    }
    log_message(msg, stdout);
    
    // 验证父指针的一致性
    validate_parent_pointers(tree);
    
    // 验证最小键数约束
    validate_min_keys_constraint(tree);
}

// 打印完整的B+树结构（所有层级节点）
void print_bplus_tree(BPlusTree* tree, const char* operation_desc, FILE* log_file) {
    char msg[1024];
    snprintf(msg, sizeof(msg), "=== 操作: %s ===\n", operation_desc);
    log_message(msg, log_file);

    if (!tree->root) {
        log_message("树为空\n", log_file);
        return;
    }

    // 打印树的基本信息
    int height = get_tree_height(tree->root);
    snprintf(msg, sizeof(msg), "B+树结构 (最大键数 n=%d, 树高=%d, 根节点=%p)\n", 
            MAX_KEYS, height, tree->root);
    log_message(msg, log_file);

    // 逐层打印所有节点
    for (int level = 1; level <= height; level++) {
        snprintf(msg, sizeof(msg), "------------------------ 第 %d 层 ------------------------\n", level);
        log_message(msg, log_file);
        print_level_nodes(tree->root, 1, level, log_file);
    }
    log_message("\n", log_file);
}

// 测试主函数
int main() {
    FILE* log_file = fopen("btree_log.txt", "w");
    if (!log_file) {
        perror("无法打开日志文件");
        return 1;
    }

    BPlusTree tree;
    init_bplus_tree(&tree);

    // 测试数据（确保无重复键）
    int keys[] = {13,  49,  23,  45,  77,  3,   29,  14,  11,  78,  30,  40,  4,   5,   15,  16};
    int data[] = {111, 333, 222, 444, 555, 666, 777, 888, 999, 100, 101, 102, 103, 104, 105, 106};

    int n = sizeof(keys) / sizeof(keys[0]);

    // 插入测试
    for (int i = 0; i < n; i++) {
        insert(&tree, keys[i], &data[i]);
        char desc[128];
        snprintf(desc, sizeof(desc), "插入 key=%d (data=%d)", keys[i], data[i]);
        print_bplus_tree(&tree, desc, log_file);
    }

    // 查找测试（覆盖所有已插入的键）
    log_message("=== 开始查找测试 ===\n", log_file);
    for (int i = 0; i < n; i++) {
        int* result = (int*)search(tree.root, keys[i]);
        char msg[128];
        if (result) {
            snprintf(msg, sizeof(msg), "=== 查找 key=%d === 找到, data=%d\n", keys[i], *result);
        } else {
            snprintf(msg, sizeof(msg), "=== 查找 key=%d === 未找到（异常）\n", keys[i]);
        }
        log_message(msg, log_file);
    }
    // 查找不存在的键
    int non_exist_key = 99;
    int* result = (int*)search(tree.root, non_exist_key);
    char msg[128];
    if (result) {
        snprintf(msg, sizeof(msg), "=== 查找 key=%d === 找到（异常）, data=%d\n", non_exist_key, *result);
    } else {
        snprintf(msg, sizeof(msg), "=== 查找 key=%d === 未找到（正常）\n", non_exist_key);
    }
    log_message(msg, log_file);
    log_message("=== 查找测试结束 ===\n\n", log_file);

    // 删除测试
    int delete_keys[] = {13, 23, 45, 11, 77};
    int dk_len = sizeof(delete_keys) / sizeof(delete_keys[0]);
    for (int i = 0; i < dk_len; i++) {
        delete_key(&tree, delete_keys[i]);
        char desc[128];
        snprintf(desc, sizeof(desc), "删除 key=%d", delete_keys[i]);
        print_bplus_tree(&tree, desc, log_file);

        // 验证删除后的键是否存在
        int* del_result = (int*)search(tree.root, delete_keys[i]);
        char del_msg[128];
        if (del_result) {
            snprintf(del_msg, sizeof(del_msg), "=== 验证删除 key=%d === 仍存在（异常）\n\n", delete_keys[i]);
        } else {
            snprintf(del_msg, sizeof(del_msg), "=== 验证删除 key=%d === 已删除（正常）\n\n", delete_keys[i]);
        }
        log_message(del_msg, log_file);
    }

    fclose(log_file);
    // 释放B+树内存
    free_bplus_tree(tree.root);
    return 0;
}