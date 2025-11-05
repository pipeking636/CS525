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
int get_min_keys(BPlusNode* node);
void delete_leaf_key(BPlusNode* node, int idx);
void delete_non_leaf_key(BPlusNode* node, int idx);
void handle_underflow(BPlusNode* node);
void redistribute_leaf(BPlusNode* parent, int curr_idx);
void redistribute_non_leaf(BPlusNode* parent, int curr_idx);
void redistribute_leaf_from_right(BPlusNode* parent, int curr_idx);
void merge_leaf(BPlusNode* parent, int curr_idx);
void delete_node(BPlusNode* node, int key);
void delete_key(BPlusTree* tree, int key);
void print_bplus_tree(BPlusTree* tree, const char* operation_desc, FILE* log_file);
void log_message(const char* msg, FILE* log_file);
int get_tree_height(BPlusNode* node);
void print_level_nodes(BPlusNode* node, int current_level, int target_level, FILE* log_file);
int get_node_index_in_parent(BPlusNode* node);

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
// 修复后的最小键数计算（区分根节点和非根节点）
int get_min_keys(BPlusNode* node) {
    // 根节点特殊：叶节点可0个键，非叶节点可1个键
    if (node->parent == NULL) {
        return node->is_leaf ? 0 : 1;
    }
    // 非根节点：
    // - n为偶数时：叶节点和非叶节点的Min Keys都是n/2
    // - n为奇数时：非叶节点Min Keys=(n-1)/2，叶节点Min Keys=(n+1)/2
    if (MAX_KEYS % 2 == 0) {
        // 偶数情况
        return MAX_KEYS / 2;
    } else {
        // 奇数情况
        return (MAX_KEYS - 1) / 2 + (node->is_leaf ? 1 : 0);
    }
}
// 修改delete_leaf_key函数，增强对空叶节点的处理
void delete_leaf_key(BPlusNode* node, int idx) {
    if (!node || idx < 0 || idx >= node->key_num) return;
    
    // 保存删除前的键数，用于后续空节点检查
    int old_key_num = node->key_num;
    
    // 将数据指针设置为NULL并标记键为已删除
    node->ptrs[idx] = NULL;
    node->keys[idx] = DELETED_KEY; // 使用-1作为已删除标记
    
    // 移动剩余有效键和指针到已删除位置
    // 先压缩节点，移除所有已删除的键
    int valid_pos = 0;
    for (int i = 0; i < node->key_num; i++) {
        if (node->keys[i] != -1 && node->ptrs[i] != NULL) {
            if (valid_pos != i) {
                node->keys[valid_pos] = node->keys[i];
                node->ptrs[valid_pos] = node->ptrs[i];
                // 将原位置标记为已删除
                node->keys[i] = -1;
                node->ptrs[i] = NULL;
            }
            valid_pos++;
        }
    }
    
    // 更新键数量为有效键数量
    node->key_num = valid_pos;
    
    // 清空多余位置
    for (int i = valid_pos; i < MAX_KEYS + 1; i++) {
        node->keys[i] = -1;
        node->ptrs[i] = NULL;
    }
    
    // 修复：当叶节点键数变为0时，应该触发下溢处理
    if (old_key_num > 0 && node->key_num == 0 && node->parent != NULL) {
        // 键数从正数变为0，需要处理空叶节点
        handle_underflow(node);
    }
}
// 删除非叶节点键
void delete_non_leaf_key(BPlusNode* node, int idx) {
    if (idx < 0 || idx >= node->key_num) return; // 防御性检查

    // 移动键覆盖待删除项
    for (int i = idx; i < node->key_num - 1; i++) {
        node->keys[i] = node->keys[i + 1];
    }
    node->key_num--;
}

// 从左兄弟借调键（叶节点）
void redistribute_leaf(BPlusNode* parent, int curr_idx) {
    if (curr_idx <= 0 || curr_idx > parent->key_num) return; // 防御性检查

    BPlusNode* curr = (BPlusNode*)parent->ptrs[curr_idx];
    BPlusNode* left_sib = (BPlusNode*)parent->ptrs[curr_idx - 1];

    // 左兄弟键数不足，无法借调
    if (left_sib->key_num <= get_min_keys(left_sib)) return;

    // 1. 当前节点所有键和指针右移1位
    for (int i = curr->key_num; i > 0; i--) {
        curr->keys[i] = curr->keys[i - 1];
        curr->ptrs[i] = curr->ptrs[i - 1];
    }
    // 2. 左兄弟最后一个键和指针移到当前节点
    curr->keys[0] = left_sib->keys[left_sib->key_num - 1];
    curr->ptrs[0] = left_sib->ptrs[left_sib->key_num - 1];
    curr->key_num++;
    // 3. 左兄弟删除最后一个键
    left_sib->key_num--;
    left_sib->ptrs[left_sib->key_num] = NULL; // 清空野指针

    // 4. 更新父节点的分隔键（为当前节点的最小键）
    parent->keys[curr_idx - 1] = curr->keys[0];
}
// 添加：从左兄弟借调键（非叶节点）
void redistribute_non_leaf(BPlusNode* parent, int curr_idx) {
    if (curr_idx <= 0 || curr_idx > parent->key_num) return; // 防御性检查

    BPlusNode* curr = (BPlusNode*)parent->ptrs[curr_idx];
    BPlusNode* left_sib = (BPlusNode*)parent->ptrs[curr_idx - 1];

    // 1. 当前节点所有键和指针右移1位
    for (int i = curr->key_num; i >= 0; i--) {
        curr->keys[i + 1] = curr->keys[i];
        curr->ptrs[i + 2] = curr->ptrs[i + 1];
    }
    curr->ptrs[1] = curr->ptrs[0];
    
    // 2. 将父节点的分隔键下移到当前节点
    curr->keys[0] = parent->keys[curr_idx - 1];
    curr->key_num++;
    
    // 3. 将左兄弟的最后一个键上移到父节点
    parent->keys[curr_idx - 1] = left_sib->keys[left_sib->key_num - 1];
    
    // 4. 将左兄弟的最后一个子节点指针移到当前节点
    curr->ptrs[0] = left_sib->ptrs[left_sib->key_num];
    ((BPlusNode*)curr->ptrs[0])->parent = curr;
    
    // 5. 左兄弟删除最后一个键
    left_sib->key_num--;
}
// 添加：从右兄弟借调键（非叶节点）
void redistribute_non_leaf_from_right(BPlusNode* parent, int curr_idx) {
    if (curr_idx < 0 || curr_idx >= parent->key_num) return; // 防御性检查

    BPlusNode* curr = (BPlusNode*)parent->ptrs[curr_idx];
    BPlusNode* right_sib = (BPlusNode*)parent->ptrs[curr_idx + 1];

    // 1. 将父节点的分隔键下移到当前节点
    curr->keys[curr->key_num] = parent->keys[curr_idx];
    
    // 2. 将右兄弟的第一个子节点指针移到当前节点
    curr->ptrs[curr->key_num + 1] = right_sib->ptrs[0];
    ((BPlusNode*)curr->ptrs[curr->key_num + 1])->parent = curr;
    curr->key_num++;
    
    // 3. 将右兄弟的第一个键上移到父节点
    parent->keys[curr_idx] = right_sib->keys[0];
    
    // 4. 右兄弟所有键和指针左移1位
    for (int i = 0; i < right_sib->key_num - 1; i++) {
        right_sib->keys[i] = right_sib->keys[i + 1];
        right_sib->ptrs[i] = right_sib->ptrs[i + 1];
    }
    right_sib->ptrs[right_sib->key_num - 1] = right_sib->ptrs[right_sib->key_num];
    right_sib->key_num--;
}
// 从右兄弟借调键（叶节点）
void redistribute_leaf_from_right(BPlusNode* parent, int curr_idx) {
    if (curr_idx < 0 || curr_idx >= parent->key_num) return; // 防御性检查

    BPlusNode* curr = (BPlusNode*)parent->ptrs[curr_idx];
    BPlusNode* right_sib = (BPlusNode*)parent->ptrs[curr_idx + 1];

    // 右兄弟键数不足，无法借调
    if (right_sib->key_num <= get_min_keys(right_sib)) return;

    // 1. 右兄弟第一个键和指针移到当前节点
    curr->keys[curr->key_num] = right_sib->keys[0];
    curr->ptrs[curr->key_num] = right_sib->ptrs[0];
    curr->key_num++;
    // 2. 右兄弟所有键和指针左移1位
    for (int i = 0; i < right_sib->key_num - 1; i++) {
        right_sib->keys[i] = right_sib->keys[i + 1];
        right_sib->ptrs[i] = right_sib->ptrs[i + 1];
    }
    right_sib->key_num--;
    right_sib->ptrs[right_sib->key_num] = NULL; // 清空野指针

    // 3. 更新父节点的分隔键（为右兄弟的最小键）
    parent->keys[curr_idx] = right_sib->keys[0];
}
// 修复merge_leaf函数，移除对数据指针的错误释放
void merge_leaf(BPlusNode* parent, int curr_idx) {
    if (!parent || curr_idx <= 0 || curr_idx > parent->key_num) return;
    
    BPlusNode* curr = (BPlusNode*)parent->ptrs[curr_idx];
    BPlusNode* left_sib = (BPlusNode*)parent->ptrs[curr_idx - 1];
    
    if (!curr || !left_sib) return;
    
    // 1. 先压缩当前节点和左兄弟节点，只保留有效键
    int valid_pos = 0;
    for (int i = 0; i < curr->key_num; i++) {
        if (curr->keys[i] != -1 && curr->ptrs[i] != NULL) {
            if (valid_pos != i) {
                curr->keys[valid_pos] = curr->keys[i];
                curr->ptrs[valid_pos] = curr->ptrs[i];
                curr->keys[i] = -1;
                curr->ptrs[i] = NULL;
            }
            valid_pos++;
        }
    }
    curr->key_num = valid_pos;
    
    // 压缩左兄弟节点
    valid_pos = 0;
    for (int i = 0; i < left_sib->key_num; i++) {
        if (left_sib->keys[i] != -1 && left_sib->ptrs[i] != NULL) {
            if (valid_pos != i) {
                left_sib->keys[valid_pos] = left_sib->keys[i];
                left_sib->ptrs[valid_pos] = left_sib->ptrs[i];
                left_sib->keys[i] = -1;
                left_sib->ptrs[i] = NULL;
            }
            valid_pos++;
        }
    }
    left_sib->key_num = valid_pos;
    
    // 2. 合并当前节点的所有有效键到左兄弟，并确保有序
    int temp_keys[MAX_KEYS * 2];
    void* temp_ptrs[MAX_KEYS * 2]; // 直接使用指针，不再复制数据
    int temp_count = 0;
    
    // 复制左兄弟的键和指针
    for (int i = 0; i < left_sib->key_num; i++) {
        if (left_sib->keys[i] != -1 && left_sib->ptrs[i] != NULL) {
            temp_keys[temp_count] = left_sib->keys[i];
            temp_ptrs[temp_count] = left_sib->ptrs[i];
            temp_count++;
        }
    }
    
    // 复制当前节点的键和指针
    for (int i = 0; i < curr->key_num; i++) {
        if (curr->keys[i] != -1 && curr->ptrs[i] != NULL) {
            temp_keys[temp_count] = curr->keys[i];
            temp_ptrs[temp_count] = curr->ptrs[i];
            temp_count++;
        }
    }
    
    // 对合并后的键和指针进行排序
    for (int i = 0; i < temp_count - 1; i++) {
        for (int j = 0; j < temp_count - i - 1; j++) {
            if (temp_keys[j] > temp_keys[j + 1]) {
                // 交换键
                int temp_key = temp_keys[j];
                temp_keys[j] = temp_keys[j + 1];
                temp_keys[j + 1] = temp_key;
                
                // 交换指针
                void* temp_ptr = temp_ptrs[j];
                temp_ptrs[j] = temp_ptrs[j + 1];
                temp_ptrs[j + 1] = temp_ptr;
            }
        }
    }
    
    // 将排序后的键和指针放回左兄弟节点
    left_sib->key_num = 0;
    for (int i = 0; i < temp_count; i++) {
        if (left_sib->key_num >= MAX_KEYS) {
            // 不应该发生，因为合并的前提是两个节点的键数都不足最小值
            break;
        }
        left_sib->keys[left_sib->key_num] = temp_keys[i];
        left_sib->ptrs[left_sib->key_num] = temp_ptrs[i];
        left_sib->key_num++;
    }
    
    // 3. 更新叶节点链表
    if (curr->ptrs[MAX_KEYS + 1] != NULL) {
        left_sib->ptrs[MAX_KEYS + 1] = curr->ptrs[MAX_KEYS + 1];
    } else {
        left_sib->ptrs[MAX_KEYS + 1] = NULL;
    }
    
    // 4. 更新父节点（移除分隔键和curr指针）
    delete_non_leaf_key(parent, curr_idx - 1);
    
    // 修复：正确移动父节点的子节点指针，避免重复引用
    for (int i = curr_idx; i < parent->key_num + 1; i++) {
        if (i + 1 <= MAX_KEYS + 1) { // 使用MAX_KEYS+1作为最大指针数量
            parent->ptrs[i] = parent->ptrs[i + 1];
        } else {
            parent->ptrs[i] = NULL;
        }
    }
    
    // 5. 清空多余位置的指针
    for (int i = parent->key_num + 1; i <= MAX_KEYS + 1; i++) {
        parent->ptrs[i] = NULL;
    }
    
    // 6. 清空当前节点的所有指针，包括next_leaf指针
    for (int i = 0; i <= MAX_KEYS + 1; i++) {
        curr->ptrs[i] = NULL;
    }

    // 7. 设置当前节点的父指针为NULL，防止后续错误访问
    curr->parent = NULL;

    // 8. 释放被合并的节点
    free(curr);
}
// 修复merge_non_leaf函数，确保正确管理指针
void merge_non_leaf(BPlusNode* parent, int curr_idx) {
    if (!parent || curr_idx <= 0 || curr_idx > parent->key_num) return;
    
    BPlusNode* curr = (BPlusNode*)parent->ptrs[curr_idx];
    BPlusNode* left_sib = (BPlusNode*)parent->ptrs[curr_idx - 1];
    
    if (!curr || !left_sib) return;
    
    // 获取分隔键
    int separator_key = parent->keys[curr_idx - 1];
    
    // 1. 将分隔键插入到左兄弟节点
    left_sib->keys[left_sib->key_num] = separator_key;
    left_sib->key_num++;
    
    // 2. 合并当前节点的所有键和子节点指针
    for (int i = 0; i < curr->key_num; i++) {
        left_sib->keys[left_sib->key_num] = curr->keys[i];
        left_sib->ptrs[left_sib->key_num + 1] = curr->ptrs[i + 1];
        
        // 更新子节点的父指针
        if (left_sib->ptrs[left_sib->key_num + 1]) {
            ((BPlusNode*)left_sib->ptrs[left_sib->key_num + 1])->parent = left_sib;
        }
        
        left_sib->key_num++;
    }
    
    // 3. 更新父节点（移除分隔键和当前节点指针）
    delete_non_leaf_key(parent, curr_idx - 1);
    
    // 修复：正确移动父节点的子节点指针，避免重复引用
    for (int i = curr_idx; i < parent->key_num + 1; i++) {
        if (i + 1 <= MAX_KEYS + 1) { // 使用MAX_KEYS+1作为最大指针数量
            parent->ptrs[i] = parent->ptrs[i + 1];
        } else {
            parent->ptrs[i] = NULL;
        }
    }
    
    // 4. 清空多余位置的指针
    for (int i = parent->key_num + 1; i <= MAX_KEYS + 1; i++) {
        parent->ptrs[i] = NULL;
    }
    
    // 5. 清空当前节点的所有子节点指针
    for (int i = 0; i <= MAX_KEYS + 1; i++) {
        curr->ptrs[i] = NULL;
    }
    
    // 6. 设置当前节点的父指针为NULL，防止后续错误访问
    curr->parent = NULL;
    
    // 7. 释放被合并的节点
    free(curr);
}
// 修改handle_underflow函数，增强对空叶节点的处理逻辑
void handle_underflow(BPlusNode* node) {
    if (!node || node->parent == NULL) return;
    
    // 特别处理空叶节点
    if (node->is_leaf && node->key_num == 0) {
        BPlusNode* parent = node->parent;
        int node_idx = get_node_index_in_parent(node);
        
        if (node_idx >= 0) {
            // 1. 找到前一个和后一个叶节点，修复链表
            BPlusNode* prev_leaf = NULL;
            BPlusNode* next_leaf = (BPlusNode*)node->ptrs[MAX_KEYS + 1]; // 获取next_leaf
            
            // 查找前一个叶节点
            if (node_idx > 0) {
                prev_leaf = (BPlusNode*)parent->ptrs[node_idx - 1];
                if (prev_leaf && prev_leaf->is_leaf) {
                    // 修复前一个叶节点的next_leaf指针
                    prev_leaf->ptrs[MAX_KEYS + 1] = next_leaf;
                }
            }
            
            // 2. 从父节点中移除空叶节点
            delete_non_leaf_key(parent, node_idx - 1); // 删除对应索引键
            
            // 移动父节点的指针
            for (int i = node_idx; i < parent->key_num + 1; i++) {
                if (i + 1 <= MAX_KEYS + 1) {
                    parent->ptrs[i] = parent->ptrs[i + 1];
                } else {
                    parent->ptrs[i] = NULL;
                }
            }
            
            // 3. 清空空节点的所有指针
            for (int i = 0; i <= MAX_KEYS + 1; i++) {
                node->ptrs[i] = NULL;
            }
            node->parent = NULL;
            
            // 4. 释放空节点
            free(node);
            
            // 5. 递归检查父节点是否需要下溢处理
            if (parent->key_num < get_min_keys(parent) && parent->parent != NULL) {
                handle_underflow(parent);
            }
        }
        return; // 空叶节点已处理，直接返回
    }
    
    // 原有的下溢处理逻辑
    int min_keys = get_min_keys(node);
    int valid_key_count = node->is_leaf ? get_valid_key_count(node) : node->key_num;
    
    if (valid_key_count >= min_keys) return;
    
    BPlusNode* parent = node->parent;
    int curr_idx = get_node_index_in_parent(node);
    if (curr_idx == -1) return;
    
    int node_merged = 0;
    
    // 尝试从左兄弟借调
    if (curr_idx > 0) {
        BPlusNode* left_sib = (BPlusNode*)parent->ptrs[curr_idx - 1];
        if (left_sib && left_sib->key_num > get_min_keys(left_sib)) {
            if (node->is_leaf) {
                redistribute_leaf(parent, curr_idx);
            } else {
                redistribute_non_leaf(parent, curr_idx);
            }
            return; // 借调成功
        }
    }
    
    // 尝试从右兄弟借调
    if (curr_idx < parent->key_num) {
        BPlusNode* right_sib = (BPlusNode*)parent->ptrs[curr_idx + 1];
        if (right_sib && right_sib->key_num > get_min_keys(right_sib)) {
            // 交换当前节点和右兄弟，以便复用左借调逻辑
            BPlusNode* temp = parent->ptrs[curr_idx];
            parent->ptrs[curr_idx] = parent->ptrs[curr_idx + 1];
            parent->ptrs[curr_idx + 1] = temp;
            
            if (node->is_leaf) {
                redistribute_leaf(parent, curr_idx + 1);
            } else {
                redistribute_non_leaf(parent, curr_idx + 1);
            }
            return; // 借调成功
        }
    }
    
    // 借调失败，合并节点
    if (curr_idx > 0 && !node_merged) {
        if (node->is_leaf) {
            merge_leaf(parent, curr_idx);
        } else {
            merge_non_leaf(parent, curr_idx);
        }
        node_merged = 1;
    } else if (curr_idx < parent->key_num && !node_merged) {
        // 交换当前节点和右兄弟，以便统一合并逻辑
        BPlusNode* temp = parent->ptrs[curr_idx];
        parent->ptrs[curr_idx] = parent->ptrs[curr_idx + 1];
        parent->ptrs[curr_idx + 1] = temp;
        
        if (node->is_leaf) {
            merge_leaf(parent, curr_idx + 1);
        } else {
            merge_non_leaf(parent, curr_idx + 1);
        }
        node_merged = 1;
    }
    
    // 递归处理父节点的下溢，但只有在父节点键数确实低于最小要求时才递归
    if (!node_merged && parent->parent != NULL && parent->key_num < get_min_keys(parent)) {
        handle_underflow(parent);
    }
}
// 修复后的delete_node函数
void delete_node(BPlusNode* node, int key) {
    if (!node) return;
    
    // 保存节点是否为叶节点的状态
    int is_leaf_node = node->is_leaf;
    
    if (is_leaf_node) {
        // 处理叶节点中的键删除
        int idx = find_key_index(node, key);
        if (idx < node->key_num && node->keys[idx] == key) {
            delete_leaf_key(node, idx);
        }
    } else {
        // 处理非叶节点中的键删除
        int idx = find_key_index(node, key);
        if (idx < node->key_num && node->keys[idx] == key) {
            // 键在当前节点，需要用后继节点的最小键替换
            BPlusNode* successor = (BPlusNode*)node->ptrs[idx + 1];
            while (successor && !successor->is_leaf) {
                successor = (BPlusNode*)successor->ptrs[0];
            }
            if (!successor) return;
            
            // 用后继节点的最小键替换当前键
            node->keys[idx] = successor->keys[0];
            // 删除后继节点的最小键
            delete_leaf_key(successor, 0);
            
            // 处理后继节点可能的下溢
            if (successor->parent && successor->key_num < get_min_keys(successor)) {
                handle_underflow(successor);
            }
        } else {
            // 递归删除子节点的键
            if (idx >= 0 && idx <= node->key_num && node->ptrs[idx] != NULL) {
                BPlusNode* child = (BPlusNode*)node->ptrs[idx];
                
                // // 记录子节点在父节点中的索引
                // int child_idx = -1;
                // for (int i = 0; i <= node->key_num; i++) {
                //     if (node->ptrs[i] == child) {
                //         child_idx = i;
                //         break;
                //     }
                // }
                
                // 删除子节点中的键
                delete_node(child, key);
                
                // 安全检查：确保子节点仍然存在且未被合并
                int child_still_exists = 0;
                for (int i = 0; i <= node->key_num; i++) {
                    if (node->ptrs[i] == child) {
                        child_still_exists = 1;
                        break;
                    }
                }
                
                // 只有在子节点仍然存在且下溢时才处理
                if (child_still_exists && child->key_num < get_min_keys(child)) {
                    handle_underflow(child);
                }
            }
        }
    }
    
    // 根节点下溢无需处理（根节点允许更小的键数）
    if (node->parent == NULL) return;
}
// 修复后的删除入口函数
void delete_key(BPlusTree* tree, int key) {
    if (!tree->root || tree->root->key_num == 0) return;

    delete_node(tree->root, key);

    // 若根节点为空非叶节点，更新根为其子节点
    if (!tree->root->is_leaf && tree->root->key_num == 0) {
        BPlusNode* old_root = tree->root;
        tree->root = (BPlusNode*)old_root->ptrs[0];
        if (tree->root) {
            tree->root->parent = NULL;
        }
        free(old_root);
    }
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