#include <stdio.h>
#include <stdlib.h>
#include <string.h>
// 配置参数（节点最大键数）
#define MAX_KEYS 2  // 支持奇数/偶数测试
// B+树节点结构体
typedef struct BPlusNode {
    int is_leaf;                  // 1=叶节点, 0=非叶节点
    int key_num;                  // 当前键数量
    int keys[MAX_KEYS + 1];       // 预留1个位置用于分裂
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
int find_key_index(BPlusNode* node, int key);
void split_leaf(BPlusNode* parent, int index);
void split_non_leaf(BPlusNode* parent, int index);
void insert_non_full(BPlusNode* node, int key, void* data);
void insert(BPlusTree* tree, int key, void* data);
void* search(BPlusNode* node, int key);
int get_min_keys(BPlusNode* node);
void delete_leaf_key(BPlusNode* node, int idx);
void delete_non_leaf_key(BPlusNode* node, int idx);
void redistribute_leaf(BPlusNode* parent, int curr_idx);
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
int get_node_index_in_parent(BPlusNode* node) {
    if (!node->parent) return -1;  // 根节点无父节点
    for (int i = 0; i <= node->parent->key_num; i++) {
        if (node->parent->ptrs[i] == node) {
            return i;
        }
    }
    return -1;  // 未找到（异常）
}
// 获取树的高度（根节点为第1层）
int get_tree_height(BPlusNode* node) {
    if (!node) return 0;
    if (node->is_leaf) return 1;
    return 1 + get_tree_height((BPlusNode*)node->ptrs[0]);  // 非叶节点高度=1+子节点高度
}
// 打印指定层级的所有节点
void print_level_nodes(BPlusNode* node, int current_level, int target_level, FILE* log_file) {
    if (!node) return;
    if (current_level == target_level) {
        // 打印节点基本信息
        char msg[1024];
        snprintf(msg, sizeof(msg), "  Level %d | %s Node | Parent Index: %d | Key Count: %d | Keys: [ ",
                target_level,
                node->is_leaf ? "Leaf" : "Non-Leaf",
                get_node_index_in_parent(node),
                node->key_num);
        
        // 打印所有键（避免超出实际键数）
        for (int i = 0; i < node->key_num; i++) {
            char key_str[32];
            snprintf(key_str, sizeof(key_str), "%d, ", node->keys[i]);
            strcat(msg, key_str);
        }
        strcat(msg, "] | Pointers: [ ");
        // 打印指针（区分叶节点和非叶节点）
        if (node->is_leaf) {
            // 叶节点：前key_num个为数据指针，最后一个为next叶节点指针
            for (int i = 0; i < node->key_num; i++) {
                if (!node->ptrs[i]) { // 防空指针
                    strcat(msg, "data=NULL, ");
                    continue;
                }
                char ptr_str[32];
                snprintf(ptr_str, sizeof(ptr_str), "data=%d, ", *(int*)node->ptrs[i]);
                strcat(msg, ptr_str);
            }
            char next_ptr_str[32];
            snprintf(next_ptr_str, sizeof(next_ptr_str), "next_leaf=%p", node->ptrs[MAX_KEYS + 1]);
            strcat(msg, next_ptr_str);
        } else {
            // 非叶节点：所有指针为子节点指针
            for (int i = 0; i <= node->key_num; i++) {
                char ptr_str[32];
                snprintf(ptr_str, sizeof(ptr_str), "child=%p, ", node->ptrs[i]);
                strcat(msg, ptr_str);
            }
        }
        strcat(msg, " ]\n");
        log_message(msg, log_file);
        return;
    }
    // 递归打印下一层级（仅非叶节点有子节点）
    if (!node->is_leaf) {
        for (int i = 0; i <= node->key_num; i++) {
            print_level_nodes((BPlusNode*)node->ptrs[i], current_level + 1, target_level, log_file);
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
// 查找键的插入/查找索引（返回第一个大于等于key的位置）
int find_key_index(BPlusNode* node, int key) {
    int idx = 0;
    while (idx < node->key_num && node->keys[idx] < key) {
        idx++;
    }
    return idx;
}
// 分裂叶节点
void split_leaf(BPlusNode* parent, int index) {
    BPlusNode* curr_leaf = (BPlusNode*)parent->ptrs[index];
    BPlusNode* new_leaf = create_node(1);
    new_leaf->parent = parent;
    // 计算左节点键数（奇数n均分，偶数n左多1）
    int left_key_cnt = (MAX_KEYS % 2 == 1) ? 
        (MAX_KEYS + 1) / 2 : 
        (MAX_KEYS / 2) + 1;
    // 复制右半部分键和数据到新叶节点（避免越界）
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
    curr_leaf->key_num = left_key_cnt;
    // 更新父节点（插入新键和新叶节点指针）
    for (int i = parent->key_num; i > index; i--) {
        parent->keys[i] = parent->keys[i - 1];
        parent->ptrs[i + 1] = parent->ptrs[i];
    }
    parent->keys[index] = new_leaf->keys[0];  // 父节点存新叶节点的最小键
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
// 向非满节点插入（确保键不重复）
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
// 插入入口函数
void insert(BPlusTree* tree, int key, void* data) {
    BPlusNode* root = tree->root;
    // 第一步：直接插入（递归至叶节点）
    insert_non_full(root, key, data);
    // 第二步：若根节点插入后溢出，分裂根节点
    if (root->key_num > MAX_KEYS) {
        BPlusNode* new_root = create_node(0);
        tree->root = new_root;
        new_root->ptrs[0] = root;  // 新根的第一个子节点是原根
        root->parent = new_root;
        // 根据原根节点类型分裂
        if (root->is_leaf) {
            split_leaf(new_root, 0);
        } else {
            split_non_leaf(new_root, 0);
        }
    }
}
// 修复后的搜索函数（核心：正确处理非叶节点的idx指引）
void* search(BPlusNode* node, int key) {
    if (!node) return NULL;
    int idx = find_key_index(node, key);
    if (node->is_leaf) {
        // 叶节点：直接检查是否存在键
        if (idx < node->key_num && node->keys[idx] == key) {
            return node->ptrs[idx];
        }
        return NULL;
    } else {
        // 非叶节点核心规则：
        // - 若key == keys[idx]：进入ptrs[idx+1]（存储≥key的键）
        // - 若key < keys[idx]：进入ptrs[idx]（存储<key的键）
        if (idx < node->key_num && node->keys[idx] == key) {
            idx++; // 关键修正：等于分隔键时，进入右侧子节点
        }
        BPlusNode* child = (BPlusNode*)node->ptrs[idx];
        return search(child, key);
    }
}
// 【修正核心】根据readme.md规则计算节点最小键数（兼容奇数/偶数MAX_KEYS）
int get_min_keys(BPlusNode* node) {
    // 根节点特殊规则：
    // - 根叶节点：允许0个键（空树场景）
    // - 根非叶节点：至少1个键、2个指针
    if (node->parent == NULL) {
        return node->is_leaf ? 0 : 1;
    }

    // 非根节点规则（严格遵循readme.md定义）
    if (node->is_leaf) {
        // 非根叶节点：Min Keys = ⌊(n+1)/2⌋
        if (MAX_KEYS % 2 == 1) { // n为奇数：(n+1)/2（如n=3→2）
            return (MAX_KEYS + 1) / 2;
        } else { // n为偶数：n/2（如n=2→1）
            return MAX_KEYS / 2;
        }
    } else {
        // 非根非叶节点：Min Keys = ⌈(n+1)/2⌉ - 1
        if (MAX_KEYS % 2 == 1) { // n为奇数：(n-1)/2（如n=3→1）
            return (MAX_KEYS - 1) / 2;
        } else { // n为偶数：n/2（如n=2→1）
            return MAX_KEYS / 2;
        }
    }
}
// 删除叶节点键
void delete_leaf_key(BPlusNode* node, int idx) {
    if (idx < 0 || idx >= node->key_num) return; // 防御性检查
    // 移动键和指针覆盖待删除项
    for (int i = idx; i < node->key_num - 1; i++) {
        node->keys[i] = node->keys[i + 1];
        node->ptrs[i] = node->ptrs[i + 1];
    }
    node->key_num--;
    // 清空最后一个指针（避免野指针）
    node->ptrs[node->key_num] = NULL;
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
    // 左兄弟键数不足（≤最小键数），无法借调
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
// 从右兄弟借调键（叶节点）
void redistribute_leaf_from_right(BPlusNode* parent, int curr_idx) {
    if (curr_idx < 0 || curr_idx >= parent->key_num) return; // 防御性检查
    BPlusNode* curr = (BPlusNode*)parent->ptrs[curr_idx];
    BPlusNode* right_sib = (BPlusNode*)parent->ptrs[curr_idx + 1];
    // 右兄弟键数不足（≤最小键数），无法借调
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
// 修复后的合并叶节点（避免合并后键数溢出）
void merge_leaf(BPlusNode* parent, int curr_idx) {
    if (curr_idx <= 0 || curr_idx > parent->key_num) return; // 防御性检查
    BPlusNode* curr = (BPlusNode*)parent->ptrs[curr_idx];
    BPlusNode* left_sib = (BPlusNode*)parent->ptrs[curr_idx - 1];
    int max_allowed = MAX_KEYS; // 合并后键数不能超过MAX_KEYS
    // 1. 合并父节点的分隔键（分隔键对应curr的最小键，需加入左兄弟）
    if (left_sib->key_num < max_allowed) {
        left_sib->keys[left_sib->key_num] = parent->keys[curr_idx - 1];
        left_sib->ptrs[left_sib->key_num] = curr->ptrs[0]; // 分隔键对应的数据指针
        left_sib->key_num++;
    }
    // 2. 合并当前节点的所有键（确保不超过最大键数）
    for (int i = 0; i < curr->key_num && left_sib->key_num < max_allowed; i++) {
        left_sib->keys[left_sib->key_num] = curr->keys[i];
        left_sib->ptrs[left_sib->key_num] = curr->ptrs[i];
        left_sib->key_num++;
    }
    // 3. 更新叶节点链表（左兄弟接管curr的下一个叶节点）
    left_sib->ptrs[MAX_KEYS + 1] = curr->ptrs[MAX_KEYS + 1];
    // 4. 更新父节点（移除分隔键和curr指针）
    delete_non_leaf_key(parent, curr_idx - 1);
    for (int i = curr_idx; i < parent->key_num + 1; i++) {
        parent->ptrs[i] = parent->ptrs[i + 1];
    }
    parent->ptrs[parent->key_num + 1] = NULL; // 清空野指针
    // 5. 释放被合并的节点（避免内存泄漏）
    free(curr);
}
// 删除节点内的键（递归处理下溢）
void delete_node(BPlusNode* node, int key) {
    if (!node) return;
    int idx = find_key_index(node, key);
    int min_keys = get_min_keys(node); // 使用修正后的最小键数计算
    if (node->is_leaf) {
        // 叶节点：直接删除目标键
        if (idx < node->key_num && node->keys[idx] == key) {
            delete_leaf_key(node, idx);
        } else {
            return; // 未找到键，无需处理
        }
    } else {
        // 非叶节点：若找到键，用后继叶节点的最小键替换（保持索引一致性）
        if (idx < node->key_num && node->keys[idx] == key) {
            // 找到后继叶节点（右子节点的最左叶节点）
            BPlusNode* successor = (BPlusNode*)node->ptrs[idx + 1];
            while (successor && !successor->is_leaf) {
                successor = (BPlusNode*)successor->ptrs[0];
            }
            if (!successor) return; // 防御性检查（避免空指针）
            // 用后继节点的最小键替换当前非叶节点的键
            node->keys[idx] = successor->keys[0];
            // 删除后继叶节点的最小键（实际数据删除仅在叶节点进行）
            delete_leaf_key(successor, 0);
            // 切换到后继节点，后续处理可能的下溢
            node = successor;
        } else {
            // 递归删除子节点的键
            BPlusNode* child = (BPlusNode*)node->ptrs[idx];
            delete_node(child, key);
            // 切换到子节点，后续处理可能的下溢
            node = child;
        }
    }
    // 根节点下溢无需处理（根节点允许更小的键数）
    if (node->parent == NULL) return;
    // 未下溢（键数≥最小键数），处理结束
    if (node->key_num >= min_keys) return;
    // 下溢：尝试借调或合并
    BPlusNode* parent = node->parent;
    int curr_idx = get_node_index_in_parent(node); // 获取当前节点在父节点的索引
    // 尝试1：从左兄弟借调（优先左兄弟）
    if (curr_idx > 0) {
        BPlusNode* left_sib = (BPlusNode*)parent->ptrs[curr_idx - 1];
        if (left_sib->key_num > get_min_keys(left_sib)) {
            if (node->is_leaf) {
                redistribute_leaf(parent, curr_idx);
                return;
            }
        }
    }
    // 尝试2：从右兄弟借调
    if (curr_idx < parent->key_num) {
        BPlusNode* right_sib = (BPlusNode*)parent->ptrs[curr_idx + 1];
        if (right_sib->key_num > get_min_keys(right_sib)) {
            if (node->is_leaf) {
                redistribute_leaf_from_right(parent, curr_idx);
                return;
            }
        }
    }
    // 尝试3：借调失败，合并节点（优先与左兄弟合并）
    if (curr_idx > 0) {
        if (node->is_leaf) {
            merge_leaf(parent, curr_idx);
        }
    } else if (curr_idx < parent->key_num) {
        // 无左兄弟，与右兄弟合并（将右兄弟合并到当前节点）
        if (node->is_leaf) {
            merge_leaf(parent, curr_idx + 1);
        }
    }
    // 递归处理父节点可能的下溢（用-1标记“处理下溢”而非“删除键”）
    delete_node(parent, -1);
}
// 删除入口函数
void delete_key(BPlusTree* tree, int key) {
    if (!tree->root || tree->root->key_num == 0) return;
    delete_node(tree->root, key);
    // 若根节点为空非叶节点（仅1个子节点），更新根为子节点（树高降低）
    if (!tree->root->is_leaf && tree->root->key_num == 0) {
        BPlusNode* old_root = tree->root;
        tree->root = (BPlusNode*)old_root->ptrs[0];
        if (tree->root) {
            tree->root->parent = NULL;
        }
        free(old_root); // 释放旧根节点（避免内存泄漏）
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
    return 0;
}