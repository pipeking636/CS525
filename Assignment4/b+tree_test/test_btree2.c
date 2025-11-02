#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// 配置参数（节点最大键数）
#define MAX_KEYS 2  // 可修改为2测试偶数情况

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
int get_min_keys();
void delete_leaf_key(BPlusNode* node, int idx);
void delete_non_leaf_key(BPlusNode* node, int idx);
void redistribute_leaf(BPlusNode* parent, int curr_idx);
void redistribute_leaf_from_right(BPlusNode* parent, int curr_idx);
void merge_leaf(BPlusNode* parent, int curr_idx);
void delete_node(BPlusNode* node, int key);
void delete_key(BPlusTree* tree, int key);
void traverse_leaves(BPlusNode* root);
void print_bplus_tree(BPlusTree* tree, const char* operation_desc, FILE* log_file);
void log_message(const char* msg, FILE* log_file);

// 日志输出（同时输出到控制台和文件）
void log_message(const char* msg, FILE* log_file) {
    printf("%s", msg);
    fprintf(log_file, "%s", msg);
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

// 查找键的插入/查找索引
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

    // 复制右半部分键和数据到新叶节点
    new_leaf->key_num = (MAX_KEYS + 1) - left_key_cnt;
    for (int i = 0; i < new_leaf->key_num; i++) {
        new_leaf->keys[i] = curr_leaf->keys[left_key_cnt + i];
        new_leaf->ptrs[i] = curr_leaf->ptrs[left_key_cnt + i];
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

    // 复制右半部分键和子节点到新非叶节点
    new_non_leaf->key_num = MAX_KEYS - mid;
    for (int i = 0; i < new_non_leaf->key_num; i++) {
        new_non_leaf->keys[i] = curr_non_leaf->keys[mid + 1 + i];
        new_non_leaf->ptrs[i] = curr_non_leaf->ptrs[mid + 1 + i];
        ((BPlusNode*)new_non_leaf->ptrs[i])->parent = new_non_leaf;
    }
    // 复制最后一个子节点指针
    new_non_leaf->ptrs[new_non_leaf->key_num] = curr_non_leaf->ptrs[mid + 1 + new_non_leaf->key_num];
    ((BPlusNode*)new_non_leaf->ptrs[new_non_leaf->key_num])->parent = new_non_leaf;

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

// 向非满节点插入
void insert_non_full(BPlusNode* node, int key, void* data) {
    int idx = node->key_num - 1;

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

// 插入入口函数（修复核心逻辑：先插入再分裂）
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

// 查找函数（防空指针）
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
        // 非叶节点：递归查找子节点
        BPlusNode* child = (BPlusNode*)node->ptrs[idx];
        return search(child, key);
    }
}

// 获取最小键数（非根节点）
int get_min_keys() {
    return (MAX_KEYS % 2 == 1) ? (MAX_KEYS + 1) / 2 : MAX_KEYS / 2;
}

// 删除叶节点键
void delete_leaf_key(BPlusNode* node, int idx) {
    for (int i = idx; i < node->key_num - 1; i++) {
        node->keys[i] = node->keys[i + 1];
        node->ptrs[i] = node->ptrs[i + 1];
    }
    node->key_num--;
}

// 删除非叶节点键
void delete_non_leaf_key(BPlusNode* node, int idx) {
    for (int i = idx; i < node->key_num - 1; i++) {
        node->keys[i] = node->keys[i + 1];
    }
    node->key_num--;
}

// 从左兄弟借调键（叶节点）
void redistribute_leaf(BPlusNode* parent, int curr_idx) {
    BPlusNode* curr = (BPlusNode*)parent->ptrs[curr_idx];
    BPlusNode* left_sib = (BPlusNode*)parent->ptrs[curr_idx - 1];

    // 左兄弟转移最后一个键到当前节点
    for (int i = curr->key_num; i > 0; i--) {
        curr->keys[i] = curr->keys[i - 1];
        curr->ptrs[i] = curr->ptrs[i - 1];
    }
    curr->keys[0] = left_sib->keys[left_sib->key_num - 1];
    curr->ptrs[0] = left_sib->ptrs[left_sib->key_num - 1];
    curr->key_num++;
    left_sib->key_num--;

    // 更新父节点的分隔键
    parent->keys[curr_idx - 1] = curr->keys[0];
}

// 从右兄弟借调键（叶节点）
void redistribute_leaf_from_right(BPlusNode* parent, int curr_idx) {
    BPlusNode* curr = (BPlusNode*)parent->ptrs[curr_idx];
    BPlusNode* right_sib = (BPlusNode*)parent->ptrs[curr_idx + 1];

    // 右兄弟转移第一个键到当前节点
    curr->keys[curr->key_num] = right_sib->keys[0];
    curr->ptrs[curr->key_num] = right_sib->ptrs[0];
    curr->key_num++;

    // 右兄弟移除第一个键
    for (int i = 0; i < right_sib->key_num - 1; i++) {
        right_sib->keys[i] = right_sib->keys[i + 1];
        right_sib->ptrs[i] = right_sib->ptrs[i + 1];
    }
    right_sib->key_num--;

    // 更新父节点的分隔键
    parent->keys[curr_idx] = right_sib->keys[0];
}

// 合并叶节点与左兄弟
void merge_leaf(BPlusNode* parent, int curr_idx) {
    BPlusNode* curr = (BPlusNode*)parent->ptrs[curr_idx];
    BPlusNode* left_sib = (BPlusNode*)parent->ptrs[curr_idx - 1];

    // 合并当前节点到左兄弟（包含父节点的分隔键）
    left_sib->keys[left_sib->key_num] = parent->keys[curr_idx - 1];
    left_sib->ptrs[left_sib->key_num] = curr->ptrs[0];
    left_sib->key_num++;

    // 复制当前节点的所有键
    for (int i = 0; i < curr->key_num; i++) {
        left_sib->keys[left_sib->key_num] = curr->keys[i];
        left_sib->ptrs[left_sib->key_num] = curr->ptrs[i];
        left_sib->key_num++;
    }

    // 更新叶节点链表
    left_sib->ptrs[MAX_KEYS + 1] = curr->ptrs[MAX_KEYS + 1];

    // 更新父节点（移除分隔键和当前节点指针）
    for (int i = curr_idx - 1; i < parent->key_num - 1; i++) {
        parent->keys[i] = parent->keys[i + 1];
        parent->ptrs[i + 1] = parent->ptrs[i + 2];
    }
    parent->key_num--;

    free(curr);  // 释放被合并的节点
}

// 删除节点内的键（递归处理下溢）
void delete_node(BPlusNode* node, int key) {
    int idx = find_key_index(node, key);
    int min_keys = get_min_keys();

    if (node->is_leaf) {
        // 叶节点：直接删除键
        if (idx < node->key_num && node->keys[idx] == key) {
            delete_leaf_key(node, idx);
        } else {
            return;  // 未找到键
        }
    } else {
        // 非叶节点：若找到键，用后继叶节点的最小键替换
        if (idx < node->key_num && node->keys[idx] == key) {
            BPlusNode* successor = (BPlusNode*)node->ptrs[idx + 1];
            while (!successor->is_leaf) {
                successor = (BPlusNode*)successor->ptrs[0];
            }
            node->keys[idx] = successor->keys[0];  // 替换键
            delete_node(successor, successor->keys[0]);  // 删除后继节点的最小键
        } else {
            // 递归删除子节点的键
            BPlusNode* child = (BPlusNode*)node->ptrs[idx];
            delete_node(child, key);

            // 检查子节点是否下溢
            if (child->key_num < min_keys) {
                int child_idx = idx;
                BPlusNode* left_sib = (child_idx > 0) ? (BPlusNode*)node->ptrs[child_idx - 1] : NULL;
                BPlusNode* right_sib = (child_idx < node->key_num) ? (BPlusNode*)node->ptrs[child_idx + 1] : NULL;

                // 尝试从左兄弟借调
                if (left_sib && left_sib->key_num > min_keys) {
                    if (child->is_leaf) redistribute_leaf(node, child_idx);
                }
                // 尝试从右兄弟借调
                else if (right_sib && right_sib->key_num > min_keys) {
                    if (child->is_leaf) redistribute_leaf_from_right(node, child_idx);
                }
                // 借调失败则合并
                else {
                    if (left_sib) {
                        if (child->is_leaf) merge_leaf(node, child_idx);
                    } else if (right_sib) {
                        if (child->is_leaf) merge_leaf(node, child_idx + 1);
                    }
                }
            }
        }
    }
}

// 删除入口函数
void delete_key(BPlusTree* tree, int key) {
    if (!tree->root || tree->root->key_num == 0) return;

    delete_node(tree->root, key);

    // 若根节点为空非叶节点，更新根为其子节点
    if (!tree->root->is_leaf && tree->root->key_num == 0) {
        BPlusNode* old_root = tree->root;
        tree->root = (BPlusNode*)old_root->ptrs[0];
        tree->root->parent = NULL;
        free(old_root);
    }
}

// 打印B+树结构
void print_bplus_tree(BPlusTree* tree, const char* operation_desc, FILE* log_file) {
    char msg[1024];
    snprintf(msg, sizeof(msg), "=== %s ===\n", operation_desc);
    log_message(msg, log_file);

    if (!tree->root) {
        log_message("树为空\n", log_file);
        return;
    }

    // 打印叶节点信息
    snprintf(msg, sizeof(msg), "Max Keys per node (n) = %d\n", MAX_KEYS);
    log_message(msg, log_file);

    BPlusNode* curr_leaf = tree->root;
    while (!curr_leaf->is_leaf) {
        curr_leaf = (BPlusNode*)curr_leaf->ptrs[0];
    }

    int leaf_idx = 0;
    while (curr_leaf) {
        snprintf(msg, sizeof(msg), "叶节点 %d: 键数量=%d | 键列表: ", leaf_idx, curr_leaf->key_num);
        log_message(msg, log_file);
        for (int i = 0; i < curr_leaf->key_num; i++) {
            snprintf(msg, sizeof(msg), "%d (data=%d) ", curr_leaf->keys[i], *(int*)curr_leaf->ptrs[i]);
            log_message(msg, log_file);
        }
        log_message("\n", log_file);
        curr_leaf = (BPlusNode*)curr_leaf->ptrs[MAX_KEYS + 1];  // 遍历下一个叶节点
        leaf_idx++;
    }
    log_message("-------------------------\n", log_file);
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

    // 测试数据
    int keys[] = {13, 49, 23, 45, 77, 3, 29, 14, 11, 78, 30, 40, 4, 5, 15, 16};
    int data[] = {111, 333, 222, 444, 555, 666, 777, 888, 999, 100, 101, 102, 103, 104, 105, 106};
    int n = sizeof(keys) / sizeof(keys[0]);

    // 插入测试
    for (int i = 0; i < n; i++) {
        insert(&tree, keys[i], &data[i]);
        char desc[128];
        snprintf(desc, sizeof(desc), "插入 key=%d (data=%d)", keys[i], data[i]);
        print_bplus_tree(&tree, desc, log_file);
    }

    // 查找测试
    int search_keys[] = {13, 23, 77, 29, 11, 30, 4, 15, 99};
    int sk_len = sizeof(search_keys) / sizeof(search_keys[0]);
    for (int i = 0; i < sk_len; i++) {
        int* result = (int*)search(tree.root, search_keys[i]);
        char msg[128];
        if (result) {
            snprintf(msg, sizeof(msg), "=== 查找 key=%d === 找到, data=%d\n", search_keys[i], *result);
        } else {
            snprintf(msg, sizeof(msg), "=== 查找 key=%d === 未找到\n", search_keys[i]);
        }
        log_message(msg, log_file);
    }

    // 删除测试
    int delete_keys[] = {23, 45, 11};
    int dk_len = sizeof(delete_keys) / sizeof(delete_keys[0]);
    for (int i = 0; i < dk_len; i++) {
        delete_key(&tree, delete_keys[i]);
        char desc[128];
        snprintf(desc, sizeof(desc), "删除 key=%d", delete_keys[i]);
        print_bplus_tree(&tree, desc, log_file);
    }

    fclose(log_file);
    return 0;
}