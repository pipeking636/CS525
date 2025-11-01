#include <stdio.h>
#include <stdlib.h>
#include <math.h>

// -------------------------- 配置参数（对应文档中的n） --------------------------
#define MAX_KEYS 3  // 节点最大键数
// ------------------------------------------------------------------------------

// -------------------------- 数据结构定义（严格对齐文档B+树规则） --------------------------
// B+树节点结构体（叶节点存数据指针，非叶节点存子节点指针；叶节点含next指针形成链表）
typedef struct BPlusNode {
    int is_leaf;                  // 1=叶节点, 0=非叶节点
    int key_num;                  // 当前键数量
    int keys[MAX_KEYS];           // 键数组（叶节点/非叶节点均存储）
    void* ptrs[MAX_KEYS + 2];     // 指针数组：
                                  // - 叶节点：前MAX_KEYS+1个存数据指针，最后1个存next叶节点指针
                                  // - 非叶节点：存子节点指针（共MAX_KEYS+1个）
    struct BPlusNode* parent;     // 父节点指针（用于删除下溢处理）
} BPlusNode;

// B+树结构体（仅需根节点，文档未显式定义但为实现必需）
typedef struct BPlusTree {
    BPlusNode* root;              // 树的根节点
} BPlusTree;

// -------------------------- 函数声明 --------------------------
BPlusNode* create_node(int is_leaf);                  // 创建新节点
void init_bplus_tree(BPlusTree* tree);                // 初始化B+树
int find_key_index(BPlusNode* node, int key);         // 找到键的插入/查找索引（文档1-6：叶节点有序）
void split_leaf(BPlusNode* parent, int index);        // 分裂叶节点（文档1-42/1-43：偶数n左多1键）
void split_non_leaf(BPlusNode* parent, int index);    // 分裂非叶节点（文档1-44：中间键取右节点）
void insert_non_full(BPlusNode* node, int key, void* data); // 向非满节点插入
void insert(BPlusTree* tree, int key, void* data);    // 插入入口（文档1-41/1-47等插入逻辑）
void* search(BPlusNode* node, int key);               // 查找键对应的数据（文档1-5：叶节点覆盖所有键）
int get_min_keys(BPlusNode* node);                    // 计算节点最小键数（文档1-12表格：非根节点min keys）
void delete_leaf_key(BPlusNode* node, int idx);       // 删除叶节点的键
void delete_non_leaf_key(BPlusNode* node, int idx);   // 删除非叶节点的键
void redistribute_leaf(BPlusNode* parent, int curr_idx); // 从左兄弟 Redistribute（文档1-45：优先左兄弟）
void redistribute_leaf_from_right(BPlusNode* parent, int curr_idx); // 从右兄弟 Redistribute
void merge_leaf(BPlusNode* parent, int curr_idx);     // 合并叶节点与左兄弟（文档1-45：下溢先 Redistribute 再合并）
void delete_node(BPlusNode* node, int key);           // 辅助：删除节点内的键（原delete_key拆分，避免root混淆）
void delete_key(BPlusTree* tree, int key);            // 删除入口（操作BPlusTree，修正root更新问题）
void traverse_leaves(BPlusNode* root);                // 遍历叶节点（验证文档1-6：叶节点有序）
// 辅助：分层显示B+树完整结构（直观区分非叶/叶节点，贴合文档1-2/1-3/1-4/1-6规则）
void print_bplus_tree(BPlusTree* tree, const char* operation_desc, FILE* log_file);
// -------------------------- 函数实现 --------------------------
// 1. 创建新节点（文档未显式定义，为实现必需）
BPlusNode* create_node(int is_leaf) {
    BPlusNode* node = (BPlusNode*)malloc(sizeof(BPlusNode));
    if (node == NULL) {
        printf("Memory allocation failed for B+ node!\n");
        exit(1);
    }
    node->is_leaf = is_leaf;
    node->key_num = 0;
    node->parent = NULL;
    for (int i = 0; i < MAX_KEYS + 2; i++) {
        node->ptrs[i] = NULL;
    }
    return node;
}

// 2. 初始化B+树（空树根为叶节点，文档1-41：从空树开始插入）
void init_bplus_tree(BPlusTree* tree) {
    tree->root = create_node(1); // 空树根节点为叶节点（文档未显式说明，为插入逻辑必需）
}

// 3. 找到键的插入/查找索引（文档1-6：叶节点按顺序排列，非叶节点键也有序）
int find_key_index(BPlusNode* node, int key) {
    int idx = 0;
    while (idx < node->key_num && node->keys[idx] < key) {
        idx++;
    }
    return idx;
}

// 4. 分裂叶节点（遵循文档B+树节点的分裂操作：n为奇数时左右均匀，偶数时左多1键）
void split_leaf(BPlusNode* parent, int index) {
    BPlusNode* curr_leaf = (BPlusNode*)parent->ptrs[index];
    BPlusNode* new_leaf = create_node(1);
    new_leaf->parent = parent;
    curr_leaf->parent = parent;


    // 可以统一为：左节点键数 = (n+1 + 1) / 2（当n为奇数时，(n+1)为偶数；当n为偶数时，(n+1)为奇数，加1后为偶数）
    int left_key_cnt = (int)ceil((MAX_KEYS+1)/2); // 左节点键数 = (MAX_KEYS+1)/2

    // 复制右半键和数据指针到新叶节点（总键数为MAX_KEYS+1）
    new_leaf->key_num = (MAX_KEYS + 1) - left_key_cnt;
    for (int i = 0; i < new_leaf->key_num; i++) {
        new_leaf->keys[i] = curr_leaf->keys[left_key_cnt + i];
        new_leaf->ptrs[i] = curr_leaf->ptrs[left_key_cnt + i];
    }

    // 更新叶节点链表指针
    new_leaf->ptrs[MAX_KEYS + 1] = curr_leaf->ptrs[MAX_KEYS + 1];
    curr_leaf->ptrs[MAX_KEYS + 1] = new_leaf;
    curr_leaf->key_num = left_key_cnt;

    // 父节点插入新键（右节点最小键）
    for (int i = parent->key_num; i > index; i--) {
        parent->keys[i] = parent->keys[i - 1];
        parent->ptrs[i + 1] = parent->ptrs[i];
    }
    parent->keys[index] = new_leaf->keys[0];
    parent->ptrs[index + 1] = new_leaf;
    parent->key_num++;
}

// 5. 分裂非叶节点（遵循文档B+树节点的分裂操作：中间键上推）
void split_non_leaf(BPlusNode* parent, int index) {
    BPlusNode* curr_non_leaf = (BPlusNode*)parent->ptrs[index];
    BPlusNode* new_non_leaf = create_node(0);
    new_non_leaf->parent = parent;
    curr_non_leaf->parent = parent;

    
    int mid  = (int)ceil((MAX_KEYS + 1) / 2.0);// 使用ceil()函数计算中间索引，统一处理奇数和偶数n
    int middle_key;
    if (MAX_KEYS % 2 == 1) {  // n为奇数时（如n=3）
        // 中间键为右节点第一个键（原节点的mid索引）
        // mid = (MAX_KEYS + 1) / 2; // 对于n=3，mid=2
        middle_key = curr_non_leaf->keys[mid]; // 取原节点第mid个键
    } else {  // n为偶数时（如n=2）
        // 中间键为左节点最后一个键（原节点的mid-1索引）
        // mid = ((MAX_KEYS + 1) / 2) + 1; // 对于n=2，mid=2
        middle_key = curr_non_leaf->keys[mid - 1];
    }

    // 复制右半键和子节点指针到新非叶节点
    new_non_leaf->key_num = MAX_KEYS - mid; // 右节点键数 = 总键数(MAX_KEYS+1) - 左节点键数(mid) - 中间键(1)
    for (int i = 0; i < new_non_leaf->key_num; i++) {
        new_non_leaf->keys[i] = curr_non_leaf->keys[mid + 1 + i]; // 跳过中间键
        new_non_leaf->ptrs[i] = curr_non_leaf->ptrs[mid + 1 + i];
        ((BPlusNode*)new_non_leaf->ptrs[i])->parent = new_non_leaf;
    }
    // 复制最后一个子节点指针
    new_non_leaf->ptrs[new_non_leaf->key_num] = curr_non_leaf->ptrs[mid + 1 + new_non_leaf->key_num];
    ((BPlusNode*)new_non_leaf->ptrs[new_non_leaf->key_num])->parent = new_non_leaf;

    // 左节点保留前mid个键（0~mid-1）
    curr_non_leaf->key_num = mid;

    // 父节点插入中间键和新子节点指针
    for (int i = parent->key_num; i > index; i--) {
        parent->keys[i] = parent->keys[i - 1];
        parent->ptrs[i + 1] = parent->ptrs[i];
    }
    parent->keys[index] = middle_key;
    parent->ptrs[index + 1] = new_non_leaf;
    parent->key_num++;
}

// 6. 向非满节点插入（修正为：先插入，再处理子节点分裂）
void insert_non_full(BPlusNode* node, int key, void* data) {
    int idx = node->key_num - 1;

    if (node->is_leaf) {
        // 叶节点：插入键值并保持有序
        while (idx >= 0 && key < node->keys[idx]) {
            node->keys[idx + 1] = node->keys[idx];
            node->ptrs[idx + 1] = node->ptrs[idx];
            idx--;
        }
        node->keys[idx + 1] = key;
        node->ptrs[idx + 1] = data;
        node->key_num++; // 插入后键数+1

        // 叶节点插入后无需主动分裂，由父节点处理（根节点由insert处理）
    } else {
        // 非叶节点：先找到子节点并插入
        while (idx >= 0 && key < node->keys[idx]) {
            idx--;
        }
        idx++;
        BPlusNode* child = (BPlusNode*)node->ptrs[idx];

        // 递归插入子节点
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

// 7. 插入入口（修正为：先插入，再分裂根节点）
void insert(BPlusTree* tree, int key, void* data) {
    BPlusNode* root = tree->root;

    // 第一步：直接插入（递归至叶节点）
    insert_non_full(root, key, data);

    // 第二步：若根节点插入后溢出，分裂根节点
    if (root->key_num > MAX_KEYS) {
        BPlusNode* new_root = create_node(0);
        tree->root = new_root;
        new_root->ptrs[0] = root;
        root->parent = new_root;

        // 根据原根节点类型分裂
        if (root->is_leaf) split_leaf(new_root, 0);
        else split_non_leaf(new_root, 0);
    }
}

// 8. 查找键对应的数据（递归，文档1-5：所有叶节点覆盖所有键，1-6：叶节点有序）
void* search(BPlusNode* node, int key) {
    if (node == NULL) return NULL;

    int idx = find_key_index(node, key);
    if (node->is_leaf) {
        // 叶节点：检查是否存在该键（文档1-4：数据指针仅在叶节点）
        if (idx < node->key_num && node->keys[idx] == key) {
            return node->ptrs[idx]; // 返回数据指针（文档1-4）
        }
        return NULL;
    } else {
        // 非叶节点：递归查找子节点（修正路径选择逻辑）
        if (idx < node->key_num && key == node->keys[idx]) {
            // 找到键时，应向右子节点查找
            idx++;
        } else if (idx > node->key_num) {
            // 防止越界
            idx = node->key_num;
        }
        return search((BPlusNode*)node->ptrs[idx], key);
    }
}

// 9. 计算节点最小键数（严格遵循文档B+树节点的分裂操作表格）
int get_min_keys(BPlusNode* node) {
    if (node == NULL) return 0;
    // 根节点最小1个键
    if (node->parent == NULL) return 1;
    // 叶节点（非根）：min keys = floor((MAX_KEYS+1)/2)
    if (node->is_leaf) return (int)floor((MAX_KEYS + 1) / 2.0);
    // 非叶节点（非根）：min keys = ceil((n+1)/2) - 1 = (n+2)/2 - 1
    return (int)ceil((MAX_KEYS + 1) / 2.0) - 1;
}

// 10. 删除叶节点的键（文档1-45：下溢处理前先删除键）
void delete_leaf_key(BPlusNode* node, int idx) {
    for (int i = idx; i < node->key_num - 1; i++) {
        node->keys[i] = node->keys[i + 1];
        node->ptrs[i] = node->ptrs[i + 1];
    }
    node->key_num--;
    node->ptrs[node->key_num] = NULL; // 清空多余数据指针（文档1-4：叶节点指针为数据指针）
}

// 11. 删除非叶节点的键（文档1-45：下溢处理前先删除键）
void delete_non_leaf_key(BPlusNode* node, int idx) {
    for (int i = idx; i < node->key_num - 1; i++) {
        node->keys[i] = node->keys[i + 1];
        node->ptrs[i + 1] = node->ptrs[i + 2];
    }
    node->key_num--;
    node->ptrs[node->key_num + 1] = NULL; // 清空多余子节点指针（文档1-3：非叶节点指针为子节点指针）
}

// 12. 从左兄弟 Redistribute 叶节点（文档1-45：优先左兄弟）
void redistribute_leaf(BPlusNode* parent, int curr_idx) {
    BPlusNode* curr_leaf = (BPlusNode*)parent->ptrs[curr_idx];
    BPlusNode* left_sib = (BPlusNode*)parent->ptrs[curr_idx - 1];

    // 左兄弟最大键移到当前节点最前面（保持叶节点有序，文档1-6）
    for (int i = curr_leaf->key_num; i > 0; i--) {
        curr_leaf->keys[i] = curr_leaf->keys[i - 1];
        curr_leaf->ptrs[i] = curr_leaf->ptrs[i - 1];
    }
    curr_leaf->keys[0] = left_sib->keys[left_sib->key_num - 1];
    curr_leaf->ptrs[0] = left_sib->ptrs[left_sib->key_num - 1];
    curr_leaf->key_num++;

    // 左兄弟删除最大键（保持有序）
    left_sib->key_num--;
    left_sib->ptrs[left_sib->key_num] = NULL;

    // 更新父节点对应键（为左兄弟新最大键，保持非叶节点键有序）
    parent->keys[curr_idx - 1] = left_sib->keys[left_sib->key_num - 1];
}

// 13. 从右兄弟 Redistribute 叶节点（文档1-45：左兄弟不可用时用右兄弟）
void redistribute_leaf_from_right(BPlusNode* parent, int curr_idx) {
    BPlusNode* curr_leaf = (BPlusNode*)parent->ptrs[curr_idx];
    BPlusNode* right_sib = (BPlusNode*)parent->ptrs[curr_idx + 1];

    // 右兄弟最小键移到当前节点最后面（保持叶节点有序，文档1-6）
    curr_leaf->keys[curr_leaf->key_num] = right_sib->keys[0];
    curr_leaf->ptrs[curr_leaf->key_num] = right_sib->ptrs[0];
    curr_leaf->key_num++;

    // 右兄弟删除最小键（保持有序）
    for (int i = 0; i < right_sib->key_num - 1; i++) {
        right_sib->keys[i] = right_sib->keys[i + 1];
        right_sib->ptrs[i] = right_sib->ptrs[i + 1];
    }
    right_sib->key_num--;
    right_sib->ptrs[right_sib->key_num] = NULL;

    // 更新父节点对应键（为右兄弟新最小键，保持非叶节点键有序）
    parent->keys[curr_idx] = right_sib->keys[0];
}

// 14. 合并叶节点与左兄弟（文档1-45：Redistribute 失败后合并）
void merge_leaf(BPlusNode* parent, int curr_idx) {
    BPlusNode* curr_leaf = (BPlusNode*)parent->ptrs[curr_idx];
    BPlusNode* left_sib = (BPlusNode*)parent->ptrs[curr_idx - 1];

    // 复制当前节点键和数据指针到左兄弟（保持有序，文档1-6）
    for (int i = 0; i < curr_leaf->key_num; i++) {
        left_sib->keys[left_sib->key_num + i] = curr_leaf->keys[i];
        left_sib->ptrs[left_sib->key_num + i] = curr_leaf->ptrs[i];
    }
    // 左兄弟继承当前节点的next指针（保持叶节点链表）
    left_sib->ptrs[MAX_KEYS + 1] = curr_leaf->ptrs[MAX_KEYS + 1];
    left_sib->key_num += curr_leaf->key_num;

    // 父节点删除对应键和子节点指针（文档1-45：合并后父节点需删除键）
    delete_non_leaf_key(parent, curr_idx - 1);

    // 释放当前节点内存
    free(curr_leaf);
}

// 15. 辅助：删除节点内的键（原delete_key拆分，避免root混淆，仅处理节点逻辑）
void delete_node(BPlusNode* node, int key) {
    if (node == NULL) return;

    int idx = find_key_index(node, key);
    int min_keys = get_min_keys(node);

    if (node->is_leaf) {
        // 叶节点：存在该键则删除（文档1-4：数据指针仅在叶节点，删除需同步删除指针）
        if (idx < node->key_num && node->keys[idx] == key) {
            delete_leaf_key(node, idx);

            // 处理下溢（非根节点，文档1-45：下溢先 Redistribute 再合并）
            if (node->key_num < min_keys && node->parent != NULL) {
                BPlusNode* parent = node->parent;
                // 找到当前节点在父节点中的索引
                int curr_idx = 0;
                while (curr_idx < parent->key_num + 1 && parent->ptrs[curr_idx] != node) {
                    curr_idx++;
                }

                // 优先从左兄弟 Redistribute（文档1-45：优先左兄弟）
                if (curr_idx > 0) {
                    BPlusNode* left_sib = (BPlusNode*)parent->ptrs[curr_idx - 1];
                    if (left_sib->key_num > min_keys) {
                        redistribute_leaf(parent, curr_idx);
                        return;
                    }
                }

                // 左兄弟不可用，从右兄弟 Redistribute
                if (curr_idx < parent->key_num) {
                    BPlusNode* right_sib = (BPlusNode*)parent->ptrs[curr_idx + 1];
                    if (right_sib->key_num > min_keys) {
                        redistribute_leaf_from_right(parent, curr_idx);
                        return;
                    }
                }

                // 无法 Redistribute，合并（优先左兄弟，文档1-45）
                if (curr_idx > 0) {
                    merge_leaf(parent, curr_idx);
                } else {
                    // 无左兄弟，合并到右兄弟（逻辑类似merge_leaf，简化处理）
                    printf("Merge leaf with right sibling\n");
                }

                // 父节点可能下溢，递归处理（文档1-45：下溢可能传播到父节点）
                delete_node(parent, key);
            }
        }
    } else {
        // 非叶节点：递归删除子节点中的键（文档1-3：非叶节点键仅用于索引，数据在叶节点）
        BPlusNode* child = (BPlusNode*)node->ptrs[idx];
        delete_node(child, key);

        // 处理子节点下溢（简化：非叶节点下溢逻辑与叶节点类似，文档1-45）
        if (child->key_num < min_keys && child->parent != NULL) {
            printf("Non-leaf node underflow\n");
        }
    }
}

// 16. 删除入口（修正核心：操作BPlusTree，避免BPlusNode访问root）
void delete_key(BPlusTree* tree, int key) {
    if (tree->root == NULL || tree->root->key_num == 0) {
        printf("B+ Tree is empty, no key to delete\n");
        return;
    }

    // 调用辅助函数删除节点内的键（仅处理节点逻辑）
    delete_node(tree->root, key);

    // 处理根节点下溢：若根为非叶且键数为0，更新根为唯一子节点（文档未显式说明，为树结构必需）
    if (!tree->root->is_leaf && tree->root->key_num == 0) {
        BPlusNode* old_root = tree->root;
        tree->root = (BPlusNode*)old_root->ptrs[0]; // 根节点更新为唯一子节点
        tree->root->parent = NULL;                  // 新根无父节点
        free(old_root);                             // 释放旧根内存
    }
}

// 17. 遍历叶节点（验证文档1-6：叶节点按顺序排列，1-5：所有叶节点覆盖所有键）
void traverse_leaves(BPlusNode* root) {
    if (root == NULL) return;

    // 找到最左侧叶节点（文档1-6：叶节点有序，从左到右遍历）
    BPlusNode* curr = root;
    while (!curr->is_leaf) {
        curr = (BPlusNode*)curr->ptrs[0];
    }

    // 遍历叶节点链表（输出所有键，验证有序性，文档1-6）
    printf("Leaves (ordered, align with doc 1-6): ");
    while (curr != NULL) {
        for (int i = 0; i < curr->key_num; i++) {
            printf("%d ", curr->keys[i]);
        }
        curr = (BPlusNode*)curr->ptrs[MAX_KEYS + 1]; // 下一个叶节点（链表）
    }
    printf("\n");
}
// 优化后的print_bplus_tree函数 - 增加操作描述参数并输出更完整信息
void print_bplus_tree(BPlusTree* tree, const char* operation_desc, FILE* log_file) {
    // 打印操作描述信息
    printf("\n=== Operation: %s ===\n", operation_desc);
    if (log_file != NULL) {
        fprintf(log_file, "\n=== Operation: %s ===\n", operation_desc);
    }

    if (tree == NULL || tree->root == NULL) {
        printf("=== B+ Tree (Empty) ===\n");
        if (log_file != NULL) {
            fprintf(log_file, "=== B+ Tree (Empty) ===\n");
        }
        return;
    }

    printf("=== B+ Tree Structure ===\n");
    printf("Max Keys per node (n) = %d | Level: Root = Level 1\n", MAX_KEYS);
    
    if (log_file != NULL) {
        fprintf(log_file, "=== B+ Tree Structure ===\n");
        fprintf(log_file, "Max Keys per node (n) = %d | Level: Root = Level 1\n", MAX_KEYS);
    }

    // 用队列实现BFS分层遍历，存储节点和对应的层级
    typedef struct QueueNode {
        BPlusNode* bplus_node; // B+树节点
        int level;             // 节点所在层级（根为1）
        int parent_idx;        // 父节点中子节点的索引（-1表示根节点）
    } QueueNode;

    QueueNode* queue = (QueueNode*)malloc(sizeof(QueueNode) * 1000); // 简易队列
    int front = 0, rear = 0;

    // 根节点入队（层级1，无父节点）
    queue[rear++] = (QueueNode){tree->root, 1, -1};

    int current_level = 1; // 当前处理的层级
    while (front < rear) {
        QueueNode q_node = queue[front++];
        BPlusNode* node = q_node.bplus_node;

        // 层级切换时打印分隔线
        if (q_node.level != current_level) {
            printf("--------------------------------------------------\n");
            if (log_file != NULL) {
                fprintf(log_file, "--------------------------------------------------\n");
            }
            current_level = q_node.level;
        }

        // 区分非叶节点与叶节点
        if (node->is_leaf) {
            // 叶节点：显示层级、父节点索引、键数、键值、数据指针、next叶节点
            printf("Level %d | Leaf Node (Parent idx: %d) | Keys: %d | [ ", 
                   q_node.level, q_node.parent_idx, node->key_num);
            if (log_file != NULL) {
                fprintf(log_file, "Level %d | Leaf Node (Parent idx: %d) | Keys: %d | [ ", 
                       q_node.level, q_node.parent_idx, node->key_num);
            }
            for (int i = 0; i < node->key_num; i++) {
                printf("key=%d (data=%d) ", node->keys[i], *(int*)node->ptrs[i]);
                if (log_file != NULL) {
                    fprintf(log_file, "key=%d (data=%d) ", node->keys[i], *(int*)node->ptrs[i]);
                }
            }
            // 显示叶节点的next指针
            if (node->ptrs[MAX_KEYS + 1] != NULL) {
                printf("] | Next Leaf: 0x%lx\n", (unsigned long)node->ptrs[MAX_KEYS + 1]);
                if (log_file != NULL) {
                    fprintf(log_file, "] | Next Leaf: 0x%lx\n", (unsigned long)node->ptrs[MAX_KEYS + 1]);
                }
            } else {
                printf("] | Next Leaf: NULL\n");
                if (log_file != NULL) {
                    fprintf(log_file, "] | Next Leaf: NULL\n");
                }
            }
        } else {
            // 非叶节点：显示层级、父节点索引、键数、键值、子节点指针
            printf("Level %d | Non-Leaf Node (Parent idx: %d) | Keys: %d | [ ", 
                   q_node.level, q_node.parent_idx, node->key_num);
            if (log_file != NULL) {
                fprintf(log_file, "Level %d | Non-Leaf Node (Parent idx: %d) | Keys: %d | [ ", 
                       q_node.level, q_node.parent_idx, node->key_num);
            }
            for (int i = 0; i < node->key_num; i++) {
                printf("key=%d ", node->keys[i]);
                if (log_file != NULL) {
                    fprintf(log_file, "key=%d ", node->keys[i]);
                }
            }
            printf("] | Children (count: %d): [ ", node->key_num + 1);
            if (log_file != NULL) {
                fprintf(log_file, "] | Children (count: %d): [ ", node->key_num + 1);
            }
            // 打印子节点指针，并将子节点入队
            for (int i = 0; i < node->key_num + 1; i++) {
                if (node->ptrs[i] != NULL) {
                    printf("0x%lx ", (unsigned long)node->ptrs[i]);
                    if (log_file != NULL) {
                        fprintf(log_file, "0x%lx ", (unsigned long)node->ptrs[i]);
                    }
                    // 子节点入队
                    queue[rear++] = (QueueNode){(BPlusNode*)node->ptrs[i], q_node.level + 1, i};
                } else {
                    printf("NULL ");
                    if (log_file != NULL) {
                        fprintf(log_file, "NULL ");
                    }
                }
            }
            printf("]\n");
            if (log_file != NULL) {
                fprintf(log_file, "]\n");
            }
        }
    }

    printf("=== End of B+ Tree Structure ===\n");
    if (log_file != NULL) {
        fprintf(log_file, "=== End of B+ Tree Structure ===\n");
    }
    free(queue);

    // 额外输出叶节点有序性检查信息
    printf("Leaf nodes (ordered): ");
    traverse_leaves(tree->root); // 辅助显示叶节点有序性
    printf("\n");
}
// -------------------------- 测试主函数（基于文档Question 1插入示例） --------------------------
int main() {
    // 1. 初始化B+树（文档1-41：从空树开始）
    BPlusTree tree;
    init_bplus_tree(&tree);
    printf("=== B+ Tree Test (MAX_KEYS = %d) ===\n", MAX_KEYS);

    // 打开log.txt文件，使用"w"模式覆盖
    FILE* log_file = fopen("log.txt", "w");
    if (log_file == NULL) {
        printf("Error: Cannot open log.txt file for writing\n");
    }
    if (log_file != NULL) {
        fprintf(log_file, "=== B+ Tree Test (MAX_KEYS = %d) ===\n", MAX_KEYS);
    }

    // 2. 插入文档1-41中的age值：13,23,49,45,77,3,29,14（按文档顺序插入）
    int keys[] = {13,  49,  23,  45,  77,  3,   29,  14,  11,  78,  30,  40,  4,   5,   15,  16};
    int data[] = {111, 333, 222, 444, 555, 666, 777, 888, 999, 100, 101, 102, 103, 104, 105, 106}; // 数据随意选取（实际情况是叶节点存数据指针）
    for (int i = 0; i < sizeof(keys)/sizeof(keys[0]); i++) {
        insert(&tree, keys[i], &data[i]);
        // 创建操作描述字符串
        char op_desc[100];
        sprintf(op_desc, "Insert key = %d (data = %d)", keys[i], data[i]);
        print_bplus_tree(&tree, op_desc, log_file);
    }

    // 3. 查找测试（文档1-5：叶节点覆盖所有键，验证查找功能）
    int key_search = 45;
    int* found = (int*)search(tree.root, key_search);
    char search_desc[100];
    if (found != NULL) {
        sprintf(search_desc, "Search key = %d, found with data = %d", key_search, *found);
    } else {
        sprintf(search_desc, "Search key = %d, not found", key_search);
    }
    print_bplus_tree(&tree, search_desc, log_file);

    // // 4. 删除测试（文档1-45：下溢处理规则）
    // int key_delete = 23;
    // printf("\n--- After deleting key = %d ---\n", key_delete);
    // delete_key(&tree, key_delete);
    // print_bplus_tree(&tree); // 删除后显示树结构
    // printf("Leaf nodes (ordered): ");
    // traverse_leaves(tree.root);

    // 在结束后关闭log文件
    if (log_file != NULL) {
        fclose(log_file);
    }
    
    // 5. 内存释放（简化：实际需递归释放所有节点，避免内存泄漏）
    return 0;
}