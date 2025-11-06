# 高级数据库组织 - 2025年秋季学期
## CS 525 - 所有班级
### 编程作业四：B+树
**截止日期**：2025年11月14日（周五）23点59分


## 1. 任务
本次作业的目标是实现一个B+树索引。该索引需由页文件（page file）提供支持，且索引的页需通过缓冲区管理器（buffer manager）进行访问。正如课堂上所讨论的，每个节点应占用一个页；但为了调试方便，需支持扇出（fan-out）更小的树，且仍保证每个节点占用一整个页。

B+树存储指向记录的指针（即上一次作业中引入的记录标识符RID），这些指针通过特定数据类型的键（key）进行索引。本次作业仅要求支持**DT_INT（整数）类型**的键（可选扩展部分可支持更多类型）。指向中间节点的指针应以节点所存储页的页号（page number）表示。

为简化测试，你的实现需遵循以下约定：

- **叶子节点分裂**：当插入操作导致叶子节点需要分裂时，若键的数量n为偶数，左节点应保留额外的键。例如：若n=2，且向节点[1,5]中插入键4，则分裂后得到的两个节点应为[1,4]和[5]。若n为奇数，可将键均匀分配给两个新节点。两种情况下，插入到父节点的键均为**右节点的最小键**。

- **非叶子节点分裂**：当插入操作导致非叶子节点需要分裂时，若键的数量n为奇数，无法将节点均匀分裂（其中一个新节点会多一个键）。此时，插入到父节点的中间键应从**右节点中选取**。例如：若n=3，且需分裂非叶子节点[1,3,4,5]，则分裂后得到的两个节点应为[1,3]和[5]，插入到父节点的键为4。

- **叶子节点下溢**：当叶子节点出现下溢时，你的实现应首先尝试从兄弟节点重新分配键；仅当重新分配失败时，才将该节点与某一兄弟节点合并。两种操作均应**优先选择左兄弟节点**。例如：若可同时从左兄弟和右兄弟借调键，则优先从左兄弟借调。


你可使用下图所示的B+树验证插入操作的实现正确性。该B+树由以下键插入序列生成：`1,11,13,17,23,52`，对应的RID为`1.1,2.3,1.2,3.5,4.4,3.2`。

```
根层：            [(ptr)      13      (ptr)      23           (ptr)]
                    ↓                  ↓                        ↓
叶子层：   [(ptr)1(ptr)11(ptr)] → [(ptr)13(ptr)17(ptr)] → [(ptr)23(ptr)52(ptr)] → NULL
             ↓     ↓                ↓      ↓                ↓      ↓  
RIDs：      1.1   2.3              1.2    3.5              4.4   3.2
```

## 2. 接口
```c
#ifndef BTREE_MGR_H
#define BTREE_MGR_H

#include "dberror.h"
#include "tables.h"

// 用于访问B+树的结构体
typedef struct BTreeHandle {
    DataType keyType;   // 键的数据类型
    char *idxId;        // 索引ID
    void *mgmtData;     // 管理数据（自定义）
} BTreeHandle;

// 用于B+树扫描的结构体
typedef struct BT_ScanHandle {
    BTreeHandle *tree;  // 指向对应的B+树
    void *mgmtData;     // 扫描管理数据（自定义）
} BT_ScanHandle;

// 索引管理器的初始化与关闭
extern RC initIndexManager(void *mgmtData);
extern RC shutdownIndexManager();

// B+树索引的创建、销毁、打开与关闭
extern RC createBtree(char *idxId, DataType keyType, int n);
extern RC openBtree(BTreeHandle **tree, char *idxId);
extern RC closeBtree(BTreeHandle *tree);
extern RC deleteBtree(char *idxId);

// B+树的信息查询
extern RC getNumNodes(BTreeHandle *tree, int *result);   // 获取节点总数
extern RC getNumEntries(BTreeHandle *tree, int *result); // 获取条目总数
extern RC getKeyType(BTreeHandle *tree, DataType *result); // 获取键的数据类型

// 索引的核心操作
extern RC findKey(BTreeHandle *tree, Value *key, RID *result);   // 查找键对应的RID
extern RC insertKey(BTreeHandle *tree, Value *key, RID rid);     // 插入键-RID对
extern RC deleteKey(BTreeHandle *tree, Value *key);              // 删除指定键
extern RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle); // 开启B+树扫描
extern RC nextEntry(BT_ScanHandle *handle, RID *result);         // 获取扫描的下一个条目
extern RC closeTreeScan(BT_ScanHandle *handle);                  // 关闭B+树扫描

// 调试与测试函数
extern char *printTree(BTreeHandle *tree); // 生成B+树的字符串表示

#endif // BTREE_MGR_H
```


### 2.1 索引管理器函数
这些函数用于初始化索引管理器、关闭索引管理器，并释放所有已分配的资源。


### 2.2 B+树函数
这些函数用于创建或销毁B+树索引。其中，**销毁索引（deleteBtree）** 还需删除对应的页文件。此外，客户端在访问B+树索引前，必须先通过`openBtree`函数打开索引；在调用`closeBtree`关闭B+树时，索引管理器需确保将索引中所有新增或修改过的页刷新回磁盘（需调用缓冲区管理器的对应函数实现）。


### 2.3 索引管理器函数（重复说明）
（原文此处存在重复，内容与2.1一致）这些函数用于初始化索引管理器、关闭索引管理器，并释放所有已分配的资源。


### 2.4 键操作函数
这些函数用于在指定B+树中查找、插入和删除键，具体功能如下：

- `findKey`：返回B+树中与目标键对应的RID；若键不存在，需返回错误码`RC_IM_KEY_NOT_FOUND`（详见`dberror.h`）。
- `insertKey`：向索引中插入新的“键-记录指针”对；若该键已存在，需返回错误码`RC_IM_KEY_ALREADY_EXISTS`。
- `deleteKey`：从索引中删除指定键及对应的记录指针；若键不存在，需返回错误码`RC_IM_KEY_NOT_FOUND`（是否将此视为错误由客户端决定）。

此外，客户端可通过`openTreeScan`、`nextEntry`和`closeTreeScan`三个方法，按排序顺序扫描B+树的所有条目：
- `nextEntry`：若扫描已超出B+树的最后一个条目（无更多条目可返回），需返回错误码`RC_IM_NO_MORE_ENTRIES`。


以下是扫描操作的使用示例：
```c
BT_ScanHandle *sc;
RID rid;
int rc;

// 开启B+树扫描
openTreeScan(btree, &sc);

// 循环获取下一个条目
while ((rc = nextEntry(sc, &rid)) == RC_OK)
    // 对获取的RID执行自定义操作
    ;

// 检查是否因“无更多条目”退出循环（非错误）
if (rc != RC_IM_NO_MORE_ENTRIES)
    // 处理其他错误
    ;

// 关闭扫描
closeTreeScan(sc);
```


### 2.5 调试函数
`printTree`函数用于生成B+树的字符串表示，供测试用例使用，也可辅助调试。你的代码需按以下规则生成格式：

- B+树的每个节点对应字符串中的一行。
- 节点的排列顺序需遵循**深度优先前序遍历**（depth-first pre-order）。
- 每个节点的表示格式为：`(位置)[指针,键,指针,...]`。其中：
  - 键需使用上一次作业中实现的`serializeValue`方法进行序列化表示。
  - 指向节点的指针需用“节点在深度优先前序遍历中的位置”表示（而非磁盘上的实际页号）。
  - RID需表示为`=页号.槽号`（如`=1.1`）。

以前文提到的B+树为例，其字符串表示如下：
```
(0)[1,13,2,23,3]
(1)[1.1,1,2.3,11,2]
(2)[1.2,13,3.5,17,3]
(3)[4.4,23,3.2,52]
```


## 3. 可选扩展
以下为可选功能扩展，不强制要求实现：

### 3.1 B+树与记录管理器集成
修改记录管理器，使其使用B+树进行查询。具体而言：
- 当通过记录管理器创建新关系（relation）时，允许用户指定表的某一属性作为键。
- 记录管理器需自动创建并维护B+树，用于存储该属性的键。
- 插入新记录时，需将对应的键插入B+树；查询指定键对应的记录时，需使用B+树而非全表扫描。
- 若需支持多属性键，可修改B+树接口以满足需求。


### 3.2 支持多种数据类型的键
允许B+树支持上一次作业（作业3）中引入的其他数据类型（如字符串、浮点数等）作为键。


### 3.3 实现指针混写（Pointer Swizzling）
当B+树节点加载到内存后，将指向该节点的逻辑指针（页号pagenum）转换为实际的内存指针。实现思路如下：
- B+树的页仅由其父中间节点指向，因此可在页加载时，将子节点的页号直接替换为内存指针（当代码尝试访问子节点时执行）。
- 复杂点在于：若缓冲区管理器将某页从缓冲区中淘汰（evict），需将所有指向该页的内存指针还原为页号。可行的实现方案为：
  1. 维护一个“记录数据结构”，存储所有已转换为内存指针的位置。
  2. 扩展缓冲区管理器，增加回调函数参数；当页被淘汰时，触发回调函数，通过“记录数据结构”找到所有需还原的指针位置，并将内存指针替换为页号。
  3. 该“记录数据结构”同样需处理以下场景：某节点被淘汰时，其内存中仍存在指向子节点的内存指针。


### 3.4 支持单个键对应多个条目
默认情况下，单个键仅能存储一个RID；扩展后，允许插入多个具有相同键的条目。实现要求如下：
- 允许修改`btree_mgr.h`接口，但**不可修改`findKey`函数的签名**；需新增`findAllEntries`函数，返回某一键对应的所有条目（数组/列表形式），而`findKey`仅返回第一个条目。
- 叶子节点中的指针不再直接指向RID，而是指向一个“存储该键所有条目的页”；若一个页无法容纳所有条目，需将额外条目存储在后续页中，并通过链表（每个页存储下一页的指针）连接这些页。


## 4. 源代码结构
你的源代码目录需按以下结构组织，且需复用已实现的存储管理器（storage manager）和缓冲区管理器（buffer manager）代码（若计划实现“记录管理器集成”扩展，还需复用记录管理器代码）。开始开发前，请将这些代码复制到`assign4`文件夹中。

### 4.1 目录结构要求
- 所有源代码文件需放在Git仓库的`assign4`文件夹中。
- 该文件夹至少需包含以下文件：
  - 提供的头文件（如`btree_mgr.h`、`dberror.h`等）和C文件。
  - 用于编译代码的`Makefile`。
  - 实现B+树功能的所有`.c`和`.h`文件。
  - `README.txt`/`README.md`：简要描述解决方案的文档（Markdown或文本格式）。


### 4.2 目录结构示例
```
git
└── assign4
    ├── Makefile
    ├── buffer_mgr.c
    ├── buffer_mgr.h
    ├── buffer_mgr_stat.c
    ├── buffer_mgr_stat.h
    ├── btree_mgr.c
    ├── btree_mgr.h
    ├── dberror.c
    ├── dberror.h
    ├── dt.h
    ├── expr.c
    ├── expr.h
    ├── record_mgr.c
    ├── record_mgr.h
    ├── rm_serializer.c
    ├── storage_mgr.h
    ├── tables.h
    ├── test_assign4_1.c
    ├── test_expr.c
    ├── test_helper.h
    └── README.md
```


## 5. 测试用例
### 5.1 `test_helper.h`
定义多个测试用例辅助方法，如断言函数`ASSERT_TRUE`。


### 5.2 `test_expr.c`
该文件使用`expr.h`接口实现多个测试用例。请在`Makefile`中配置编译规则，生成`test_expr`可执行文件。建议你扩展该文件，添加新测试用例，或以此为模板开发自定义测试文件。


### 5.3 `test_assign4_1.c`
该文件使用`btree_mgr.h`接口实现多个测试用例。请在`Makefile`中配置编译规则，生成`test_assign4`可执行文件。建议你扩展该文件，添加新测试用例，或以此为模板开发自定义测试文件。