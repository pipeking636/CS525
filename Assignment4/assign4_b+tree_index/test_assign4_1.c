#include <stdlib.h>

#include "dberror.h"
#include "expr.h"
#include "btree_mgr.h"
#include "tables.h"
#include "test_helper.h"

#define ASSERT_EQUALS_RID(_l,_r, message)				\
  do {									\
    ASSERT_TRUE((_l).page == (_r).page && (_l).slot == (_r).slot, message); \
  } while(0)

// // my test methods
// static void testBtreeInitAndInfo(void);
// static void testBtreeInsert(void);
// static void testBtreeFind(void);
// static void testBtreeDelete(void);
// static void testBtreeScan(void);
// static void testBtreeEdgeCases(void);

// offical test methods
static void testInsertAndFind (void);
static void testDelete (void);
static void testIndexScan (void);

// helper methods
static Value **createValues (char **stringVals, int size);
static void freeValues (Value **vals, int size);
static int *createPermutation (int size);

// test name
char *testName;

// main method
int 
main (void) 
{
  testName = "";

  // my test methods
  // testBtreeInitAndInfo();
  // testBtreeInsert();
  // testBtreeFind();
  // testBtreeDelete();
  // testBtreeScan();
  // testBtreeEdgeCases();

  // official test methods
  testInsertAndFind();
  testDelete();
  testIndexScan();

  return 0;
}
/*=============================my test methods========================*/
// static void testBtreeInitAndInfo(void)
// {
//   testName = "test B+ tree initialization and basic info";
//   BTreeHandle *tree = NULL;
//   DataType keyType;
//   int nodeCount, entryCount;

//   // 初始化索引管理器
//   TEST_CHECK(initIndexManager(NULL));

//   // 测试创建B树（键类型为INT，阶数为3）
//   TEST_CHECK(createBtree("init_test_idx", DT_INT, 3));
//   TEST_CHECK(openBtree(&tree, "init_test_idx"));

//   // 验证键类型
//   TEST_CHECK(getKeyType(tree, &keyType));
//   ASSERT_EQUALS_INT(keyType, DT_INT, "check key type is INT");

//   // 新树应无节点和条目
//   TEST_CHECK(getNumNodes(tree, &nodeCount));
//   ASSERT_EQUALS_INT(nodeCount, 0, "new tree should have 0 nodes");
//   TEST_CHECK(getNumEntries(tree, &entryCount));
//   ASSERT_EQUALS_INT(entryCount, 0, "new tree should have 0 entries");

//   // 清理资源
//   TEST_CHECK(closeBtree(tree));
//   TEST_CHECK(deleteBtree("init_test_idx"));
//   TEST_CHECK(shutdownIndexManager());

//   TEST_DONE();
// }

// static void testBtreeInsert(void)
// {
//   testName = "test B+ tree insert operations";
//   BTreeHandle *tree = NULL;
//   Value **keys;
//   char *stringKeys[] = {"i10", "i20", "i30", "i20"}; // 包含重复键"i20"
//   int numInserts = 4;
//   RID rids[] = {{1,1}, {1,2}, {1,3}, {1,4}};
//   int entryCount;

//   // 准备键值
//   keys = createValues(stringKeys, numInserts);

//   // 初始化
//   TEST_CHECK(initIndexManager(NULL));
//   TEST_CHECK(createBtree("insert_test_idx", DT_INT, 2));
//   TEST_CHECK(openBtree(&tree, "insert_test_idx"));

//   // 插入前3个不重复键
//   for (int i = 0; i < 3; i++) {
//       TEST_CHECK(insertKey(tree, keys[i], rids[i]));
//   }

//   // 验证条目数（应等于3）
//   TEST_CHECK(getNumEntries(tree, &entryCount));
//   ASSERT_EQUALS_INT(entryCount, 3, "entry count after 3 inserts");

//   // 测试重复插入（预期错误：RC_IM_KEY_ALREADY_EXISTS）
//   ASSERT_ERROR(insertKey(tree, keys[3], rids[3]), "insert duplicate key should fail");

//   // 清理
//   TEST_CHECK(closeBtree(tree));
//   TEST_CHECK(deleteBtree("insert_test_idx"));
//   TEST_CHECK(shutdownIndexManager());
//   freeValues(keys, numInserts);

//   TEST_DONE();
// }

// static void testBtreeFind(void)
// {
//   testName = "test B+ tree find operations";
//   BTreeHandle *tree = NULL;
//   Value **keys;
//   char *stringKeys[] = {"i5", "i15", "i25"};
//   int numKeys = 3;
//   RID insertRids[] = {{2,1}, {2,2}, {2,3}};
//   RID foundRid;
//   Value *nonExistentKey = stringToValue("i10"); // 不存在的键

//   keys = createValues(stringKeys, numKeys);

//   // 初始化并插入数据
//   TEST_CHECK(initIndexManager(NULL));
//   TEST_CHECK(createBtree("find_test_idx", DT_INT, 2));
//   TEST_CHECK(openBtree(&tree, "find_test_idx"));
//   for (int i = 0; i < numKeys; i++) {
//       TEST_CHECK(insertKey(tree, keys[i], insertRids[i]));
//   }

//   // 查找存在的键
//   for (int i = 0; i < numKeys; i++) {
//       TEST_CHECK(findKey(tree, keys[i], &foundRid));
//       ASSERT_EQUALS_RID(foundRid, insertRids[i], "find existing key");
//   }

//   // 查找不存在的键（预期错误：RC_IM_KEY_NOT_FOUND）
//   int rc = findKey(tree, nonExistentKey, &foundRid);
//   ASSERT_TRUE(rc == RC_IM_KEY_NOT_FOUND, "find non-existent key should fail");

//   // 清理
//   TEST_CHECK(closeBtree(tree));
//   TEST_CHECK(deleteBtree("find_test_idx"));
//   TEST_CHECK(shutdownIndexManager());
//   freeValues(keys, numKeys);
//   free(nonExistentKey);

//   TEST_DONE();
// }

// static void testBtreeDelete(void)
// {
//   testName = "test B+ tree delete operations";
//   BTreeHandle *tree = NULL;
//   Value **keys;
//   char *stringKeys[] = {"i3", "i6", "i9", "i12"};
//   int numKeys = 4;
//   RID rids[] = {{3,1}, {3,2}, {3,3}, {3,4}};
//   RID foundRid;
//   int entryCount;

//   keys = createValues(stringKeys, numKeys);

//   // 初始化并插入数据
//   TEST_CHECK(initIndexManager(NULL));
//   TEST_CHECK(createBtree("delete_test_idx", DT_INT, 3));
//   TEST_CHECK(openBtree(&tree, "delete_test_idx"));
//   for (int i = 0; i < numKeys; i++) {
//       TEST_CHECK(insertKey(tree, keys[i], rids[i]));
//   }

//   // 删除第2个键（"i6"）
//   TEST_CHECK(deleteKey(tree, keys[1]));
//   // 验证条目数（4-1=3）
//   TEST_CHECK(getNumEntries(tree, &entryCount));
//   ASSERT_EQUALS_INT(entryCount, 3, "entry count after delete");
//   // 验证删除的键无法找到
//   int rc = findKey(tree, keys[1], &foundRid);
//   ASSERT_TRUE(rc == RC_IM_KEY_NOT_FOUND, "deleted key should not be found");

//   // 测试删除不存在的键（预期错误）
//   Value *nonExistentKey = stringToValue("i15");
//   ASSERT_ERROR(deleteKey(tree, nonExistentKey), "delete non-existent key should fail");

//   // 清理
//   TEST_CHECK(closeBtree(tree));
//   TEST_CHECK(deleteBtree("delete_test_idx"));
//   TEST_CHECK(shutdownIndexManager());
//   freeValues(keys, numKeys);
//   free(nonExistentKey);

//   TEST_DONE();
// }

// static void testBtreeScan(void)
// {
//   testName = "test B+ tree scan operations";
//   BTreeHandle *tree = NULL;
//   BT_ScanHandle *scan = NULL;
//   Value **keys;
//   char *stringKeys[] = {"i22", "i11", "i33", "i5"}; // 无序插入
//   int numKeys = 4;
//   RID rids[] = {{4,1}, {4,2}, {4,3}, {4,4}};
//   RID scanRid;
//   int count = 0;

//   // 预期扫描顺序（按键值升序：i5 < i11 < i22 < i33）
//   RID expectedRids[] = {{4,4}, {4,2}, {4,1}, {4,3}};

//   keys = createValues(stringKeys, numKeys);

//   // 初始化并插入数据
//   TEST_CHECK(initIndexManager(NULL));
//   TEST_CHECK(createBtree("scan_test_idx", DT_INT, 2));
//   TEST_CHECK(openBtree(&tree, "scan_test_idx"));
//   for (int i = 0; i < numKeys; i++) {
//       TEST_CHECK(insertKey(tree, keys[i], rids[i]));
//   }

//   // 打开扫描并验证顺序
//   TEST_CHECK(openTreeScan(tree, &scan));
//   while (nextEntry(scan, &scanRid) == RC_OK) {
//       ASSERT_EQUALS_RID(scanRid, expectedRids[count], "scan order check");
//       count++;
//   }
//   // 验证扫描条目数
//   ASSERT_EQUALS_INT(count, numKeys, "scan total entries");
//   // 验证扫描结束返回正确错误码
//   int rc = nextEntry(scan, &scanRid);
//   ASSERT_EQUALS_INT(rc, RC_IM_NO_MORE_ENTRIES, "scan end check");

//   // 清理
//   TEST_CHECK(closeTreeScan(scan));
//   TEST_CHECK(closeBtree(tree));
//   TEST_CHECK(deleteBtree("scan_test_idx"));
//   TEST_CHECK(shutdownIndexManager());
//   freeValues(keys, numKeys);

//   TEST_DONE();
// }

// static void testBtreeEdgeCases(void)
// {
//   testName = "test B+ tree split and merge";
//   BTreeHandle *tree = NULL;
//   Value **keys;
//   char *stringKeys[10]; // 生成10个有序键
//   RID rids[10];
//   int nodeCount, i;

//   // 生成键（i1到i10）和RID
//   for (i = 0; i < 10; i++) {
//       char buf[10];
//       sprintf(buf, "i%d", i+1);
//       stringKeys[i] = buf;
//       rids[i].page = 5;
//       rids[i].slot = i+1;
//   }
//   keys = createValues(stringKeys, 10);

//   // 初始化（阶数为3，满节点时触发分裂）
//   TEST_CHECK(initIndexManager(NULL));
//   TEST_CHECK(createBtree("edge_test_idx", DT_INT, 3));
//   TEST_CHECK(openBtree(&tree, "edge_test_idx"));

//   // 插入10个键（触发多次分裂）
//   for (i = 0; i < 10; i++) {
//       TEST_CHECK(insertKey(tree, keys[i], rids[i]));
//   }
//   TEST_CHECK(getNumNodes(tree, &nodeCount));
//   ASSERT_TRUE(nodeCount > 1, "split should create multiple nodes");

//   // 删除8个键（触发合并）
//   for (i = 2; i < 10; i++) {
//       TEST_CHECK(deleteKey(tree, keys[i]));
//   }
//   TEST_CHECK(getNumNodes(tree, &nodeCount));
//   ASSERT_EQUALS_INT(nodeCount, 1, "merge should reduce to 1 node");

//   // 清理
//   TEST_CHECK(closeBtree(tree));
//   TEST_CHECK(deleteBtree("edge_test_idx"));
//   TEST_CHECK(shutdownIndexManager());
//   freeValues(keys, 10);

//   TEST_DONE();
// }

/*=============================official test methods========================*/
// ************************************************************ 
void
testInsertAndFind (void)
{
  RID insert[] = { 
    {1,1},
    {2,3},
    {1,2},
    {3,5},
    {4,4},
    {3,2}, 
  };
  int numInserts = 6;
  Value **keys;
  char *stringKeys[] = {
    "i1",
    "i11",
    "i13",
    "i17",
    "i23",
    "i52"
  };
  testName = "test b-tree inserting and search";
  int i, testint;
  BTreeHandle *tree = NULL;
  
  keys = createValues(stringKeys, numInserts);

  // init
  TEST_CHECK(initIndexManager(NULL));
  // create test b-tree
  TEST_CHECK(createBtree("testidx", DT_INT, 2));
  // open test b-tree
  TEST_CHECK(openBtree(&tree, "testidx"));

  // insert keys
  for(i = 0; i < numInserts; i++)
    TEST_CHECK(insertKey(tree, keys[i], insert[i]));

  // check index stats
  TEST_CHECK(getNumNodes(tree, &testint));
  ASSERT_EQUALS_INT(testint,4, "number of nodes in btree");
  TEST_CHECK(getNumEntries(tree, &testint));
  ASSERT_EQUALS_INT(testint, numInserts, "number of entries in btree");

  // search for keys
  for(i = 0; i < 1000; i++)
    {
      int pos = rand() % numInserts;
      RID rid;
      Value *key = keys[pos];

      TEST_CHECK(findKey(tree, key, &rid));
      ASSERT_EQUALS_RID(insert[pos], rid, "did we find the correct RID?");
    }

  // cleanup
  TEST_CHECK(closeBtree(tree));
  TEST_CHECK(deleteBtree("testidx"));
  TEST_CHECK(shutdownIndexManager());
  freeValues(keys, numInserts);

  TEST_DONE();
}

// ************************************************************ 
void
testDelete (void)
{
  RID insert[] = { 
    {1,1},
    {2,3},
    {1,2},
    {3,5},
    {4,4},
    {3,2}, 
  };
  int numInserts = 6;
  Value **keys;
  char *stringKeys[] = {
    "i1",
    "i11",
    "i13",
    "i17",
    "i23",
    "i52"
  };
  testName = "test b-tree inserting and search";
  int i, iter;
  BTreeHandle *tree = NULL;
  int numDeletes = 3;
  bool *deletes = (bool *) malloc(numInserts * sizeof(bool));
  
  keys = createValues(stringKeys, numInserts);

  // init
  TEST_CHECK(initIndexManager(NULL));

  // create test b-tree and randomly remove entries
  for(iter = 0; iter < 50; iter++)
    {
      // randomly select entries for deletion (may select the same on twice)
      for(i = 0; i < numInserts; i++)
	deletes[i] = FALSE;
      for(i = 0; i < numDeletes; i++)
	deletes[rand() % numInserts] = TRUE;

      // init B-tree
      TEST_CHECK(createBtree("testidx", DT_INT, 2));
      TEST_CHECK(openBtree(&tree, "testidx"));

      // insert keys
      for(i = 0; i < numInserts; i++)
	TEST_CHECK(insertKey(tree, keys[i], insert[i]));
      
      // delete entries
      for(i = 0; i < numInserts; i++)
	{
	  if (deletes[i])
	    TEST_CHECK(deleteKey(tree, keys[i]));
	}

      // search for keys
      for(i = 0; i < 1000; i++)
	{
	  int pos = rand() % numInserts;
	  RID rid;
	  Value *key = keys[pos];
	  
	  if (deletes[pos])
	    {
	      int rc = findKey(tree, key, &rid);
	      ASSERT_TRUE((rc == RC_IM_KEY_NOT_FOUND), "entry was deleted, should not find it");
	    }
	  else
	    {
	      TEST_CHECK(findKey(tree, key, &rid));
	      ASSERT_EQUALS_RID(insert[pos], rid, "did we find the correct RID?");
	    }
	}

      // cleanup
      TEST_CHECK(closeBtree(tree));
      TEST_CHECK(deleteBtree("testidx"));
    }

  TEST_CHECK(shutdownIndexManager());
  freeValues(keys, numInserts);
  free(deletes);

  TEST_DONE();
}

// ************************************************************ 
void
testIndexScan (void)
{
  RID insert[] = { 
    {1,1},
    {2,3},
    {1,2},
    {3,5},
    {4,4},
    {3,2}, 
  };
  int numInserts = 6;
  Value **keys;
  char *stringKeys[] = {
    "i1",
    "i11",
    "i13",
    "i17",
    "i23",
    "i52"
  };
  
  testName = "random insertion order and scan";
  int i, testint, iter, rc;
  BTreeHandle *tree = NULL;
  BT_ScanHandle *sc = NULL;
  RID rid;
  
  keys = createValues(stringKeys, numInserts);

  // init
  TEST_CHECK(initIndexManager(NULL));

  for(iter = 0; iter < 50; iter++)
    {
      int *permute;

      // create permutation
      permute = createPermutation(numInserts);

      // create B-tree
      TEST_CHECK(createBtree("testidx", DT_INT, 2));
      TEST_CHECK(openBtree(&tree, "testidx"));

      // insert keys
      for(i = 0; i < numInserts; i++)
	TEST_CHECK(insertKey(tree, keys[permute[i]], insert[permute[i]]));

      // check index stats
      TEST_CHECK(getNumEntries(tree, &testint));
      ASSERT_EQUALS_INT(testint, numInserts, "number of entries in btree");
      
      // execute scan, we should see tuples in sort order
      openTreeScan(tree, &sc);
      i = 0;
      while((rc = nextEntry(sc, &rid)) == RC_OK)
	{
	  RID expRid = insert[i++];
	  ASSERT_EQUALS_RID(expRid, rid, "did we find the correct RID?");
	}
      ASSERT_EQUALS_INT(RC_IM_NO_MORE_ENTRIES, rc, "no error returned by scan");
      ASSERT_EQUALS_INT(numInserts, i, "have seen all entries");
      closeTreeScan(sc);

      // cleanup
      TEST_CHECK(closeBtree(tree));
      TEST_CHECK(deleteBtree("testidx"));
      free(permute);
    }

  TEST_CHECK(shutdownIndexManager());
  freeValues(keys, numInserts);

  TEST_DONE();
}

// ************************************************************ 
int *
createPermutation (int size)
{
  int *result = (int *) malloc(size * sizeof(int));
  int i;

  for(i = 0; i < size; result[i] = i, i++);

  for(i = 0; i < 100; i++)
    {
      int l, r, temp;
      l = rand() % size;
      r = rand() % size;
      temp = result[l];
      result[l] = result[r];
      result[r] = temp;
    }
  
  return result;
}

// ************************************************************ 
Value **
createValues (char **stringVals, int size)
{
  Value **result = (Value **) malloc(sizeof(Value *) * size);
  int i;
  
  for(i = 0; i < size; i++)
    result[i] = stringToValue(stringVals[i]);

  return result;
}

// ************************************************************ 
void
freeValues (Value **vals, int size)
{
  while(--size >= 0)
    free(vals[size]);
  free(vals);
}

