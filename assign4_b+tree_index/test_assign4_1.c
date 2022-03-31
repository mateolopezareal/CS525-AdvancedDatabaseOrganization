#include <stdlib.h>

#include "dberror.h"
#include "expr.h"
#include "btree_mgr.h"
#include "tables.h"
#include "test_helper.h"
#include "buffer_mgr.h"

#define ASSERT_EQUALS_RID(_l, _r, message)                \
  do {                                    \
    ASSERT_TRUE((_l).page == (_r).page && (_l).slot == (_r).slot, message); \
  } while(0)

// test methods
static void testDraft(void);

static void testInsertAndFind(void);

static void testDelete(void);

static void testIndexScan(void);

// helper methods
static Value **createValues(char **stringVals, int size);

static void freeValues(Value **vals, int size);

static int *createPermutation(int size);

// test name
char *testName;

// main method
int
main(void) {
    testName = "";

    testDraft();
    testInsertAndFind();
//  testDelete();
//  testIndexScan();

    return 0;
}

void testDraft() {
    BTreeHandle *tree = NULL;
    TEST_CHECK(initIndexManager(NULL));
    TEST_CHECK(createBtree("testdraft", DT_INT, 2));
    TEST_CHECK(openBtree(&tree, "testdraft"));
    BM_BufferPool * bufferPool = MAKE_POOL();
    BM_PageHandle * pageHandle = MAKE_PAGE_HANDLE();
    initBufferPool(bufferPool, "testdraft", 4, RS_LRU, NULL);

    pinPage(bufferPool, pageHandle, 1); // first node
    * (char *) pageHandle->data = 'N'; //type
    pageHandle->data ++;
    * (int *) pageHandle->data = 1; // num of keys
    pageHandle->data += sizeof (int);
    * (int *) pageHandle->data = 2; //pointer to node 2 in page 2
    pageHandle->data += sizeof (int);
    * (int *) pageHandle->data = -1; //slot we don't care
    pageHandle->data += sizeof (int);
    * (int *) pageHandle->data = 5; // key
    pageHandle->data += sizeof (int);
    * (int *) pageHandle->data = 3; // pointer to node 3 in page 3
    pageHandle->data += sizeof (int);
    * (int *) pageHandle->data = -1; // slot we don't care pointing to a node not record
    pageHandle->data += sizeof (int);
    markDirty(bufferPool, pageHandle);
    unpinPage(bufferPool, pageHandle);

    pinPage(bufferPool, pageHandle, 2); // second node
    * (char *) pageHandle->data = 'L';
    pageHandle->data ++;
    * (int *) pageHandle->data = 1;
    pageHandle->data += sizeof (int);
    * (int *) pageHandle->data = 5; // pointer to record in page 5 (arbitrary value)
    pageHandle->data += sizeof (int);
    * (int *) pageHandle->data = 0; //slot
    pageHandle->data += sizeof (int);
    * (int *) pageHandle->data = 2; // key
    pageHandle->data += sizeof (int);
    * (int *) pageHandle->data = 3; // pointer to sibling
    pageHandle->data += sizeof (int);
    * (int *) pageHandle->data = -1; //slot we don't care pointing to a node not record
    pageHandle->data += sizeof (int);
    markDirty(bufferPool, pageHandle);
    unpinPage(bufferPool, pageHandle);

    pinPage(bufferPool, pageHandle, 3); // third node
    * (char *) pageHandle->data = 'L';
    pageHandle->data ++;
    * (int *) pageHandle->data = 1;
    pageHandle->data += sizeof (int);
    * (int *) pageHandle->data = 2; // pointer to record in page 2 (arbitrary value)
    pageHandle->data += sizeof (int);
    * (int *) pageHandle->data = 0; //slot
    pageHandle->data += sizeof (int);
    * (int *) pageHandle->data = 7; // key
    pageHandle->data += sizeof (int);
    * (int *) pageHandle->data = 3; // pointer to sibling (it doesnt have any but we don't care)
    pageHandle->data += sizeof (int);
    * (int *) pageHandle->data = -1; // slot we don't care pointing to a node not record
    pageHandle->data += sizeof (int);
    markDirty(bufferPool, pageHandle);
    unpinPage(bufferPool, pageHandle);
    forceFlushPool(bufferPool);

    Value * value;
    RID rid;
    MAKE_VALUE(value, DT_INT, 7);
    TEST_CHECK(findKey(tree, value, &rid));
    deleteBtree("testdraft");
    printf("found RID page %d slot %d\n", rid.page, rid.slot);
}

// ************************************************************ 
void
testInsertAndFind(void) {
    RID insert[] = {
            {1, 1},
            {2, 3},
            {1, 2},
            {3, 5},
            {4, 4},
            {3, 2},
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
    TEST_CHECK(createBtree("testidx", DT_INT, 2));
    TEST_CHECK(openBtree(&tree, "testidx"));
/*
  // insert keys
  for(i = 0; i < numInserts; i++)
    TEST_CHECK(insertKey(tree, keys[i], insert[i]));

  // check index stats
  TEST_CHECK(getNumNodes(tree, &testint));
  ASSERT_EQUALS_INT(4,testint, "number of nodes in btree"); // Fixed Summer 2021 - Old: ASSERT_EQUALS_INT(testint,4, "number of nodes in btree");
  TEST_CHECK(getNumEntries(tree, &testint));
  ASSERT_EQUALS_INT(numInserts,testint, "number of entries in btree");// Fixed Summer 2021 - Old: ASSERT_EQUALS_INT(testint, numInserts, "number of entries in btree");

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
  */
    TEST_CHECK(closeBtree(tree));
    TEST_CHECK(deleteBtree("testidx"));
//  TEST_CHECK(shutdownIndexManager());
//  freeValues(keys, numInserts);

    TEST_DONE();
}

// ************************************************************ 
void
testDelete(void) {
    RID insert[] = {
            {1, 1},
            {2, 3},
            {1, 2},
            {3, 5},
            {4, 4},
            {3, 2},
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
    for (iter = 0; iter < 50; iter++) {
        // randomly select entries for deletion (may select the same on twice)
        for (i = 0; i < numInserts; i++)
            deletes[i] = FALSE;
        for (i = 0; i < numDeletes; i++)
            deletes[rand() % numInserts] = TRUE;

        // init B-tree
        TEST_CHECK(createBtree("testidx", DT_INT, 2));
        TEST_CHECK(openBtree(&tree, "testidx"));

        // insert keys
        for (i = 0; i < numInserts; i++)
            TEST_CHECK(insertKey(tree, keys[i], insert[i]));

        // delete entries
        for (i = 0; i < numInserts; i++) {
            if (deletes[i])
                TEST_CHECK(deleteKey(tree, keys[i]));
        }

        // search for keys
        for (i = 0; i < 1000; i++) {
            int pos = rand() % numInserts;
            RID rid;
            Value *key = keys[pos];

            if (deletes[pos]) {
                int rc = findKey(tree, key, &rid);
                ASSERT_TRUE((rc == RC_IM_KEY_NOT_FOUND), "entry was deleted, should not find it");
            } else {
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
testIndexScan(void) {
    RID insert[] = {
            {1, 1},
            {2, 3},
            {1, 2},
            {3, 5},
            {4, 4},
            {3, 2},
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

    for (iter = 0; iter < 50; iter++) {
        int *permute;

        // create permutation
        permute = createPermutation(numInserts);

        // create B-tree
        TEST_CHECK(createBtree("testidx", DT_INT, 2));
        TEST_CHECK(openBtree(&tree, "testidx"));

        // insert keys
        for (i = 0; i < numInserts; i++)
            TEST_CHECK(insertKey(tree, keys[permute[i]], insert[permute[i]]));

        // check index stats
        TEST_CHECK(getNumEntries(tree, &testint));
        ASSERT_EQUALS_INT(numInserts, testint,
                          "number of entries in btree"); // Fixed: Summer 2021 - Old: ASSERT_EQUALS_INT(testint, numInserts, "number of entries in btree");

        // execute scan, we should see tuples in sort order
        openTreeScan(tree, &sc);
        i = 0;
        while ((rc = nextEntry(sc, &rid)) == RC_OK) {
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
createPermutation(int size) {
    int *result = (int *) malloc(size * sizeof(int));
    int i;

    for (i = 0; i < size; result[i] = i, i++);

    for (i = 0; i < 100; i++) {
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
createValues(char **stringVals, int size) {
    Value **result = (Value **) malloc(sizeof(Value *) * size);
    int i;

    for (i = 0; i < size; i++)
        result[i] = stringToValue(stringVals[i]);

    return result;
}

// ************************************************************ 
void
freeValues(Value **vals, int size) {
    while (--size >= 0)
        free(vals[size]);
    free(vals);
}

