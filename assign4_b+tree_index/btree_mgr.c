#include "btree_mgr.h"
#include "dberror.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <math.h>

typedef struct BT_FreePage BT_FreePage;

typedef struct BT_FreePage {
    int page;
    BT_FreePage *next;
} BT_freePage;

typedef struct BT_FreePagesQueue {
    BT_FreePage *first;
    BT_FreePage *last;
    int numberOfFreePages;
} BT_FreePagesQueue;

typedef struct IndexMgr {
    int numNodes;
    int numEntries;
    int maxKeysPerNode;
    int minKeysPerNode;
    int minKeysPerLeaf;
    BM_BufferPool *bufferPool;
    BT_FreePagesQueue *freePagesQueue;
} IndexMgr;

// a node of the tree /!\ ASSUMING KEYS ARE ONLY INT
typedef struct Node {
    bool isLeaf; // true if Node is a leaf
    int * keys; // array containing the keys of the node
    RID * pointers; // array containing the pointers either to other nodes or to record
    int numKeyInNode; // number of keys in the node
} Node;

typedef struct Node2 {
    int nodeType; // 0 = root, 1 = node, 2 = leaf
    int * keys; // array containing the keys of the node
    Node * father; // pointer to the father of the node
    Node ** children; // array of pointers to its child. NULL if node is a leaf
    int numKeyInNode; // number of keys in the node
    RID * rids; // array of records in the node. NULL if node is not a leaf
} Node2;


void printBtreeHandle(BTreeHandle * tree){
    printf("#############################\n");
    printf("[DEBUG  PRINTING BTreeHandle]\n");

    IndexMgr * indexMgr = (IndexMgr *) tree->mgmtData;
    printf("name of file : %s\n", tree->idxId);
    printf("key type : %d\n", tree->keyType);
    printf("num of nodes : %d\n", indexMgr->numNodes);
    printf("num of entries : %d\n", indexMgr->numEntries);
    printf("maxKeysPerNode : %d\n", indexMgr->maxKeysPerNode);
    printf("minKeysPerNode : %d\n", indexMgr->minKeysPerNode);
    printf("minKeysPerLeaf : %d\n", indexMgr->minKeysPerLeaf);
    printf("#############################\n");
}

void printNode(Node * node){
    printf("#############################\n");
    printf("[DEBUG  PRINTING NODE]\n");
    printf("Is node a leaf : %d\n", node->isLeaf);
    printf("Number of keys in node : %d\n", node->numKeyInNode);
    printf("keys : [ ");
    for (int i = 0; i < node->numKeyInNode; i++){
        printf("%d, ", node->keys[i]);
    }
    printf("]\n");

    printf("pointers : [ ");
    for (int i = 0; i < node->numKeyInNode + 1; i++){
        printf("p%d s%d, ", node->pointers[i].page, node->pointers[i].slot);
    }
    printf("]\n");
}

BT_FreePagesQueue* initFreePagesQueue() {
    BT_FreePagesQueue* queue = malloc(sizeof(BT_FreePagesQueue));
    queue->first = NULL;
    queue->last = NULL;
    queue->numberOfFreePages = 0;
    return queue;
}

void freePagesQueue(BT_FreePagesQueue* queue) {
    if (queue->numberOfFreePages != 0) {
        while (queue->first->next != NULL) {
            BT_FreePage* next = queue->first->next;
            free(queue->first);
            queue->first = next;
            queue->numberOfFreePages--;
        }
    }
    if (queue->numberOfFreePages == 1) {
        free(queue->first);
    }
    free(queue);
}

void insertInFreePagesQueue(BT_FreePagesQueue* queue, int pageNumber) {
    BT_FreePage* freePage = malloc(sizeof(BT_FreePage));
    freePage->page = pageNumber;
    freePage->next = (BT_FreePage*) NULL;
    if (queue->last != NULL) {
        queue->last->next = freePage;
    }
    if (queue->first == NULL) {
        queue->first = freePage;
    }
    queue->last = freePage;
    queue->numberOfFreePages++;
}

int getFirstFreePage(BT_FreePagesQueue* queue) {
    BT_FreePage* freePage;
    if (queue->numberOfFreePages == 0) {
        return -1;
    }

    int pageNum = queue->first->page;
    freePage = queue->first;
    queue->first = queue->first->next;
    free(freePage);
    queue->numberOfFreePages--;
    return pageNum;
}

// init and shutdown index manager
extern RC initIndexManager(void *mgmtData) {
    initStorageManager();
    return RC_OK;
}

extern RC shutdownIndexManager() {
    return RC_OK;
}

// create, destroy, open, and close a btree index
extern RC createBtree(char *idxId, DataType keyType, int n) {
    if (createPageFile(idxId) != RC_OK) {
        return RC_WRITE_FAILED;
    }

    BM_BufferPool *bufferPool = MAKE_POOL();
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE();

    //We decided 10 pages for the buffer and LRU strategy
    if (initBufferPool(bufferPool, idxId, 1, RS_LRU, NULL) != RC_OK) {
        return RC_FILE_NOT_FOUND;
    }

    // pinning page 0, will contain metadata
    if (pinPage(bufferPool, pageHandle, 0) != RC_OK) {
        return RC_WRITE_FAILED;
    }

    // filling initial metadata [numberOfNodes, numberOfEntries, keyType, maxNumOfKeyPerNode]
    *(int *) pageHandle->data = 0;
    pageHandle->data += sizeof(int);

    *(int *) pageHandle->data = 0;
    pageHandle->data += sizeof(int);

    *(DataType *) pageHandle->data = keyType;
    pageHandle->data += sizeof(DataType);

    *(int *) pageHandle->data = n;
    pageHandle->data += sizeof(int);

    if (unpinPage(bufferPool, pageHandle) != RC_OK) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    if (forcePage(bufferPool, pageHandle) != RC_OK) {
        return RC_WRITE_FAILED;
    }

    if (shutdownBufferPool(bufferPool) != RC_OK) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    free(pageHandle);
    free(bufferPool);

    return RC_OK;
}

extern RC openBtree(BTreeHandle **tree, char *idxId) {
    (* tree) = malloc(sizeof (BTreeHandle));
    (*tree)->mgmtData = (IndexMgr *) malloc(sizeof (IndexMgr));
    IndexMgr * indexMgr = (*tree)->mgmtData;

    indexMgr->freePagesQueue = initFreePagesQueue();
    indexMgr->bufferPool = MAKE_POOL();
    (*tree)->idxId = idxId;
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE();


    if (initBufferPool(indexMgr->bufferPool, idxId, 10, RS_LRU, NULL) != RC_OK) {
        return RC_FILE_NOT_FOUND;
    }

    if (pinPage(indexMgr->bufferPool, pageHandle, 0) != RC_OK) {
        return RC_WRITE_FAILED;
    }

    // filling the indexMgr with the metadata from the index pageFile
    // [numberOfNodes, numberOfEntries, keyType, maxNumOfKeyPerNode, freePageNumber, freePageNumber...]
    indexMgr->numNodes = *(int *) pageHandle->data;
    pageHandle->data += sizeof(int);

    indexMgr->numEntries = *(int *) pageHandle->data;
    pageHandle->data += sizeof(int);

    (*tree)->keyType = *(DataType *) pageHandle->data;
    pageHandle->data += sizeof(DataType);

    indexMgr->maxKeysPerNode = *(int *) pageHandle->data;
    pageHandle->data += sizeof(int);

    double frac = ((float) indexMgr->maxKeysPerNode + 1.0)/2.0;
    indexMgr->minKeysPerNode = floor(frac);
    indexMgr->minKeysPerLeaf = ceil(frac - 1);

    int freePageNumber;
    while (*pageHandle->data != '\0') {
        freePageNumber = *(int *) pageHandle->data;

        insertInFreePagesQueue(indexMgr->freePagesQueue, freePageNumber);

        pageHandle->data += sizeof(int);
    }
    if (unpinPage(indexMgr->bufferPool, pageHandle) != RC_OK) {
        return RC_READ_NON_EXISTING_PAGE;
    }

//    printBtreeHandle(*(tree));

    free(pageHandle);
    return RC_OK;
}

extern RC closeBtree(BTreeHandle *tree) {
    IndexMgr * indexMgr = (IndexMgr *) tree->mgmtData;
    BM_BufferPool *bufferPool = indexMgr->bufferPool;
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE();

    if (pinPage(bufferPool, pageHandle, 0) != RC_OK) {
        return RC_WRITE_FAILED;
    }

    // putting info about the index in the file
    // [numberOfNodes, numberOfEntries, keyType, maxNumOfKeyPerNode, freePageNumber, freePageNumber...]

    *(int *) pageHandle->data = indexMgr->numNodes;
    pageHandle->data += sizeof(int);

    *(int *) pageHandle->data = indexMgr->numEntries;
    pageHandle->data += sizeof(int);

    *(DataType *) pageHandle->data = tree->keyType;
    pageHandle->data += sizeof(DataType);

    *(int *) pageHandle->data = indexMgr->maxKeysPerNode;
    pageHandle->data += sizeof(int);

    BT_FreePagesQueue *queue = indexMgr->freePagesQueue;
    BT_FreePage *freePage = queue->first;
    while (freePage != NULL) {
        *(int *) pageHandle->data = freePage->page;
        pageHandle->data += sizeof(int);
        freePage = freePage->next;
    }

    if (markDirty(bufferPool, pageHandle) != RC_OK) {
        return RC_WRITE_FAILED;
    }

    if (unpinPage(bufferPool, pageHandle) != RC_OK) {
        return RC_WRITE_FAILED;
    }

    if (forceFlushPool(bufferPool) != RC_OK) {
        return RC_WRITE_FAILED;
    }

    freePagesQueue(indexMgr->freePagesQueue);

    free(pageHandle);

    RC r = shutdownBufferPool(bufferPool);

    free(bufferPool);

    return r;
}

extern RC deleteBtree(char *idxId) {
    return destroyPageFile(idxId);
}

// access information about a b-tree
extern RC getNumNodes(BTreeHandle *tree, int *result) {
    IndexMgr * indexMgr = (IndexMgr *) tree->mgmtData;
    *result = indexMgr->numNodes;
    return RC_OK;
}

extern RC getNumEntries(BTreeHandle *tree, int *result) {
    IndexMgr * indexMgr = (IndexMgr *) tree->mgmtData;
    *result = indexMgr->numEntries;
    return RC_OK;
}

extern RC getKeyType(BTreeHandle *tree, DataType *result) {
    *result = (*tree).keyType;
    return RC_OK;
}

// index access

// fill a node by looking at the page file it is in
// param: node should be allocated
void nodeFromPageHandle(BM_PageHandle * pageHandle, Node * node){
    char nodeType = * pageHandle->data;
    pageHandle->data += sizeof (char);

    if (nodeType == 'L'){
        node->isLeaf = TRUE;
    } // node is a leaf
    else if(nodeType == 'N'){
        node->isLeaf = FALSE;
    } // node is simple node
    else {
        printf("ERROR READING NODE TYPE. FOUND %d. EXITING...", nodeType);
        exit(1);
    }

    node->numKeyInNode = * pageHandle->data;
    pageHandle->data += sizeof (int);
    node->keys = malloc(sizeof (int)*node->numKeyInNode);
    node->pointers = malloc(sizeof (RID)*(node->numKeyInNode+1)); // +1 because there are n + 1 pointer in a node

    for (int i = 0; i < node->numKeyInNode; i++){
        node->pointers[i].page = *pageHandle->data;
        pageHandle->data += sizeof (int);

        if (node->isLeaf)
            node->pointers[i].slot = *pageHandle->data;
        else
            node->pointers[i].slot = -1; // no slot only need page. -1 to be sure we don't use it whatsoever
        pageHandle->data += sizeof (int);

        node->keys[i] = * pageHandle->data;
        pageHandle->data += sizeof (int);
    }

    // the last pointer
    node->pointers[node->numKeyInNode].page = *pageHandle->data;
    pageHandle->data += sizeof (int);

    if (node->isLeaf)
        node->pointers[node->numKeyInNode].slot = *pageHandle->data;
    else
        node->pointers[node->numKeyInNode].slot = -1; // no slot only need page. -1 to be sure we don't use it whatsoever
    pageHandle->data += sizeof (int);
}

void recursivelyFindNode(BTreeHandle * tree, Value * key, Node * node, int pageNumber){
    IndexMgr * indexMgr = (IndexMgr *) tree->mgmtData;
    BM_PageHandle * pageHandle = MAKE_PAGE_HANDLE();
    if (pinPage(indexMgr->bufferPool, pageHandle, pageNumber) != RC_OK){
        return NULL;
    }

    nodeFromPageHandle(pageHandle, node);

    if (node->isLeaf == TRUE){ // we are the bottom of the tree
        return;
    }

    for (int i = 0; i < node->numKeyInNode; i++){ // finding the next node to explore
        if (key->v.intV < node->keys[i]){
            int pageToGo = node->pointers[i].page;
            free(node->pointers); // is going to be re-allocated by nodeFromPageHandle
            free(node->keys); // is going to be re-allocated by nodeFromPageHandle
            return recursivelyFindNode(tree, key, node, pageToGo);
        }
    }

    // key is bigger than the biggest key in the node. We go to the page pointed by the last pointer
    int pageToGo = node->pointers[node->numKeyInNode].page;
    free(node->pointers); // is going to be re-allocated by nodeFromPageHandle
    free(node->keys); // is going to be re-allocated by nodeFromPageHandle
    return recursivelyFindNode(tree, key, node, pageToGo);
}


// will return the node where the key SHOULD BE, no guaranty the key is effectively in this node
Node * findNode(BTreeHandle * tree, Value *key){
    Node * node = malloc(sizeof (Node));

    recursivelyFindNode(tree, key, node, 1);

    return node;
}

extern RC findKey(BTreeHandle *tree, Value *key, RID *result) {
    Node * node = findNode(tree, key);
    if (node != NULL){
        printf("searching key\n");
        for (int i = 0; i < node->numKeyInNode; i++){
            if (node->keys[i] == key->v.intV){
                result->page = node->pointers[i].page;
                result->slot = node->pointers[i].slot;
                return RC_OK;
            }
        }
    }
    return RC_WRITE_FAILED;
}

RC insertInNodeOdd(parent, key){
    checkIfPlace(parent)
    if place{
        insert(parent, key); // if enough place in the node we can just insert the key in it (keyInNode < maxKeyPerNode). Need to keep the list of key sorted !
        return
    }
    if not place{
        indexOfInsertion = where to insert the new key in the array of keys in the node;
        insertKeyInArray(key, indexOfInsertion);
        splitArrayEvenly(array, leftArrayOfKeys, rightArrayOfKeys);
        keyToGoUp = first key of the right array;
        newInsertedNode = createNode(rightArrayOfKeys);
        updateNode(node, leftArrayOfKeys);
        updateTheChild(newInsertedNode); // bc nodes knows their father so we need to update it
        return insertInNodeOdd(parent of parent, keyToGoUp)
    }
}

RC insertInNodeEven(parent, key){
    checkIfPlace(parent)
    if place{
        insert(parent, key); // if enough place in the node we can just insert the key in it (keyInNode < maxKeyPerNode). Need to keep the list of key sorted !
        return
    }
    if not place{
                indexOfInsertion = where to insert the new key in the array of keys in the node;
                insertKeyInArray(key, indexOfInsertion);
                keyToGoUp = we need to find the middle key of the array;

                splitArrayEvenly(array, leftArrayOfKeys, rightArrayOfKeys); // array is the keys array without the middle key
                newInsertedNode = createNode(rightArrayOfKeys);
                updateNode(node, leftArrayOfKeys);
                updateTheChild(newInsertedNode); // bc nodes knows their father so we need to update it
                return insertInNodeOdd(parent of parent, keyToGuUp)
        }
}

extern RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
    if treeIsEmpty(){
        createRoot(key, rid);
        return;
    }
    node = findNode(key); // node is a leaf
    checkIfKeyInNode(node); // if the key is already in the tree we have nothing to do

    checkIfPlaceIntTheNode(node); // if enough place in the node we can just insert the key in it (keyInNode < maxKeyPerNode). Need to keep the list of key sorted !

    if noPlaceInNode{
        checkIfOddOrEven; // to know how to split the node
        indexOfInsertion = where to insert the new key in the array of keys in the node;
        insertKeyInArray(key, indexOfInsertion);
        if odd{
            splitArrayEvenly(array, leftArrayOfKeys, rightArrayOfKeys);
            createNode(rightArrayOfKeys);
            updateNode(node, leftArrayOfKeys);
            keyToGoUp = first key of the right array;

            insertInNodeOdd(parent, key); // basically does the same thing as insertKey
        }
        if even{
            splitArrayOddly(array, leftArrayOfKeys, rightArrayOfKeys); // extra key goes to left sibling
            createNode(rightArrayOfKeys);
            updateNode(node, leftArrayOfKeys);
            keyToGoUp = first key of the right array;

            insertInNodeEven(parent, key); // basically does the same thing as insertKey
        }
    }
    return RC_WRITE_FAILED;
}

extern RC deleteKey(BTreeHandle *tree, Value *key) {
    return RC_WRITE_FAILED;
}

extern RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
    return RC_WRITE_FAILED;
}

extern RC nextEntry(BT_ScanHandle *handle, RID *result) {
    return RC_WRITE_FAILED;
}

extern RC closeTreeScan(BT_ScanHandle *handle) {
    return RC_WRITE_FAILED;
}

// debug and test functions
extern char *printTree(BTreeHandle *tree) {
    return NULL;
}