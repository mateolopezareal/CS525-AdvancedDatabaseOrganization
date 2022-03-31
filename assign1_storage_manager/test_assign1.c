#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */
static void testCreateOpenClose(void);
static void test_scenario(void);
static void testSinglePageContent(void);

/* main function running all tests */
int
main(void) {
    printf("==============================\n");
    printf("TEST ASSIGN 1_1 : TEACHER TEST\n");
    printf("==============================\n\n");

    testName = "";

    initStorageManager();
    test_scenario();
    testCreateOpenClose();
    testSinglePageContent();

    return 0;
}


/* check a return code. If it is not RC_OK then output a message, error description, and exit */
/* Try to create, open, and close a page file */
void
testCreateOpenClose(void) {
    SM_FileHandle fh;

    testName = "test create open and close methods";

    TEST_CHECK(createPageFile(TESTPF));

    TEST_CHECK(openPageFile(TESTPF, &fh));
    ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "filename correct");
    ASSERT_TRUE((fh.totalNumPages == 1), "expect 1 page in new file");
    ASSERT_TRUE((fh.curPagePos == 0), "freshly opened file's page position should be 0");

    TEST_CHECK(closePageFile (&fh));
    TEST_CHECK(destroyPageFile (TESTPF));

    //after destruction trying to open the file should cause an error
    ASSERT_TRUE((openPageFile(TESTPF, &fh) != RC_OK), "opening non-existing file should return an error.");
    TEST_DONE();
}

//Test scenario in order to try some methods
void test_scenario(void){
    SM_FileHandle f;
    SM_PageHandle p;
    
    testName = "test scenario";
    
    p = (SM_PageHandle) malloc(PAGE_SIZE);
    
    //file created and opened
    TEST_CHECK(createPageFile("assignment1.txt"));
    TEST_CHECK(openPageFile("assignment1.txt", &f));
    printf("created and opened file\n");
    
    //try methods
    TEST_CHECK(ensureCapacity(4, &f));
    ASSERT_EQUALS_INT(4, f.totalNumPages, "file should have 4 pages");

    TEST_CHECK(readNextBlock(&f,p));
    for(int i=0;i<PAGE_SIZE;i++){
        p[i] = 'a';
    }

    TEST_CHECK(writeBlock(2, &f, p));
    
    //CLOSE FILE
    TEST_CHECK(closePageFile(&f));
    free(p);
    TEST_DONE();   
}

/* Try to create, open, and close a page file */
void
testSinglePageContent(void) {
    SM_FileHandle fh;
    SM_PageHandle ph;
    int i;

    testName = "test single page content";
    
    

    ph = (SM_PageHandle) malloc(PAGE_SIZE);

    // create a new page file
    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh));
    printf("created and opened file\n");

    // read first page into handle
    TEST_CHECK(readFirstBlock(&fh, ph));
    printf("read first block ok\n");
    // the page should be empty (zero bytes)
    for (i = 8; i < PAGE_SIZE; i++){
        ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
    }
    printf("first block was empty\n");

    ASSERT_EQUALS_INT(1, getBlockPos(&fh), "excepted getblockposition to be 1");

    // change ph to be a string and write that one to disk
    for (i = 0; i < PAGE_SIZE; i++)
        ph[i] = (i % 10) + '0';
    TEST_CHECK(writeBlock(0, &fh, ph));
    printf("writing first block\n");

    // read back the page containing the string and check that it is correct
    TEST_CHECK(readFirstBlock(&fh, ph));
    for (i = 0; i < PAGE_SIZE; i++)
        ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
    printf("reading first block\n");

    printf("Ensuring file has 5 pages");
    TEST_CHECK(ensureCapacity(5, &fh));
    ASSERT_EQUALS_INT(fh.totalNumPages, 5, "file should have 5 pages");
    ASSERT_EQUALS_INT(fh.curPagePos, 1, "curPagePos should be 1 even if we added pages");

    printf("Writing in fourth block");
    for (i = 0; i < PAGE_SIZE/2; i++)
        ph[i] = 'a';
    for (i = PAGE_SIZE/2; i < PAGE_SIZE; i++)
        ph[i] = 'b';
    TEST_CHECK(writeBlock(3, &fh, ph));

    TEST_CHECK(readBlock(3, &fh, ph));
    for (i = 0; i < PAGE_SIZE/2; i++)
        ASSERT_TRUE((ph[i] == 'a'), "character in page read from disk is the one we expected.");
    for (i = PAGE_SIZE/2; i < PAGE_SIZE; i++)
        ASSERT_TRUE((ph[i] == 'b'), "character in page read from disk is the one we expected.");

    printf("%lu\n", sizeof (int));
    TEST_CHECK(writeBlock(0, &fh, ph));
    // destroy new page file
    TEST_CHECK(closePageFile(&fh));
    TEST_CHECK(openPageFile(TESTPF, &fh));
    ASSERT_EQUALS_INT(5, fh.totalNumPages, "should have 5 pages");
    TEST_CHECK(closePageFile(&fh));

    TEST_CHECK(destroyPageFile(TESTPF));
    free(ph);
    TEST_DONE();
}
