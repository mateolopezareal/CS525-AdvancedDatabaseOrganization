#include <stdio.h>
#include <stdlib.h>
#include "storage_mgr.h"
#include <unistd.h>
#include <errno.h>
#include <string.h>


/* manipulating page files */
extern void initStorageManager (void){
    if (access(".", W_OK) != 0){
        printf("Storage manager doesn't have write permission in this folder.\nExiting...\n");
        exit(RC_WRITE_FAILED);
    }
    printf("Storage manager initialized\n");
}

extern RC createPageFile (char *fileName){
    // opening the file in write mode
    FILE * file = fopen(fileName, "w+");
    if (file != NULL){
        /*
         * Creating 2 pages.
         * First one will be reserved for storing usefull data such as the total number of pages
         * Second one is the first "real" page of the file, in the sense of this is where the user data will be written
         *
         */
        int numberOfChar = 2*PAGE_SIZE/(sizeof (char));
        char * charArray = malloc(numberOfChar);
        for (int i = 0; i < numberOfChar; i++){
            charArray[i] = '\0';
        }
        int wrote = fwrite(charArray, sizeof(char), numberOfChar, file);
        if (wrote < numberOfChar){
            fclose(file);
            return RC_WRITE_FAILED;
        }
        /*
         * writing the number of page at the begining of the file
         * the number is 1 because the first page is reserved so it does not count as a page
         */
        fseek(file, 0L, SEEK_SET);
        if (fprintf(file, "%d", 1) != 1){
            return RC_WRITE_FAILED;
        }
        free(charArray);
        fclose(file);
        return RC_OK;
    }
    fclose(file);
    return RC_FILE_NOT_FOUND;
}

/*
closePageFile or destroyPageFile need to be called after this method to close the file and avoiding memory leaks
*/
extern RC openPageFile (char *fileName, SM_FileHandle * fHandle){
    FILE * file = fopen(fileName, "r+");
    if (file != NULL){
        // filling the file handle attributes
        fHandle -> fileName = fileName;
        fHandle -> curPagePos = 0;

        // save the pointer to the opened file in the file handle
        fHandle->mgmtInfo = file;

        // reading the number of pages in the file (stored at the beginning of the file, in the reserved page)
        if (fscanf(file, "%d", &(fHandle->totalNumPages)) != 1) {
            fclose(file);
            return RC_READ_FAILED;
        }

        return RC_OK;
    }
    return RC_FILE_NOT_FOUND;
}

extern RC closePageFile (SM_FileHandle *fHandle){
    FILE * file = fHandle->mgmtInfo;
    if (fclose(file) != 0) {
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}

extern RC destroyPageFile (char *fileName){
    if (remove(fileName) != 0){
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}

/* reading blocks from disc */
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    // checking if there is enough page in the file
    // -1 because index starts at 0
    if (pageNum > fHandle->totalNumPages - 1){
        return RC_READ_NON_EXISTING_PAGE;
    }

    // getting the pointer to the opened file
    FILE * file = fHandle->mgmtInfo;

    /*
     * Compute the position of the desired page start
     * We add the number of reserved pages to the desired page number to be sure not to write in the reserved pages
    */
    int numberOfReservedPages = 1;
    long startingOffset = (pageNum + numberOfReservedPages)*PAGE_SIZE;

    /*
     * Compute the size of the file, this is needed to be sure not to read after the end of the file
     * For example if the start of the last page + PAGESIZE > size of file. Normally should not happen.
     * But just to be sure.
    */
    fseek(file, 0L, SEEK_END);
    long maxOffset = ftell(file);
    rewind(file);

    // trying to move the cursor to the right position in the file
    if (fseek(file, startingOffset, SEEK_SET) == -1){
        // change to more accurate error
        return RC_SEEK_FAILED;
    }


    long desiredOffset = startingOffset + PAGE_SIZE;

    // taking the minimum of both
    long possibleOffset = (((maxOffset) <= (desiredOffset)) ? (maxOffset) : (desiredOffset));

    // computing how many chars to read
    int numberOfChar = (possibleOffset - startingOffset)/(sizeof (char));
    if (fread(memPage, sizeof (char), numberOfChar, file) != numberOfChar){
        return RC_READ_NON_EXISTING_PAGE;
    }
    fHandle->curPagePos = pageNum + 1;
    return RC_OK;
}

extern int getBlockPos (SM_FileHandle *fHandle){
    return fHandle->curPagePos;
}

extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(0, fHandle, memPage);
}

extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    int curBlockPos = getBlockPos(fHandle);
    return readBlock(curBlockPos - 1, fHandle, memPage);
}
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    int curBlockPos = getBlockPos(fHandle);
    return readBlock(curBlockPos, fHandle, memPage);
}

extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    int curBlockPos = getBlockPos(fHandle);
    return readBlock(curBlockPos+1, fHandle, memPage);
}

extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    int lastBlock = fHandle->totalNumPages;
    // -1 because index starts at 0
    return readBlock(lastBlock - 1, fHandle, memPage);
}

/* writing blocks to a page file */
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    // checking if there is enough page in the file
    // -1 because index starts at 0
    if (pageNum > fHandle->totalNumPages -1){
        return RC_WRITE_FAILED;
    }

    // getting the pointer to the opened file
    FILE * file = fHandle->mgmtInfo;

    /*
     * Compute the position of the desired page start
     * We add the number of reserved pages to the desired page number to be sure not to write in the reserved pages
    */
    int numberOfReservedPages = 1;
    long startingOffset = (pageNum + numberOfReservedPages)*PAGE_SIZE;
    fseek(file, startingOffset, SEEK_SET);

    int wrote = fwrite(memPage, sizeof (char), PAGE_SIZE/sizeof (char), file);

    if(wrote != PAGE_SIZE){
        return RC_WRITE_FAILED;
    }

    fHandle->curPagePos = pageNum + 1;
    return RC_OK;
}

extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    int curBlockPos = getBlockPos(fHandle);
    return writeBlock(curBlockPos, fHandle, memPage);
}

extern RC appendEmptyBlock (SM_FileHandle *fHandle){
    int curBlockPos = getBlockPos(fHandle);

    FILE * file = fHandle->mgmtInfo;
    fseek(file, 0L, SEEK_END);

    int numberOfChar = PAGE_SIZE/(sizeof (char));
    char * charArray = malloc(numberOfChar);

    for (int i = 0; i < numberOfChar; i++){
        charArray[i] = '\0';
    }

    int wrote = fwrite(charArray, sizeof(char), numberOfChar, file);

    if (wrote < numberOfChar){
        fclose(file);
        return RC_WRITE_FAILED;
    }
    free(charArray);
    fHandle->totalNumPages++;

    // writing new number of pages in the file
    fseek(file, 0L, SEEK_SET);
    if (fprintf(file, "%d", fHandle->totalNumPages) != 1){
        return RC_WRITE_FAILED;
    }

    fseek(file, curBlockPos*PAGE_SIZE, SEEK_SET);
    return RC_OK;
}

extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    while (fHandle->totalNumPages < numberOfPages){
        if (appendEmptyBlock(fHandle) != RC_OK){
            return RC_WRITE_FAILED;
        }
    }
    return RC_OK;
}
