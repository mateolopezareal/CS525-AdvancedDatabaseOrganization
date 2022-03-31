#include "dberror.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>


/*
 * Create an empty frame container with numberOfFrames frames
 * The result need to be freed before the end of the program
 */
BM_FramesHandle *createFrames(int numberOfFrames) {
    BM_FramesHandle *frames = malloc(sizeof(BM_FramesHandle));
    frames->frames = malloc(sizeof(BM_FrameHandle *) * numberOfFrames);
    for (int i = 0; i < numberOfFrames; i++) {
        frames->frames[i] = NULL;
    }
    frames->actualUsedFrames = 0;
    frames->lastPinnedPosition = -1;
    return frames;
}


/*
 * Loop over the frames in order to find which one contains the page number pageNum and returns it
 * If not found returns NULL
 * This could be improved by using a hash table.
 */
BM_FrameHandle *findFrameNumberN(BM_BufferPool *const bm, const PageNumber pageNum) {
    BM_FramesHandle *framesHandle = (BM_FramesHandle *) bm->mgmtData;
    for (int i = 0; i < bm->numPages; i++) {
        if (framesHandle->frames[i] != NULL) {
            if (framesHandle->frames[i]->page->pageNum == pageNum) {
                return framesHandle->frames[i];
            }
        }
    }
    return (BM_FrameHandle *) NULL;
}

/*
 * Taking a buffer pool, page handle already initialized (i.e pageNum and data are correct) finds a frame in the buffer
 * pool to store the information.
 * The strategy used to find a place is FIFO
 */
RC fifoReplacement(BM_BufferPool *const bm, BM_PageHandle *const page, SM_FileHandle fh) {
    BM_FramesHandle *framesHandle = (BM_FramesHandle *) bm->mgmtData;
    for (int i = 1; i < bm->numPages; i++) {
        int position = (framesHandle->lastPinnedPosition + i) % bm->numPages;
        BM_FrameHandle *frame = framesHandle->frames[position];
        /* The frame can be evicted */
        if (frame->fixCount == 0) {
            if (frame->isDirty == TRUE) {
                if (writeBlock(frame->page->pageNum, &fh, frame->page->data) != RC_OK)
                    return RC_WRITE_FAILED;
                bm->numberOfWriteIO++;
            }
            free(frame->page->data);
            frame->page->data = page->data;
            frame->page->pageNum = page->pageNum;
            frame->fixCount = 1;
            frame->isDirty = 0;
            time(&frame->lastAccess);
            frame->positionInFramesArray = position;
            framesHandle->lastPinnedPosition = position;
            closePageFile(&fh);
            return RC_OK;
        }
    }
    // CHANGE RETURN CODE
    return RC_WRITE_FAILED;
}

/*
 * Taking a buffer pool, page handle already initialized (i.e pageNum and data are correct) finds a frame in the buffer
 * pool to store the information.
 * The strategy used to find a place is LRU
 */
RC lruReplacement(BM_BufferPool *const bm, BM_PageHandle *const page, SM_FileHandle fh) {
    BM_FramesHandle *framesHandle = (BM_FramesHandle *) bm->mgmtData;
    BM_FrameHandle *leastRecentlyUsedFrame = framesHandle->frames[0];
    for (int i =0; i < bm->numPages; i++) {
        BM_FrameHandle *frame = framesHandle->frames[i];
        /* Searching the least recently used frame from the one that can be evicted (i.e fixCount = 0) */
        if (frame->fixCount == 0) {
            if (difftime(leastRecentlyUsedFrame->lastAccess, frame->lastAccess) >= 0) {
                leastRecentlyUsedFrame = frame;
            }
        }
    }

    /* Every frames are pinned at least once */
    if (leastRecentlyUsedFrame->fixCount != 0){
        // CHANGE RETURN CODE
        return RC_WRITE_FAILED;
    }

    if (leastRecentlyUsedFrame->isDirty == TRUE) {
        if (writeBlock(leastRecentlyUsedFrame->page->pageNum, &fh, leastRecentlyUsedFrame->page->data) != RC_OK)
            return RC_WRITE_FAILED;
        bm->numberOfWriteIO++;
    }
    struct timeval tv;
    free(leastRecentlyUsedFrame->page->data);

    leastRecentlyUsedFrame->page->data = page->data;
    leastRecentlyUsedFrame->page->pageNum = page->pageNum;
    leastRecentlyUsedFrame->fixCount = 1;
    leastRecentlyUsedFrame->isDirty = 0;
    gettimeofday(&tv, NULL);
    leastRecentlyUsedFrame->lastAccess = tv.tv_usec;

    framesHandle->lastPinnedPosition = leastRecentlyUsedFrame->positionInFramesArray;
    closePageFile(&fh);
    return RC_OK;
}

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData) {
    // CHECK IF FILE EXISTS
    if (access(pageFileName, F_OK) == 0) {
        // file exists
        initStorageManager();
        bm->pageFile = pageFileName;
        bm->numPages = numPages;
        bm->mgmtData = createFrames(numPages);
        bm->strategy = strategy;
        bm->numberOfReadIO = 0;
        bm->numberOfWriteIO = 0;

        return RC_OK;
    }
    return RC_FILE_NOT_FOUND;
}

RC shutdownBufferPool(BM_BufferPool *const bm) {
    char *filename = (char *) bm->pageFile;
    SM_FileHandle fh;
    if (openPageFile(filename, &fh) != RC_OK) {
        return RC_FILE_NOT_FOUND;
    }
    BM_FramesHandle *frames = bm->mgmtData;
    for (int i = 0; i < bm->numPages; i++) {
        BM_FrameHandle *frame = frames->frames[i];
        if (frame != NULL) {
            if (frame->fixCount != 0) {
                //CHANGE RETURN CODE
                return RC_WRITE_FAILED;
            }
            if (frame->isDirty == TRUE) {
                writeBlock(frame->page->pageNum, &fh, frame->page->data);
            }
            free(frame->page->data);
            free(frame->page);
            free(frame);
        }
    }
    free(frames->frames);
    free(frames);
    closePageFile(&fh);
    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm) {
    char *filename = (char *) bm->pageFile;
    SM_FileHandle fh;
    if (openPageFile(filename, &fh) != RC_OK) {
        return RC_FILE_NOT_FOUND;
    }
    BM_FramesHandle *frames = bm->mgmtData;
    for (int i = 0; i < bm->numPages; i++) {
        BM_FrameHandle *frame = frames->frames[i];
        if (frame != NULL) {
            if (frame->isDirty == TRUE) {
                writeBlock(frame->page->pageNum, &fh, frame->page->data);
                frame->isDirty = FALSE;
                bm->numberOfWriteIO++;
            }
        }
    }
    closePageFile(&fh);
    return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    BM_FrameHandle *foundFrame = findFrameNumberN(bm, page->pageNum);
    if (foundFrame != NULL) {
        foundFrame->isDirty = TRUE;
        return RC_OK;
    }

    // CHANGE RETURNED CODE
    return RC_WRITE_FAILED;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    BM_FrameHandle *foundFrame = findFrameNumberN(bm, page->pageNum);
    if (foundFrame != NULL) {
        foundFrame->fixCount--;
        return RC_OK;
    }

    // CHANGE RETURNED CODE
    return RC_WRITE_FAILED;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    BM_FrameHandle *foundFrame = findFrameNumberN(bm, page->pageNum);
    if (foundFrame != NULL) {

        char *filename = (char *) bm->pageFile;
        SM_FileHandle fh;
        if (openPageFile(filename, &fh) != RC_OK) {
            return RC_FILE_NOT_FOUND;
        }

        if (writeBlock(page->pageNum, &fh, foundFrame->page->data) != RC_OK) {
            return RC_WRITE_FAILED;
        }
        bm->numberOfWriteIO++;
        foundFrame->isDirty = 0;
        closePageFile(&fh);
        return RC_OK;
    }

    // CHANGE RETURNED CODE
    return RC_WRITE_FAILED;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
           const PageNumber pageNum) {

    BM_FramesHandle *framesHandle = (BM_FramesHandle *) bm->mgmtData;
    BM_FrameHandle *foundFrame = findFrameNumberN(bm, pageNum);
    struct timeval tv;

    /* We found the page in the buffer */
    if (foundFrame != NULL) {
        page->data = foundFrame->page->data;
        page->pageNum = pageNum;
        foundFrame->fixCount++;
        gettimeofday(&tv, NULL);
        foundFrame->lastAccess = tv.tv_usec;
        framesHandle->lastPinnedPosition = foundFrame->positionInFramesArray;
        return RC_OK;
    }

    /* Page is not in buffer, we will need to storage manager to get it from the disk */
    char *filename = (char *) bm->pageFile;
    SM_FileHandle fh;
    if (openPageFile(filename, &fh) != RC_OK) {
        return RC_FILE_NOT_FOUND;
    }
    ensureCapacity(pageNum + 1, &fh); // +1 because pages are numbered started from 0

    page->data = malloc(PAGE_SIZE);


    RC read = readBlock(pageNum, &fh, page->data);
    if (read != RC_OK) {
        free(page->data);
        closePageFile(&fh);
        return read;
    }
    bm->numberOfReadIO++;

    page->pageNum = pageNum;

    /* Looking if we still have place in the frames */
    if (framesHandle->actualUsedFrames < bm->numPages) {
        int availablePosition = framesHandle->lastPinnedPosition + 1;

        BM_FrameHandle *frame = malloc(sizeof(BM_FrameHandle));
        frame->page = malloc(sizeof(BM_PageHandle));

        frame->page->data = page->data;
        frame->fixCount = 1;
        frame->isDirty = FALSE;
        frame->positionInFramesArray = availablePosition;
        frame->page->pageNum = pageNum;
        gettimeofday(&tv, NULL);
        frame->lastAccess = tv.tv_usec;
        framesHandle->frames[availablePosition] = frame;
        framesHandle->actualUsedFrames++;
        framesHandle->lastPinnedPosition = availablePosition;
        closePageFile(&fh);
        return RC_OK;
    }

    /* If we don't have any place */

    switch (bm->strategy) {
        case RS_FIFO:
            return fifoReplacement(bm, page, fh);
        case RS_CLOCK:
            break;
        case RS_LRU:
            return lruReplacement(bm, page, fh);
        case RS_LFU:
            break;
        case RS_LRU_K:
            break;
        default:
            // CHANGE RETURN CODE
            return RC_WRITE_FAILED;
    }


    /*We didn't find any evicable page */
    // CHANGE RETURN CODE
    closePageFile(&fh);
    return RC_WRITE_FAILED;
}


// Statistics Interface

/* Results need to be freed after use */
PageNumber *getFrameContents(BM_BufferPool *const bm) {
    BM_FramesHandle *frames = bm->mgmtData;
    PageNumber *arrayOfPageNumber = malloc(sizeof(PageNumber) * bm->numPages);
    for (int i = 0; i < bm->numPages; i++) {
        BM_FrameHandle *frame = frames->frames[i];
        if (frame != NULL)
            arrayOfPageNumber[i] = frame->page->pageNum;
        else
            arrayOfPageNumber[i] = NO_PAGE;
    }
    return arrayOfPageNumber;
}
/* Results need to be freed after use */
bool *getDirtyFlags(BM_BufferPool *const bm) {
    bool *array = malloc(sizeof(bool) * bm->numPages);
    BM_FramesHandle *frames = bm->mgmtData;
    for (int i = 0; i < bm->numPages; i++) {
        BM_FrameHandle *frame = frames->frames[i];
        if (frame != NULL) {
            array[i] = frame->isDirty;
        } else
            array[i] = FALSE;
    }
    return array;
}
/* Results need to be freed after use */
int *getFixCounts(BM_BufferPool *const bm) {
    int *array = malloc(sizeof(int) * bm->numPages);
    BM_FramesHandle *frames = bm->mgmtData;
    for (int i = 0; i < bm->numPages; i++) {
        BM_FrameHandle *frame = frames->frames[i];
        if (frame != NULL) {
            array[i] = frame->fixCount;
        } else
            array[i] = 0;
    }
    return array;
}

int getNumReadIO(BM_BufferPool *const bm) {
    return bm->numberOfReadIO;
}

int getNumWriteIO(BM_BufferPool *const bm) {
    return bm->numberOfWriteIO;
}