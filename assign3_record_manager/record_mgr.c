#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "record_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"

#define ATTRIBUTE_NAME_LEN 5

typedef struct RM_FreeRecord RM_FreeRecord; // prototype so it can be used inside the declaration

typedef struct RM_FreeRecord {
	RID* rid;
	RM_FreeRecord* next;
} RM_FreeRecord;

typedef struct RM_FreeRecordsQueue {
	RM_FreeRecord* first;
	RM_FreeRecord* last;
	int numberOfFreeRecords;
} RM_FreeRecordsQueue;

typedef struct RM_RecordMgr {
	BM_PageHandle* pageHandle;
	BM_BufferPool* bufferPool;
	int tuplesCount;
	RM_FreeRecordsQueue* freeRecordsQueue;
	int recordSize;
} RM_RecordMgr;

typedef struct RM_ScanMgr {
	RID lastRid;
	Expr* condition;
	int scanCount;
} RM_ScanMgr;

void printMetaData(char* metapage) {
	printf("PRINTING METADATA : ");
	int numTuples = *(int*)metapage;
	metapage += sizeof(int);

	int numAttr = *(int*)metapage;
	metapage += sizeof(int);

	int keySize = *(int*)metapage;
	metapage += sizeof(int);

	printf("%d %d %d ", numTuples, numAttr, keySize);

	for (int i = 0; i < keySize; i++) {
		printf("%d ", *metapage);
		metapage += sizeof(int);
	}

	for (int i = 0; i < numAttr; i++) {
		char* name = malloc(ATTRIBUTE_NAME_LEN);
		strncpy(name, metapage, ATTRIBUTE_NAME_LEN);
		metapage += ATTRIBUTE_NAME_LEN;

		int dataType = *(int*)metapage;
		metapage += sizeof(int);

		int typeLen = *(int*)metapage;
		metapage += sizeof(int);

		printf("%s %d %d ", name, dataType, typeLen);
		free(name);
	}
	int recordSize = *(int*)metapage;
	printf("%d ", recordSize);
	metapage += sizeof(int);

	int page;
	int slot;
	while (*metapage != '\0') {
		page = *(int*)metapage;
		metapage += sizeof(int);

		slot = *(int*)metapage;
		metapage += sizeof(int);

		printf("%d %d ", page, slot);
	}
	printf("\n");
}

RM_FreeRecordsQueue* initFreeRecordsQueue() {
	RM_FreeRecordsQueue* queue = malloc(sizeof(RM_FreeRecordsQueue));
	queue->first = NULL;
	queue->last = NULL;
	queue->numberOfFreeRecords = 0;
	return queue;
}

void freeRecordsQueue(RM_FreeRecordsQueue* queue) {
	if (queue->numberOfFreeRecords != 0) {
		while (queue->first->next != NULL) {
			RM_FreeRecord* next = queue->first->next;
			free(queue->first->rid);
			free(queue->first);
			queue->first = next;
			queue->numberOfFreeRecords--;
		}
	}
	if (queue->numberOfFreeRecords == 1) {
		free(queue->first->rid);
		free(queue->first);

	}
	free(queue);
}

void insertInFreeQueue(RM_FreeRecordsQueue* queue, RID rid) {
	RM_FreeRecord* freeRecord = malloc(sizeof(RM_FreeRecord));
	freeRecord->rid = malloc(sizeof(RID));
	freeRecord->rid->slot = rid.slot;
	freeRecord->rid->page = rid.page;
	freeRecord->next = (RM_FreeRecord*)NULL;
	if (queue->last != NULL) {
		queue->last->next = freeRecord;
	}
	if (queue->first == NULL) {
		queue->first = freeRecord;
	}
	queue->last = freeRecord;
	queue->numberOfFreeRecords++;
}

RID* getFirstFreeRecordRid(RM_FreeRecordsQueue* queue) {
	RM_FreeRecord* freeRecord;
	if (queue->numberOfFreeRecords == 0) {
		return NULL;
	}

	RID* rid = queue->first->rid;
	freeRecord = queue->first;
	queue->first = queue->first->next;
	free(freeRecord);
	queue->numberOfFreeRecords--;
	return rid;
}

// global var record manager to store useful info
RM_RecordMgr* recordMgr;

/*
 * Fill a pageHandle with initial values
 * content is [numberOfTuples numberOfAttributes keySize keyAttr1 keyAttr2 ... attr1Name attr1DataType attr1TypeLen attr2Name attr2DataType attr2TypeLen ... recordSize freeRid1 freeRid2 ...]
*/
void initialFillPageHandle(BM_PageHandle* pageHandle, Schema* schema) {
	//Number of Tuples: 0 in the first 4 bytes
	*(int*)pageHandle->data = 0;
	pageHandle->data = pageHandle->data + sizeof(int);

	// Setting the number of attributes
	*(int*)pageHandle->data = schema->numAttr;
	pageHandle->data = pageHandle->data + sizeof(int);

	// Setting the Key Size of the attributes
	*(int*)pageHandle->data = schema->keySize;
	pageHandle->data = pageHandle->data + sizeof(int);

	// storing key attr
	for (int i = 0; i < schema->keySize; i++) {
		*(int*)pageHandle->data = (int)schema->keyAttrs[i];
		pageHandle->data += sizeof(int);
	}

	for (int i = 0; i < schema->numAttr; i++) {
		strncpy(pageHandle->data, schema->attrNames[i], ATTRIBUTE_NAME_LEN);
		pageHandle->data = pageHandle->data + ATTRIBUTE_NAME_LEN;

		*(int*)pageHandle->data = (int)schema->dataTypes[i];
		pageHandle->data += sizeof(int);

		*(int*)pageHandle->data = (int)schema->typeLength[i];
		pageHandle->data += sizeof(int);
	}

	*(int*)pageHandle->data = getRecordSize(schema);
	pageHandle->data = pageHandle->data + sizeof(int);
}

void finalFillPageHandle(BM_PageHandle* pageHandle, RM_TableData* table) {
	Schema* schema = table->schema;
	RM_RecordMgr* recordMgr = (RM_RecordMgr*)table->mgmtData;

	//Number of Tuples: 0 in the first 4 bytes
	*(int*)pageHandle->data = recordMgr->tuplesCount;
	pageHandle->data = pageHandle->data + sizeof(int);

	// Setting the number of attributes
	*(int*)pageHandle->data = schema->numAttr;
	pageHandle->data = pageHandle->data + sizeof(int);

	// Setting the Key Size of the attributes
	*(int*)pageHandle->data = schema->keySize;
	pageHandle->data = pageHandle->data + sizeof(int);

	// storing key attr
	for (int i = 0; i < schema->keySize; i++) {
		*(int*)pageHandle->data = (int)schema->keyAttrs[i];
		pageHandle->data += sizeof(int);
	}

	for (int i = 0; i < schema->numAttr; i++) {
		strncpy(pageHandle->data, schema->attrNames[i], ATTRIBUTE_NAME_LEN);
		pageHandle->data = pageHandle->data + ATTRIBUTE_NAME_LEN;

		*(int*)pageHandle->data = (int)schema->dataTypes[i];
		pageHandle->data += sizeof(int);

		*(int*)pageHandle->data = (int)schema->typeLength[i];
		pageHandle->data += sizeof(int);
	}

	*(int*)pageHandle->data = recordMgr->recordSize;
	pageHandle->data = pageHandle->data + sizeof(int);

	RM_FreeRecordsQueue* queue = recordMgr->freeRecordsQueue;
	RM_FreeRecord* freeRecord = queue->first;
	while (freeRecord != NULL) {
		printf("putting free in page. rid %d %d\n", freeRecord->rid->page, freeRecord->rid->slot);
		*(int*)pageHandle->data = freeRecord->rid->page;
		pageHandle->data = pageHandle->data + sizeof(int);

		*(int*)pageHandle->data = freeRecord->rid->slot;
		pageHandle->data = pageHandle->data + sizeof(int);

		freeRecord = freeRecord->next;
	}

}

void fillSchemaFromLoadedPage(BM_PageHandle* pageHandle, Schema* schema) {
	int numAttr = *(int*)pageHandle->data;
	schema->numAttr = numAttr;
	pageHandle->data += sizeof(int);

	schema->keySize = *(int*)pageHandle->data;
	pageHandle->data += sizeof(int);

	schema->keyAttrs = malloc(sizeof(int) * schema->keySize);

	// get key attr
	for (int i = 0; i < schema->keySize; i++) {
		schema->keyAttrs[i] = *(int*)pageHandle->data;
		pageHandle->data += sizeof(int);
	}

	// allocating memory to store the values

	schema->attrNames = (char**)malloc(sizeof(char*) * numAttr);
	schema->dataTypes = (DataType*)malloc(sizeof(DataType) * numAttr);
	schema->typeLength = (int*)malloc(sizeof(int) * numAttr);


	for (int i = 0; i < numAttr; i++) {
		schema->attrNames[i] = (char*)malloc(ATTRIBUTE_NAME_LEN);
		memcpy(schema->attrNames[i], pageHandle->data, ATTRIBUTE_NAME_LEN);
		pageHandle->data += ATTRIBUTE_NAME_LEN;

		schema->dataTypes[i] = *(int*)pageHandle->data;
		pageHandle->data = pageHandle->data + sizeof(int);

		schema->typeLength[i] = *(int*)pageHandle->data;
		pageHandle->data = pageHandle->data + sizeof(int);
	}
}

RC initRecordManager(void* mgmtData) {
	initStorageManager();
	recordMgr = (RM_RecordMgr*)malloc(sizeof(RM_RecordMgr));
	return RC_OK;
}

RC shutdownRecordManager() {
	free(recordMgr);
	return RC_OK;
}

RC createTable(char* name, Schema* schema) {
	if (createPageFile(name) != RC_OK) {
		return RC_FILE_NOT_FOUND;
	}

	BM_BufferPool* bufferPool = MAKE_POOL();
	BM_PageHandle* pageHandle = MAKE_PAGE_HANDLE();

	//We decided 5 pages for the buffer and LRU strategy
	if (initBufferPool(bufferPool, name, 5, RS_LRU, NULL) != RC_OK) {
		return RC_FILE_NOT_FOUND;
	}

	if (pinPage(bufferPool, pageHandle, 0) != RC_OK) {
		return RC_WRITE_FAILED;
	}

	initialFillPageHandle(pageHandle, schema);

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

RC openTable(RM_TableData* rel, char* name) {

	recordMgr->bufferPool = MAKE_POOL();
	recordMgr->pageHandle = MAKE_PAGE_HANDLE();
	recordMgr->freeRecordsQueue = initFreeRecordsQueue();

	if (initBufferPool(recordMgr->bufferPool, name, 5, RS_LRU, NULL) != RC_OK) {
		return RC_FILE_NOT_FOUND;
	}

	if (pinPage(recordMgr->bufferPool, recordMgr->pageHandle, 0) != RC_OK) {
		return RC_WRITE_FAILED;
	}

	printf("In open table\n");
	printMetaData(recordMgr->pageHandle->data);

	if (unpinPage(recordMgr->bufferPool, recordMgr->pageHandle) != RC_OK) {
		return RC_WRITE_FAILED;
	}

	rel->name = name;

	recordMgr->tuplesCount = *(int*)recordMgr->pageHandle->data;
	printf("there are %d tuples\n", recordMgr->tuplesCount);
	recordMgr->pageHandle->data += sizeof(int);

	rel->schema = (Schema*)malloc(sizeof(Schema));


	fillSchemaFromLoadedPage(recordMgr->pageHandle, rel->schema);

	recordMgr->recordSize = *(int*)recordMgr->pageHandle->data;
	recordMgr->pageHandle->data += sizeof(int);

	//     filling the queue of free slots
	RID rid;
	while (*recordMgr->pageHandle->data != '\0') {
		rid.page = *(int*)recordMgr->pageHandle->data;
		recordMgr->pageHandle->data += sizeof(int);

		rid.slot = *(int*)recordMgr->pageHandle->data;
		recordMgr->pageHandle->data += sizeof(int);

		insertInFreeQueue(recordMgr->freeRecordsQueue, rid);
	}

	rel->mgmtData = recordMgr;

	return RC_OK;

}

RC closeTable(RM_TableData* rel) {
	printf("Closing table\n");
	recordMgr = rel->mgmtData;

	if (pinPage(recordMgr->bufferPool, recordMgr->pageHandle, 0) != RC_OK) {
		return RC_WRITE_FAILED;
	}
	printMetaData(recordMgr->pageHandle->data);
	finalFillPageHandle(recordMgr->pageHandle, rel);
	if (markDirty(recordMgr->bufferPool, recordMgr->pageHandle) != RC_OK) {
		return RC_WRITE_FAILED;
	}
	if (unpinPage(recordMgr->bufferPool, recordMgr->pageHandle) != RC_OK) {
		return RC_WRITE_FAILED;
	}
	if (forceFlushPool(recordMgr->bufferPool) != RC_OK) {
		return RC_WRITE_FAILED;
	}

	freeRecordsQueue(recordMgr->freeRecordsQueue);
	if (freeSchema(rel->schema) != RC_OK) {
		return RC_WRITE_FAILED;
	}
	free(recordMgr->pageHandle);
	RC r = shutdownBufferPool(recordMgr->bufferPool);
	free(recordMgr->bufferPool);
	return r;
}

RC deleteTable(char* name) {
	printf("Deleting table\n");
	return destroyPageFile(name);
}

int getNumTuples(RM_TableData* rel) {
	recordMgr = rel->mgmtData;
	if (pinPage(recordMgr->bufferPool, recordMgr->pageHandle, 0) != RC_OK) {
		return RC_WRITE_FAILED;
	}
	int num = *(int*)recordMgr->pageHandle->data;
	if (unpinPage(recordMgr->bufferPool, recordMgr->pageHandle) != RC_OK) {
		return RC_WRITE_FAILED;
	}
	return num;
}

RC insertRecord(RM_TableData* rel, Record* record) {
	recordMgr = rel->mgmtData;

	RID* freeSlotRID = getFirstFreeRecordRid(recordMgr->freeRecordsQueue);

	if (freeSlotRID == NULL) {
		freeSlotRID = (RID*)malloc(sizeof(RID));

		int maxRecordPerPage = floor(PAGE_SIZE / (recordMgr->recordSize + 1));
		int lastPage = 1 + floor(recordMgr->tuplesCount / maxRecordPerPage);
		freeSlotRID->page = lastPage;

		int recordInLastPage = recordMgr->tuplesCount - (lastPage - 1) * maxRecordPerPage;
		freeSlotRID->slot = recordInLastPage;
	}

	if (pinPage(recordMgr->bufferPool, recordMgr->pageHandle, freeSlotRID->page) != RC_OK) {
		return RC_WRITE_FAILED;
	}

	recordMgr->pageHandle->data += (recordMgr->recordSize + 1) * freeSlotRID->slot;

	char* start = record->data;
	*(char*)recordMgr->pageHandle->data = '+';
	recordMgr->pageHandle->data++;

	for (int i = 0; i < recordMgr->recordSize; i++) {
		*(char*)recordMgr->pageHandle->data = *(char*)record->data;
		recordMgr->pageHandle->data++;
		record->data++;
	}

	record->data = start;
	record->id = *freeSlotRID;
	free(freeSlotRID);


	if (markDirty(recordMgr->bufferPool, recordMgr->pageHandle) != RC_OK) {
		return RC_WRITE_FAILED;
	}
	if (unpinPage(recordMgr->bufferPool, recordMgr->pageHandle) != RC_OK) {
		return RC_WRITE_FAILED;
	}
	recordMgr->tuplesCount++;
	return RC_OK;
}

RC deleteRecord(RM_TableData* rel, RID id) {
	recordMgr = rel->mgmtData;
	int page = id.page;
	int slot = id.slot;
	if (pinPage(recordMgr->bufferPool, recordMgr->pageHandle, page) != RC_OK) {
		return RC_WRITE_FAILED;
	}
	recordMgr->pageHandle->data += slot * (recordMgr->recordSize + 1);
	printf("PUTTING -\n");
	*(char*)recordMgr->pageHandle->data = '-';

	if (markDirty(recordMgr->bufferPool, recordMgr->pageHandle) != RC_OK) {
		return RC_WRITE_FAILED;
	}

	if (unpinPage(recordMgr->bufferPool, recordMgr->pageHandle) != RC_OK) {
		return RC_WRITE_FAILED;
	}

	insertInFreeQueue(recordMgr->freeRecordsQueue, id);
	recordMgr->tuplesCount--;
	return RC_OK;
}

RC updateRecord(RM_TableData* rel, Record* record) {
	recordMgr = rel->mgmtData;
	int page = record->id.page;
	int slot = record->id.slot;

	if (pinPage(recordMgr->bufferPool, recordMgr->pageHandle, page) != RC_OK) {
		return RC_WRITE_FAILED;
	}

	recordMgr->pageHandle->data += slot * (recordMgr->recordSize + 1) + 1;

	char* start = record->data;

	for (int i = 0; i < recordMgr->recordSize; i++) {
		*(char*)recordMgr->pageHandle->data = *(char*)record->data;
		recordMgr->pageHandle->data++;
		record->data++;
	}

	record->data = start;

	if (markDirty(recordMgr->bufferPool, recordMgr->pageHandle) != RC_OK) {
		return RC_WRITE_FAILED;
	}

	if (unpinPage(recordMgr->bufferPool, recordMgr->pageHandle) != RC_OK) {
		return RC_READ_NON_EXISTING_PAGE;
	}

	return RC_OK;
}

RC getRecord(RM_TableData* rel, RID id, Record* record) {
	recordMgr = rel->mgmtData;
	int page = id.page;
	int slot = id.slot;

	if (pinPage(recordMgr->bufferPool, recordMgr->pageHandle, page) != RC_OK) {
		return RC_WRITE_FAILED;
	}

	if (unpinPage(recordMgr->bufferPool, recordMgr->pageHandle) != RC_OK) {
		return RC_READ_NON_EXISTING_PAGE;
	}

	recordMgr->pageHandle->data += slot * (recordMgr->recordSize + 1);

	// tuple is deleted
	if (*recordMgr->pageHandle->data != '+') {
		return RC_WRITE_FAILED;
	}
	recordMgr->pageHandle->data++;

	memcpy(record->data, recordMgr->pageHandle->data, recordMgr->recordSize);
	record->id = id;


	return RC_OK;
}

RC createRecord(Record** record, Schema* schema) {
	*record = (Record*)malloc(sizeof(Record));

	int recordSize = getRecordSize(schema);

	(*record)->data = (char*)malloc(recordSize);
	(*record)->id.page = 0;
	(*record)->id.slot = 0;

	return RC_OK;

}

RC freeRecord(Record* record) {
	free(record->data);
	free(record);
	return RC_OK;
}

RC startScan(RM_TableData* rel, RM_ScanHandle* scan, Expr* cond) {
	RM_ScanMgr* scanManager = (RM_ScanMgr*)malloc(sizeof(RM_ScanMgr));
	recordMgr = (RM_RecordMgr*)rel->mgmtData;

	scanManager->condition = cond;
	scanManager->lastRid.page = 1;
	scanManager->lastRid.slot = 0;

	scanManager->scanCount = 0;

	scan->mgmtData = scanManager;
	scan->rel = rel;
	return RC_OK;
}

RID computeNextRid(RID actualRid, int recordSize) {
	RID nextRid;
	int maxRecordPerPage = floor(PAGE_SIZE / (recordSize + 1));
	if (actualRid.slot >= maxRecordPerPage) {
		nextRid.page = actualRid.page + 1;
		nextRid.slot = 0;
	}
	else {
		nextRid.page = actualRid.page;
		nextRid.slot = actualRid.slot + 1;
	}
	return nextRid;
}

RC next(RM_ScanHandle* scan, Record* record) {
	RM_ScanMgr* scanManager = (RM_ScanMgr*)scan->mgmtData;
	RID nextRid = computeNextRid(scanManager->lastRid, recordMgr->recordSize);
	if (scanManager->condition == NULL) {
		getRecord(scan->rel, nextRid, record);
		return RC_OK;
	}
	Value* result = (Value*)malloc(sizeof(Value));
	result->v.boolV = FALSE;
	while (result->v.boolV != TRUE) {
		free(result);
		if (getRecord(scan->rel, nextRid, record) != RC_OK) {
			return RC_RM_NO_MORE_TUPLES;
		}
		evalExpr(record, scan->rel->schema, scanManager->condition, &result);
		nextRid = computeNextRid(nextRid, recordMgr->recordSize);
		scanManager->scanCount++;
	}

	if (result->v.boolV == TRUE) {
		free(result);
		scanManager->lastRid = nextRid;
		return RC_OK;
	}

	if (scanManager->scanCount == recordMgr->tuplesCount) {
		free(result);
		return RC_RM_NO_MORE_TUPLES;
	}
	free(result);
	printf("Found record satisfying condition\n");
	return RC_OK;
}

RC closeScan(RM_ScanHandle* scan) {
	RM_ScanMgr* scanMgr = scan->mgmtData;
	free(scanMgr);
	return RC_OK;
}

// dealing with schemas
int getRecordSize(Schema* schema) {
	int size = 0;
	for (int i = 0; i < schema->numAttr; i++) {
		switch (schema->dataTypes[i]) {
		case DT_INT:
			size += sizeof(int);
			break;
		case DT_STRING:
			size += schema->typeLength[i];
			break;
		case DT_FLOAT:
			size += sizeof(float);
			break;
		case DT_BOOL:
			size += sizeof(bool);
			break;
		}
	}
	return size;
}

Schema* createSchema(int numAttr, char** attrNames, DataType* dataTypes, int* typeLength, int keySize, int* keys) {
	Schema* schema = malloc(sizeof(Schema));
	schema->numAttr = numAttr;
	schema->attrNames = attrNames;
	schema->dataTypes = dataTypes;
	schema->typeLength = typeLength;
	schema->keyAttrs = keys;
	schema->keySize = keySize;
	return schema;
}

RC freeSchema(Schema* schema) {
	for (int i = 0; i < schema->numAttr; i++) {
		free(schema->attrNames[i]);
	}
	free(schema->attrNames);
	free(schema->dataTypes);
	free(schema->typeLength);
	free(schema->keyAttrs);
	free(schema);
	return RC_OK;
}


// to have access to func in rm_serializer.c
static RC attrOffset(Schema* schema, int attrNum, int* result);

RC getAttr(Record* record, Schema* schema, int attrNum, Value** value) {
	// same way as in rm_serializer.c
	int offset;
	char* attrData;
	(*value) = malloc(sizeof(Value));
	DataType type = schema->dataTypes[attrNum];
	attrOffset(schema, attrNum, &offset);
	attrData = record->data + offset;
	switch (type) {
	case DT_INT: ;
		int int_val = (int)*attrData;
		(*value)->dt = DT_INT;
		(*value)->v.intV = int_val;
		break;
	case DT_STRING: ;
		int len = schema->typeLength[attrNum];
		(*value)->v.stringV = (char*)malloc(len + 1);
		strncpy((*value)->v.stringV, attrData, len);
		(*value)->v.stringV[len] = '\0';
		(*value)->dt = DT_STRING;
		break;
	case DT_FLOAT: ;
		float float_val = (float)*attrData;
		(*value)->dt = DT_FLOAT;
		(*value)->v.floatV = float_val;
		break;
	case DT_BOOL: ;
		bool bool_val = (bool)*attrData;
		(*value)->dt = DT_BOOL;
		(*value)->v.boolV = bool_val;
		break;
	}
	return RC_OK;
}

RC setAttr(Record* record, Schema* schema, int attrNum, Value* value) {
	int offset;

	attrOffset(schema, attrNum, &offset);

	char* pointerToData = record->data + offset;

	switch (schema->dataTypes[attrNum]) {
	case DT_STRING: {
		int string_len = schema->typeLength[attrNum];

		strncpy(pointerToData, value->v.stringV, string_len);
		break;
	}

	case DT_INT: {
		// Setting attribute value of an attribute of type INTEGER
		*(int*)pointerToData = value->v.intV;
		break;
	}

	case DT_FLOAT: {
		// Setting attribute value of an attribute of type FLOAT
		*(float*)pointerToData = value->v.floatV;
		break;
	}

	case DT_BOOL: {
		// Setting attribute value of an attribute of type STRING
		*(bool*)pointerToData = value->v.boolV;
		break;
	}
	}
	return RC_OK;
}

RC
attrOffset(Schema* schema, int attrNum, int* result) {
	int offset = 0;
	int attrPos = 0;

	for (attrPos = 0; attrPos < attrNum; attrPos++) {
		switch (schema->dataTypes[attrPos]) {
		case DT_STRING:
			offset += schema->typeLength[attrPos];
			break;
		case DT_INT:
			offset += sizeof(int);
			break;
		case DT_FLOAT:
			offset += sizeof(float);
			break;
		case DT_BOOL:
			offset += sizeof(bool);
			break;
		}
	}
	*result = offset;
	return RC_OK;
}
