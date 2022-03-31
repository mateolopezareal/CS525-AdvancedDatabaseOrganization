# CS525 Coding assignment 3 : Record manager
## Group
|Student name| iit mail|
|---|---|
|Decou Nathan|[ndecou@hawk.iit.edu](mailto:ndecou@hawk.iit.edu)|
|Fernandez	Mateo|[mfernandezlopezareal@hawk.iit.edu](mailto:mfernandezlopezareal@hawk.iit.edu)|
|Santana	Mikel|[msantanamarana@hawk.iit.edu](mailto:msantanamarana@hawk.iit.edu)|

## Preliminary note
Modification has been done in some `.h` files (`buffer_mgr.h` for example) in order to have access to 
our own structure and methods. Using only `record_mgr.c` with the stock `.h` files won't compile because of this.
However, no modification that changes the comportment of the stock function has been done.

Also, some memory leaks have been fixed in `test_assign3_1.c`. Some persists as we don't really know where do they come from.
Because of the deadline we didn't have the time to fix the leaks in the updated version of the tests. That is why running a
memory analysis on this will return some errors.

## How to compile and run
- To compile the test file use the Makefile rule `make test_assign3_1`
- To run the tests use the Makefile rule `make run_test_assign3_1`.
> :arrow_up: This rule will compile **and** run the tests. No need to use `make test_assign3_1` before.
- Use `make memory_check_test_assign3_1` to check for memory leaks.
> :arrow_up: This rule will compile **and** run the tests using Valgrind. No need to use `make test_assign3_1` before.

Addin `_V2` at the end of these command will do the exact same thing using the V2 version of the file test.

- To clean (i.e. remove binary files, temporary files etc.) use `make clean`

## Added tests scenarios
Regarding the tests, we did not add any new but use the ones which were already created. All of them work as expected.

## Code explanation
### What it is in the first page of file (Metadata).
The structure of the first page of every file, we have decided to be as following: 
  - First, number of tuples.
  
  - As for the schema of the table, the following. Number of attributes, keysize, attributes of each of the keys. Then, per each attribute, attributes names, data type and type     lenght. Finally, the record size.
  
  - After the schema, we save on that first page the free slots in the table. Each free slots is saved as two integers, its page and its slot number in the page.
  
### What is it in the record manager
We have decided to use a structure called RM_RecordMgr in order to deal with the records in a page of the file. 
For that, we will need the structures of previous assignments RM_PageHandle and RM_BufferPool. 
We also store the number of tuples in the table, the record size and finally the queue of free slots.

The queue of free slots is a queue implemented as a linked list where each element store a RID to a free slot and a
pointer to the next element in the queue.

### Initializing record manager
First initialize the record manager together with the storageManager of assignment 1.

### Table methods
After initializing, we create a table with the name and the schema of it and place it in the first page of the file. 
We will create a new file for each of the tables generated. 
After creating it, there is also a method to open it, in which the information of that first page will be stored in the RM_TableData structure (rel). 
This structure, stores the name of the table and the schema, and then another variable where the record manager structure will also be stored.

We also have methods to close the table (which places back all the information onto the first page of the file in order not to lose changes) and a method to delete the table also, destroying the file associated with it.

### Record methods
The available methods are for creating, getting, inserting, deleting and updating a record. 

The createRecord method, just allocates memory and initializes the record without saving it on the table. 

The insert method is the one which will deal with the file and its pages to store the record in the first available free space (checked from the RM_freeRecords structure). 
If no free slot is available then the record is placed after the last one.

In order to know in a page, if a record has been inserted or deleted the following nomenclature will be used: '+' for inserted record and '-' for free records, in the first char position of the record.

Get record method returns the record placed in a table for a given page and slot. Delete, deletes the record of a table in a given page and position, marking it with the mentioned '-' and updating the free queue information.

Finally, the update method, changes the information that already exists of a record.

Because we store '+' or '-' at the beginning of the record, the effective size that a record will take in the table is recordSize + 1.

The getAttr and setAttr method are highly inspired by what is done in the serializer. It is based on the attrOffset method which given an attribute number
and a schema will return the place of the attribute in a record. We then just have to apply whatever we need to do on this attribute.

### Scan methods
In order to keep track of the scanned records, two structures have been defined. RM_ScanMgr has the record ID (RID) of the last scanned record, the condition for scanning and the number of scans done. RM_ScanHandle will store the RM_TableData structure and the RM_ScanMgr.

The method startScan initialized the scan manager in the first position of the file (page 1 slot 0) and the condition used by the user. 

In addition, the next method will return the record that satisfies the condition. 

This method will read each record from the last scanned one to the first one that satisfied the condition.

When the scanning is done (because it reaches the end of the file or because no more records satisfy the condition), the next method will return RC_RM_NO_MORE_TUPLES. 
There is also a simple method to close the scan.