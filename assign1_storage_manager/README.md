# CS525 Coding assignment 1 : Storage manager
## Group
|Student name| iit mail|
|---|---|
|Decou Nathan|[ndecou@hawk.iit.edu](mailto:ndecou@hawk.iit.edu)|
|Fernandez	Mateo|[mfernandezlopezareal@hawk.iit.edu](mailto:mfernandezlopezareal@hawk.iit.edu)|
|Santana	Mikel|[msantanamarana@hawk.iit.edu](mailto:msantanamarana@hawk.iit.edu)|

## How to compile and run
- To compile the test file use the Makefile rule `make test_assign1`
- To run the tests use the Makefile rule `make run_test_assign1`. 
> :arrow_up: This rule will compile **and** run the tests. No need to use `make test_assign1` before.
- Use `make memory_check_test_assign1` to check for memory leaks. 
> :arrow_up: This rule will compile **and** run the tests using Valgrind. No need to use `make test_assign1` before.
- To clean (i.e. remove binary files, temporary files etc.) use `make clean`

## Added tests scenarios
We have added a method in the main to test an scenario. This methos does the following things:
- Create and open a file named "assignment1.txt".
- Ensured the capacity of the file to 4 pages.
- Read the next block from the cursor (in this case as we opened the file, it's the second block).
- Fill a block with 'a' character and write it in the third block.
- Close the file and free the memory
The file assignment.txt can be found in the directory to see how the test scenario worked.

## Code explanation
### Initialization
Initialization simply consists of checking if the program can write in the folder. If not we exit the program.

### Structure of the file
In order to know the total number of pages in the file we decided to store this number in the beginning of the first page.
This page is "reserved", which means the user's data will not be written on it but on the pages after it.

We chose to reserve an entire page and not just the number of bits to store an int. 
This is so that if we need to store other things in the future we can without having to redo everything.

The figure below shows this structure for a file with 3 accessible pages.

![structure of a file](img/file_diagram.png)

### What's in `void *mgmtInfo`
When a file is opened the pointer to the opened stream is stored in `mgmtInfo`.

Thanks to this pointer we are able to access the file at any moment given the associated `SM_FileHandle`. 

This is used in the `closePageFile`method.

### Read and write block
All the `read[?]Block` (e.g `readFirstBlock`) methods uses the `readBlock` method with the block number chosen accordingly.
Same technique is used for `writeCurrentBlock` using `writeBlock` with the block number equals the result of the `getBlockPos` method.

### Ensure capacity
The `ensureCapacity` method will call the `appendEmptyBlock` until the file contains the desired number of page.
The current block position is the same before and after calling this method.
