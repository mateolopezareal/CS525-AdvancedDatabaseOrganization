all: run_test_assign1

test_assign1: test_assign1.c storage_mgr.c dberror.c
	gcc -o test_assign1 test_assign1.c storage_mgr.c dberror.c

run_test_assign1: test_assign1
	./test_assign1

memory_check_test_assign1: test_assign1
	valgrind --leak-check=full \
             --show-leak-kinds=all \
             --track-origins=yes \
             --verbose \
              ./test_assign1
clean:
	rm -f *.o *.out test_assign1 test_pagefile.bin assignment1.txt