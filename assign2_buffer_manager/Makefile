all: run_test_assign2_1

test_assign2_1: test_assign2_1.c ../assign1_storage_manager/storage_mgr.c dberror.c buffer_mgr.c buffer_mgr_stat.c
	gcc -o test_assign2_1 test_assign2_1.c ../assign1_storage_manager/storage_mgr.c dberror.c buffer_mgr.c buffer_mgr_stat.c

run_test_assign2_1: test_assign2_1
	./test_assign2_1

memory_check_test_assign2_1: test_assign2_1
	valgrind --leak-check=full \
             --show-leak-kinds=all \
             --track-origins=yes \
             --verbose \
              ./test_assign2_1
clean:
	rm -f *.o *.out test_assign2_1