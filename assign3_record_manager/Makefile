all: run_test_assign3

test_assign3: test_assign3_1.c ../assign1_storage_manager/storage_mgr.c dberror.c ../assign2_buffer_manager/buffer_mgr.c buffer_mgr_stat.c expr.c rm_serializer.c record_mgr.c
	gcc -g -lm -o test_assign3_1 test_assign3_1.c ../assign1_storage_manager/storage_mgr.c dberror.c ../assign2_buffer_manager/buffer_mgr.c buffer_mgr_stat.c expr.c rm_serializer.c record_mgr.c

run_test_assign3_1: test_assign3
	./test_assign3_1

memory_check_test_assign3_1: test_assign3
	valgrind --leak-check=full \
             --show-leak-kinds=all \
             --track-origins=yes \
             --verbose \
              ./test_assign3_1 > /dev/null

test_assign3_V2: test_assign3_1_V2.c ../assign1_storage_manager/storage_mgr.c dberror.c ../assign2_buffer_manager/buffer_mgr.c buffer_mgr_stat.c expr.c rm_serializer.c record_mgr.c
	gcc -g -lm -o test_assign3_1_V2 test_assign3_1_V2.c ../assign1_storage_manager/storage_mgr.c dberror.c ../assign2_buffer_manager/buffer_mgr.c buffer_mgr_stat.c expr.c rm_serializer.c record_mgr.c

run_test_assign3_1_V2: test_assign3_V2
	./test_assign3_1_V2

memory_check_test_assign3_1_V2: test_assign3_V2
	valgrind --leak-check=full \
             --show-leak-kinds=all \
             --track-origins=yes \
             --verbose \
              ./test_assign3_1_V2 > /dev/null
clean:
	rm -f *.o *.out test_assign3_1 test_assign3_1_V2 test_table_t
