all: run_test_assign4

test_assign4: test_assign4_1.c ../assign1_storage_manager/storage_mgr.c dberror.c ../assign2_buffer_manager/buffer_mgr.c ../assign3_record_manager/record_mgr.c buffer_mgr_stat.c expr.c rm_serializer.c btree_mgr.c
	gcc -g -o test_assign4_1 test_assign4_1.c ../assign1_storage_manager/storage_mgr.c dberror.c ../assign2_buffer_manager/buffer_mgr.c ../assign3_record_manager/record_mgr.c buffer_mgr_stat.c expr.c rm_serializer.c btree_mgr.c -lm

run_test_assign4_1: test_assign4
	./test_assign4_1

memory_check_test_assign4_1: test_assign4
	valgrind --leak-check=full \
             --show-leak-kinds=all \
             --track-origins=yes \
             --verbose \
              ./test_assign4_1 > /dev/null
              
clean:
	rm -f *.o *.out test_assign4_1
