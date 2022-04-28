LIB := ../src/libsecrecy.a

all: test-conversion test-primitives test-rsz test-share test-select test-rca test-and-all test-equality test-relational test-group test-distinct test-sort test-inequality test-join test-in test-mask test-arithmetic-to-binary test-ageq test-group-join test-group-min-max test-q1 test-q2 test-q3 test-tpch-q4 test-tpch-q4-baseline test-tpch-q13 test-tpch-q13-baseline test-qpwd test-qpwd-baseline test-qcredit test-qcredit-baseline

run_tests:
		./tests.sh

test-init: test_init.c $(LIB)
			$(CC) $(CFLAGS) -o test-init test_init.c $(LIB) $(LDFLAGS)

test-conn: test_conn.c $(LIB)
			$(CC) $(CFLAGS) -o test-conn test_conn.c $(LIB) $(LDFLAGS)

test-primitives: test_primitives.c $(LIB)
			$(CC) $(CFLAGS) -o test-primitives test_primitives.c $(LIB) $(LDFLAGS)

test-share: test_sharing.c $(LIB)
			$(CC) $(CFLAGS) -o test-share test_sharing.c $(LIB) $(LDFLAGS)

test-query: test_simple_query.c $(LIB)
			$(CC) $(CFLAGS) -o test-query test_simple_query.c $(LIB) $(LDFLAGS)

test-rsz: 	test_rsz.c $(LIB)
			$(CC) $(CFLAGS) -o test-rsz test_rsz.c $(LIB) $(LDFLAGS)

test-and-all:	test_and_all.c $(LIB)
			$(CC) $(CFLAGS) -o test-and-all test_and_all.c $(LIB) $(LDFLAGS)

test-equality:	test_binary_equality.c $(LIB)
			$(CC) $(CFLAGS) -o test-equality test_binary_equality.c $(LIB) $(LDFLAGS)

test-inequality:	test_binary_inequality.c $(LIB)
			$(CC) $(CFLAGS) -o test-inequality test_binary_inequality.c $(LIB) $(LDFLAGS)

test-rca:	test_rca.c $(LIB)
			$(CC) $(CFLAGS) -o test-rca test_rca.c $(LIB) $(LDFLAGS)

test-join:	test_join.c $(LIB)
			$(CC) $(CFLAGS) -o test-join test_join.c $(LIB) $(LDFLAGS)

test-batched-join:	test_batched_join.c $(LIB)
			$(CC) $(CFLAGS) -o test-batched-join test_batched_join.c $(LIB) $(LDFLAGS)

test-select:	test_select.c $(LIB)
			$(CC) $(CFLAGS) -o test-select test_select.c $(LIB) $(LDFLAGS)

test-relational:	test_relational.c $(LIB)
			$(CC) $(CFLAGS) -o test-relational test_relational.c $(LIB) $(LDFLAGS)

test-conversion:	test_conversion.c $(LIB)
			$(CC) $(CFLAGS) -o test-conversion test_conversion.c $(LIB) $(LDFLAGS)

test-arithmetic-to-binary:	test_arithmetic_to_binary.c $(LIB)
			$(CC) $(CFLAGS) -o test-arithmetic-to-binary test_arithmetic_to_binary.c $(LIB) $(LDFLAGS)

test-sort:	test_sort.c $(LIB)
			$(CC) $(CFLAGS) -o test-sort test_sort.c $(LIB) $(LDFLAGS)

test-distinct:	test_distinct.c $(LIB)
			$(CC) $(CFLAGS) -o test-distinct test_distinct.c $(LIB) $(LDFLAGS)

test-group:	test_group_by.c $(LIB)
			$(CC) $(CFLAGS) -o test-group test_group_by.c $(LIB) $(LDFLAGS)

test-group-join:	test_group_by_join.c $(LIB)
			$(CC) $(CFLAGS) -o test-group-join test_group_by_join.c $(LIB) $(LDFLAGS)

test-group-min-max:	test_group_by_min_max.c $(LIB)
			$(CC) $(CFLAGS) -o test-group-min-max test_group_by_min_max.c $(LIB) $(LDFLAGS)

test-in:	test_in.c $(LIB)
			$(CC) $(CFLAGS) -o test-in test_in.c $(LIB) $(LDFLAGS)

test-mask:	test_mask.c $(LIB)
			$(CC) $(CFLAGS) -o test-mask test_mask.c $(LIB) $(LDFLAGS)

test-ageq:	test_adjacent_geq.c $(LIB)
			$(CC) $(CFLAGS) -o test-ageq test_adjacent_geq.c $(LIB) $(LDFLAGS)

test-q1:	test_q1.c $(LIB)
			$(CC) $(CFLAGS) -o test-q1 test_q1.c $(LIB) $(LDFLAGS)

test-q2:	test_q2.c $(LIB)
			$(CC) $(CFLAGS) -o test-q2 test_q2.c $(LIB) $(LDFLAGS)

test-q3:	test_q3.c $(LIB)
			$(CC) $(CFLAGS) -o test-q3 test_q3.c $(LIB) $(LDFLAGS)

test-tpch-q4:	test_tpch_q4.c $(LIB)
			$(CC) $(CFLAGS) -o test-tpch-q4 test_tpch_q4.c $(LIB) $(LDFLAGS)

test-tpch-q4-baseline:	test_tpch_q4_baseline.c $(LIB)
			$(CC) $(CFLAGS) -o test-tpch-q4-baseline test_tpch_q4_baseline.c $(LIB) $(LDFLAGS)

test-tpch-q6:	test_tpch_q6.c $(LIB)
			$(CC) $(CFLAGS) -o test-tpch-q6 test_tpch_q6.c $(LIB) $(LDFLAGS)

test-tpch-q13:	test_tpch_q13.c $(LIB)
			$(CC) $(CFLAGS) -o test-tpch-q13 test_tpch_q13.c $(LIB) $(LDFLAGS)

test-tpch-q13-baseline:	test_tpch_q13_baseline.c $(LIB)
			$(CC) $(CFLAGS) -o test-tpch-q13-baseline test_tpch_q13_baseline.c $(LIB) $(LDFLAGS)

test-tpch-q6-baseline:	test_tpch_q6_baseline.c $(LIB)
			$(CC) $(CFLAGS) -o test-tpch-q6-baseline test_tpch_q6_baseline.c $(LIB) $(LDFLAGS)

test-qpwd:	test_qpwd.c $(LIB)
			$(CC) $(CFLAGS) -o test-qpwd test_qpwd.c $(LIB) $(LDFLAGS)

test-qpwd-baseline:	test_qpwd_baseline.c $(LIB)
			$(CC) $(CFLAGS) -o test-qpwd-baseline test_qpwd_baseline.c $(LIB) $(LDFLAGS)

test-qcredit:	test_qcredit.c $(LIB)
			$(CC) $(CFLAGS) -o test-qcredit test_qcredit.c $(LIB) $(LDFLAGS)

test-qcredit-baseline:	test_qcredit_baseline.c $(LIB)
			$(CC) $(CFLAGS) -o test-qcredit-baseline test_qcredit_baseline.c $(LIB) $(LDFLAGS)

test-temp:	test-temp.c $(LIB)
			$(CC) $(CFLAGS) -o test-temp test-temp.c $(LIB) $(LDFLAGS)

test-party:	test_party.c $(LIB)
			$(CC) $(CFLAGS) -o test-party test_party.c $(LIB) $(LDFLAGS)

test-async:	test_async.c $(LIB)
			$(CC) $(CFLAGS) -o test-async test_async.c $(LIB) $(LDFLAGS)

clean:
			rm -f test-init test-conn test-tpch-q6 test-conversion test-primitives test-rsz test-share test-select test-rca test-and-all test-equality test-relational test-group test-distinct test-sort test-inequality test-join test-in test-mask test-arithmetic-to-binary test-ageq test-group-join test-group-min-max test-q1 test-q2 test-q3 test-tpch-q4 test-tpch-q4-baseline test-tpch-q13 test-tpch-q13-baseline test-qpwd test-qpwd-baseline test-qcredit test-qcredit-baseline test-party test-async *.o *.exec
