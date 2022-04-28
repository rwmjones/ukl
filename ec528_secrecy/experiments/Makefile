LIB := ../src/libsecrecy.a

all:	exp-equality exp-cmp-swap exp-exchange exp-reveal exp-greater exp-rca exp-distinct exp-join exp-semi-join exp-order-by exp-group-by exp-q1 exp-q2 exp-q3 exp-tpch-q4 exp-tpch-q4-baseline exp-tpch-q13 exp-tpch-q13-baseline exp-qpwd exp-qpwd-baseline exp-qcredit exp-qcredit-baseline

exp-equality:	exp_equality.c $(LIB)
			$(CC) $(CFLAGS) -o exp-equality exp_equality.c $(LIB) $(LDFLAGS)

exp-cmp-swap:	exp_comp_swap.c $(LIB)
			$(CC) $(CFLAGS) -o exp-cmp-swap exp_comp_swap.c $(LIB) $(LDFLAGS)

exp-exchange:	exp_exchange.c $(LIB)
			$(CC) $(CFLAGS) -o exp-exchange exp_exchange.c $(LIB) $(LDFLAGS)

exp-reveal:	exp_reveal.c $(LIB)
			$(CC) $(CFLAGS) -o exp-reveal exp_reveal.c $(LIB) $(LDFLAGS)

exp-greater:	exp_greater.c $(LIB)
			$(CC) $(CFLAGS) -o exp-greater exp_greater.c $(LIB) $(LDFLAGS)

exp-rca:	exp_rca.c $(LIB)
			$(CC) $(CFLAGS) -o exp-rca exp_rca.c $(LIB) $(LDFLAGS)

exp-distinct:	exp_distinct.c $(LIB)
			$(CC) $(CFLAGS) -o exp-distinct exp_distinct.c $(LIB) $(LDFLAGS)

exp-join:	exp_join.c $(LIB)
			$(CC) $(CFLAGS) -o exp-join exp_join.c $(LIB) $(LDFLAGS)

exp-semi-join:	exp_semi_join.c $(LIB)
			$(CC) $(CFLAGS) -o exp-semi-join exp_semi_join.c $(LIB) $(LDFLAGS)

exp-order-by:	exp_order_by.c $(LIB)
			$(CC) $(CFLAGS) -o exp-order-by exp_order_by.c $(LIB) $(LDFLAGS)

exp-group-by:	exp_group_by.c $(LIB)
			$(CC) $(CFLAGS) -o exp-group-by exp_group_by.c $(LIB) $(LDFLAGS)

exp-group-by-baseline:	exp_group_by_baseline.c $(LIB)
			$(CC) $(CFLAGS) -o exp-group-by-baseline exp_group_by_baseline.c $(LIB) $(LDFLAGS)

exp-q1:	exp_q1.c $(LIB)
			$(CC) $(CFLAGS) -o exp-q1 exp_q1.c $(LIB) $(LDFLAGS)

exp-q1-baseline:	exp_q1_baseline.c $(LIB)
			$(CC) $(CFLAGS) -o exp-q1-baseline exp_q1_baseline.c $(LIB) $(LDFLAGS)

exp-q2:	exp_q2.c $(LIB)
			$(CC) $(CFLAGS) -o exp-q2 exp_q2.c $(LIB) $(LDFLAGS)

exp-q2-baseline:	exp_q2_baseline.c $(LIB)
			$(CC) $(CFLAGS) -o exp-q2-baseline exp_q2_baseline.c $(LIB) $(LDFLAGS)

exp-q3:	exp_q3.c $(LIB)
			$(CC) $(CFLAGS) -o exp-q3 exp_q3.c $(LIB) $(LDFLAGS)

exp-q3-baseline:	exp_q3_baseline.c $(LIB)
			$(CC) $(CFLAGS) -o exp-q3-baseline exp_q3_baseline.c $(LIB) $(LDFLAGS)

exp-join-distinct-naive:	exp_join_distinct_naive.c $(LIB)
			$(CC) $(CFLAGS) -o exp-join-distinct-naive exp_join_distinct_naive.c $(LIB) $(LDFLAGS)

exp-join-distinct-opt:	exp_join_distinct_opt.c $(LIB)
			$(CC) $(CFLAGS) -o exp-join-distinct-opt exp_join_distinct_opt.c $(LIB) $(LDFLAGS)

exp-group-by-join-naive:	exp_group_by_join_naive.c $(LIB)
			$(CC) $(CFLAGS) -o exp-group-by-join-naive exp_group_by_join_naive.c $(LIB) $(LDFLAGS)

exp-group-by-join-opt:	exp_group_by_join_opt.c $(LIB)
			$(CC) $(CFLAGS) -o exp-group-by-join-opt exp_group_by_join_opt.c $(LIB) $(LDFLAGS)

exp-select-distinct-opt:	exp_select_distinct_opt.c $(LIB)
			$(CC) $(CFLAGS) -o exp-select-distinct-opt exp_select_distinct_opt.c $(LIB) $(LDFLAGS)

exp-select-distinct-naive:	exp_select_distinct_naive.c $(LIB)
			$(CC) $(CFLAGS) -o exp-select-distinct-naive exp_select_distinct_naive.c $(LIB) $(LDFLAGS)

exp-geq-join:	exp_geq_join.c $(LIB)
			$(CC) $(CFLAGS) -o exp-geq-join exp_geq_join.c $(LIB) $(LDFLAGS)

exp-tpch-q4:	exp_tpch_q4.c $(LIB)
			$(CC) $(CFLAGS) -o exp-tpch-q4 exp_tpch_q4.c $(LIB) $(LDFLAGS)

exp-tpch-q4-baseline:	exp_tpch_q4_baseline.c $(LIB)
			$(CC) $(CFLAGS) -o exp-tpch-q4-baseline exp_tpch_q4_baseline.c $(LIB) $(LDFLAGS)

exp-tpch-q6:	exp_tpch_q6.c $(LIB)
			$(CC) $(CFLAGS) -o exp-tpch-q6 exp_tpch_q6.c $(LIB) $(LDFLAGS)

exp-tpch-q13:	exp_tpch_q13.c $(LIB)
			$(CC) $(CFLAGS) -o exp-tpch-q13 exp_tpch_q13.c $(LIB) $(LDFLAGS)

exp-tpch-q13-baseline:	exp_tpch_q13_baseline.c $(LIB)
			$(CC) $(CFLAGS) -o exp-tpch-q13-baseline exp_tpch_q13_baseline.c $(LIB) $(LDFLAGS)

exp-qpwd:	exp_qpwd.c $(LIB)
			$(CC) $(CFLAGS) -o exp-qpwd exp_qpwd.c $(LIB) $(LDFLAGS)

exp-qpwd-baseline:	exp_qpwd_baseline.c $(LIB)
			$(CC) $(CFLAGS) -o exp-qpwd-baseline exp_qpwd_baseline.c $(LIB) $(LDFLAGS)

exp-qcredit:	exp_qcredit.c $(LIB)
			$(CC) $(CFLAGS) -o exp-qcredit exp_qcredit.c $(LIB) $(LDFLAGS)

exp-qcredit-baseline:	exp_qcredit_baseline.c $(LIB)
			$(CC) $(CFLAGS) -o exp-qcredit-baseline exp_qcredit_baseline.c $(LIB) $(LDFLAGS)

exp-tpch-q6-baseline:	exp_tpch_q6_baseline.c $(LIB)
			$(CC) $(CFLAGS) -o exp-tpch-q6-baseline exp_tpch_q6_baseline.c $(LIB) $(LDFLAGS)

clean:
			rm -f exp-equality exp-cmp-swap exp-exchange exp-reveal exp-greater exp-rca exp-distinct exp-join exp-semi-join exp-order-by exp-group-by exp-q1 exp-q2 exp-q3 exp-tpch-q4 exp-tpch-q4-baseline exp-tpch-q13 exp-tpch-q13-baseline exp-qpwd exp-qpwd-baseline exp-qcredit exp-qcredit-baseline exp-group-by-join-naive *.o *.exec
