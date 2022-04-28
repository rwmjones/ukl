LIB := ../src/libsecrecy.a

.PHONY: all clean

# Nothing in this directory actually compiles
#all:	comorbidity join-distinct
all:

comorbidity:	comorbidity.c $(LIB)
			$(CC) $(CFLAGS) -o comorbidity comorbidity.c $(LIB) $(LDFLAGS)

join-distinct:	join_distinct.c $(LIB)
			$(CC) $(CFLAGS) -o join-distinct join_distinct.c $(LIB) $(LDFLAGS)

clean:
			rm -f comorbidity join-distinct *.o *.exec
