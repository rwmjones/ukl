all: getppid_info

GLIBC_CRT_PATH=../../build-glibc/glibc-build/csu/

# Need to build on this machine.
LIBGCC_PATH=../../gcc-build/x86_64-pc-linux-gnu/libgcc/

# XXX TODO re-enable this
CRT_STARTS= $(GLIBC_CRT_PATH)/crt1.o $(GLIBC_CRT_PATH)/crti.o $(LIBGCC_PATH)/crtbeginT.o

CRT_ENDS=  $(LIBGCC_PATH)/crtend.o $(GLIBC_CRT_PATH)/crtn.o

# GLIBC_PATH=/root/unikernel/build-glibc/glibc-build
GLIBC_PATH=../../build-glibc/glibc-build/

GLIBC=$(GLIBC_PATH)/libc.a

MY_LD_FLAGS= --allow-multiple-definition --defsym=__pthread_initialize_minimal=__pthread_initialize_minimal_internal --static

LD_DEBUG=--verbose -Map=getppid.map

MY_LD_FLAGS+= $(LD_DEBUG)

LOG_OUT= > linker_verbose.txt #2> (tee linker_err.txt >&2) #

# This builds the partially linked app library that's ready to
# be finally linked with the modified linux kernel.
GCC_LIBS= $(LIBGCC_PATH)/libgcc.a $(LIBGCC_PATH)/libgcc_eh.a

GLIBC_AUX_LIBS= $(GLIBC_PATH)/nptl/libpthread.a

# I don't understand why we have to include glibc and pthread as whole archive.
getppid_partial.o: getppid.o undefined_sys_hack.o $(CRT_STARTS) $(CRT_ENDS) $(GLIBC_LIBS)
	ld -r -o $@ \
		$(MY_LD_FLAGS) \
		$(CRT_STARTS) \
		$<  \
		--whole-archive $(GLIBC) $(GLIBC_AUX_LIBS) --no-whole-archive \
		--start-group $(GCC_LIBS) --end-group \
		$(CRT_ENDS) \
		$(LOG_OUT)
	ar cr UKL.a getppid_partial.o undefined_sys_hack.o

undefined_sys_hack.o: undefined_sys_hack.c
	gcc -c -o $@ $< -mcmodel=kernel -ggdb -mno-red-zone

getppid.o: getppid.c
# Assemble the app Special flags for running in the kernel.
	gcc --static -c -o getppid.o $< -mcmodel=kernel -ggdb -mno-red-zone --verbose

getppid_info: getppid_partial.o
	nm $< > getppid_all_syms.txt
	nm -u $< > getppid_unresolved_syms.txt
	objdump -d $< > getppid_objdump.txt
	file $< > getppid_file.txt
	md5sum $< > getppid_md5.txt
# Container for linux files after final link is later done.
	mkdir finished_linux_files
	mkdir info
	mv *.txt info
	mv *.map info

clean:
	rm -rf info finished_linux_files
	rm -rf getppid.o getppid_partial.o
	rm -rf fsb.o getppid.map linker_verbose.txt
	rm -rf undefined_sys_hack.o
	rm -rf *.txt UKL.a
