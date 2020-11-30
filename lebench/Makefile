all: lebench_info

# GLIBC_CRT_PATH=/root/MPC-UKL/C-Constructor/helpers/
# GLIBC_CRT_PATH=/root//unikernel/build-glibc/glibc-build/csu
GLIBC_CRT_PATH=../../build-glibc/glibc-build/csu/

# Need to build on this machine.
# LIBGCC_PATH=/root/MPC-UKL/C-Constructor/helpers
# LIBGCC_PATH=../../gcc-build-cpp/x86_64-pc-linux-gnu/libgcc/
LIBGCC_PATH=../../gcc-build/x86_64-pc-linux-gnu/libgcc/

# XXX TODO re-enable this
CRT_STARTS= $(GLIBC_CRT_PATH)/crt1.o $(GLIBC_CRT_PATH)/crti.o $(LIBGCC_PATH)/crtbeginT.o

CRT_ENDS=  $(LIBGCC_PATH)/crtend.o $(GLIBC_CRT_PATH)/crtn.o

# GLIBC_PATH=/root/unikernel/build-glibc/glibc-build
GLIBC_PATH=../../build-glibc/glibc-build/

GLIBC=$(GLIBC_PATH)/libc.a

MY_LD_FLAGS= --allow-multiple-definition --defsym=__pthread_initialize_minimal=__pthread_initialize_minimal_internal --static

LD_DEBUG=--verbose -Map=lebench.map

MY_LD_FLAGS+= $(LD_DEBUG)

LOG_OUT= > linker_verbose.txt #2> (tee linker_err.txt >&2) #

# UKL_MISC= ../fsbringup.o

# LINUX_LIBS=arch/x86/kernel/head_64.o arch/x86/kernel/head64.o arch/x86/kernel/ebda.o arch/x86/kernel/platform-quirks.o init/built-in.a usr/built-in.a arch/x86/built-in.a kernel/built-in.a certs/built-in.a mm/built-in.a fs/built-in.a ipc/built-in.a security/built-in.a crypto/built-in.a block/built-in.a lib/built-in.a arch/x86/lib/built-in.a lib/lib.a arch/x86/lib/lib.a drivers/built-in.a sound/built-in.a arch/x86/pci/built-in.a arch/x86/power/built-in.a arch/x86/video/built-in.a net/built-in.a virt/built-in.a

# LINUX_SYMS=../../linux/.tmp_vmlinux.kallsyms2.o

# LL_FULL_PATH=$(addprefix ../../linux/, $(LINUX_LIBS))

# resolve_with_linux.o: lebench_partial.o
# 	 ld -r -o $@ $(MY_LD_FLAGS) $< \
# 		--start-group  $(LL_FULL_PATH) --end-group $(LINUX_SYMS)

# This builds the partially linked app library that's ready to
# be finally linked with the modified linux kernel.
GCC_LIBS= $(LIBGCC_PATH)/libgcc.a $(LIBGCC_PATH)/libgcc_eh.a

GLIBC_AUX_LIBS= $(GLIBC_PATH)/nptl/libpthread.a

# I don't understand why we have to include glibc and pthread as whole archive.
lebench_partial.o: lebench.o undefined_sys_hack.o $(CRT_STARTS) $(CRT_ENDS) $(GLIBC_LIBS)
	ld -r -o $@ \
		$(MY_LD_FLAGS) \
		$(CRT_STARTS) \
		$<  \
		--whole-archive $(GLIBC) $(GLIBC_AUX_LIBS) --no-whole-archive \
		--start-group $(GCC_LIBS) --end-group \
		$(CRT_ENDS) \
		$(LOG_OUT)
	ar cr UKL.a lebench_partial.o undefined_sys_hack.o

# lebench_partial.o: lebench.o undefined_sys_hack.o $(CRT_STARTS) $(CRT_ENDS) $(GLIBC_LIBS)
# 	ld -r -o $@ \
# 		$(MY_LD_FLAGS) \
# 		$(CRT_STARTS) \
# 		$<  $(GLIBC_AUX_LIBS) \
# 		--whole-archive $(GLIBC) --no-whole-archive \
# 		--start-group $(GCC_LIBS) --end-group \
# 		$(CRT_ENDS) \
# 		$(LOG_OUT)
# 	ar cr UKL.a lebench_partial.o undefined_sys_hack.o

# Why doesnt this work?
# lebench_partial.o: lebench.o undefined_sys_hack.o $(CRT_STARTS) $(CRT_ENDS) $(GLIBC_LIBS)
# 	ld -r -o $@ \
# 		$(MY_LD_FLAGS) \
# 		$(CRT_STARTS) \
# 		$<  \
# 		$(GLIBC_AUX_LIBS) \
# 		--start-group $(GCC_LIBS) $(GLIBC) --end-group \
# 		$(CRT_ENDS) \
# 		$(LOG_OUT)
# 	ar cr UKL.a lebench_partial.o undefined_sys_hack.o

# lebench_partial_normal.o

undefined_sys_hack.o: undefined_sys_hack.c
	gcc -c -o $@ $< -mcmodel=kernel -ggdb -mno-red-zone

lebench.o: lebench.c
# Assemble the app Special flags for running in the kernel.
	gcc --static -c -o lebench.o $< -mcmodel=kernel -ggdb -mno-red-zone --verbose

lebench_info: lebench_partial.o
	nm $< > lebench_all_syms.txt
	nm -u $< > lebench_unresolved_syms.txt
	objdump -d $< > lebench_objdump.txt
	file $< > lebench_file.txt
	md5sum $< > lebench_md5.txt
# Container for linux files after final link is later done.
	mkdir finished_linux_files
	mkdir info
	mv *.txt info
	mv *.map info

clean:
	rm -rf info finished_linux_files
	rm -rf lebench.o lebench_partial.o
	rm -rf fsb.o lebench.map linker_verbose.txt
	rm -rf undefined_sys_hack.o
	rm -rf *.txt UKL.a
