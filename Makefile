.PHONY: lebench mybench_small redis memcached libevent fork_test1 pthread_test1

PARALLEL= -j$(shell nproc)

#PATHS
DIR := ${CURDIR}
GCC_LIB=$(DIR)/gcc-build/x86_64-pc-linux-gnu/libgcc/
LC_DIR=$(DIR)/glibc-build/
CRT_LIB=$(LC_DIR)csu/
C_LIB=$(LC_DIR)libc.a
PTHREAD_LIB=$(LC_DIR)nptl/libpthread.a
RT_LIB=$(LC_DIR)rt/librt.a
MATH_LIB=$(LC_DIR)math/libm.a
CRT_STARTS=$(CRT_LIB)crt1.o $(CRT_LIB)crti.o $(GCC_LIB)crtbeginT.o
CRT_ENDS=$(GCC_LIB)crtend.o $(CRT_LIB)crtn.o
SYS_LIBS=$(GCC_LIB)libgcc.a $(GCC_LIB)libgcc_eh.a

UKL_FLAGS=-ggdb -mno-red-zone -mcmodel=kernel

all: cloneRepos
	make lebench

cloneRepos:
	make gcc-dir
	make glibc-dir
	make min-initrd-dir
	make linux-dir

undefined_sys_hack.o: undefined_sys_hack.c
	gcc -c -o $@ $< -mcmodel=kernel -ggdb -mno-red-zone

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

#FORK_TEST1
fork_test1: undefined_sys_hack.o gcc-build glibc-build
	- rm -rf UKL.a fork_test1.o 
	gcc -c -o fork_test1.o fork_test1.c $(UKL_FLAGS) 
	ld -r -o fork_test1.ukl --allow-multiple-definition $(CRT_STARTS) fork_test1.o \
                --start-group --whole-archive  $(PTHREAD_LIB) \
                $(C_LIB) --no-whole-archive $(SYS_LIBS) --end-group $(CRT_ENDS)
	ar cr UKL.a fork_test1.ukl undefined_sys_hack.o
	objcopy --prefix-symbols=ukl_ UKL.a
	objcopy --redefine-syms=redef_sym_names UKL.a
	- rm -rf linux/vmlinux

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

#PTHREAD_TEST1
pthread_test1: undefined_sys_hack.o gcc-build glibc-build
	- rm -rf UKL.a pthread_test1.o 
	gcc -c -o pthread_test1.o pthread_test1.c $(UKL_FLAGS) 
	ld -r -o pthread_test1.ukl --allow-multiple-definition $(CRT_STARTS) pthread_test1.o \
                --start-group --whole-archive  $(PTHREAD_LIB) \
                $(C_LIB) --no-whole-archive $(SYS_LIBS) --end-group $(CRT_ENDS)
	ar cr UKL.a pthread_test1.ukl undefined_sys_hack.o
	objcopy --prefix-symbols=ukl_ UKL.a
	objcopy --redefine-syms=redef_sym_names UKL.a
	- rm -rf linux/vmlinux

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

#MYBENCH_SMALL
mybench_small: undefined_sys_hack.o gcc-build glibc-build
	- rm -rf UKL.a mybench_small.o 
	gcc -c -o mybench_small.o mybench_small.c $(UKL_FLAGS) -UUSE_VMALLOC -UBYPASS -UUSE_MALLOC \
                -DREF_TEST -UWRITE_TEST -UREAD_TEST -UMMAP_TEST -UMUNMAP_TEST -UPF_TEST -UEPOLL_TEST \
                -USELECT_TEST -UPOLL_TEST
	ld -r -o mybench_small.ukl --allow-multiple-definition $(CRT_STARTS) mybench_small.o \
                --start-group --whole-archive  $(PTHREAD_LIB) \
                $(C_LIB) --no-whole-archive $(SYS_LIBS) --end-group $(CRT_ENDS)
	ar cr UKL.a mybench_small.ukl undefined_sys_hack.o
	objcopy --prefix-symbols=ukl_ UKL.a
	objcopy --redefine-syms=redef_sym_names UKL.a
	- rm -rf linux/vmlinux

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

#LEBench
lebench: undefined_sys_hack.o gcc-build glibc-build
	rm -rf UKL.a
	gcc -c -o OS_Eval.o OS_Eval.c $(UKL_FLAGS)
	ld -r -o lebench.ukl --allow-multiple-definition $(CRT_STARTS) OS_Eval.o \
                --start-group --whole-archive  $(PTHREAD_LIB) \
		$(C_LIB) --no-whole-archive $(SYS_LIBS) --end-group $(CRT_ENDS)
	ar cr UKL.a lebench.ukl undefined_sys_hack.o
	objcopy --prefix-symbols=ukl_ UKL.a
	objcopy --redefine-syms=redef_sym_names UKL.a
	make linux-build

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

redis: gcc-build glibc-build undefined_sys_hack.o
	- rm -rf redis-server.ukl UKL.a
	make distclean -C redis/
	- make MALLOC=libc V=1 CFLAGS='-ggdb -mno-red-zone -mcmodel=kernel' -C redis/
	- rm -rf redis/src/redis-server redis/src/redis-cli redis/src/redis-benchmark
	cd redis/deps/lua/src/ && ld -r -o lua --allow-multiple-definition $(CRT_STARTS) lua.o liblua.a \
		--start-group --whole-archive $(MATH_LIB) $(C_LIB) --no-whole-archive $(SYS_LIBS) \
		--end-group $(CRT_ENDS)
	cd redis/deps/lua/src/ && ld -r -o luac --allow-multiple-definition $(CRT_STARTS) luac.o \
		print.o liblua.a --start-group --whole-archive $(MATH_LIB) $(C_LIB) --no-whole-archive \
		$(SYS_LIBS) --end-group $(CRT_ENDS)
	cd redis/src/ && ld -r -o redis-server --allow-multiple-definition $(CRT_STARTS) \
		adlist.o quicklist.o ae.o \
		anet.o dict.o server.o sds.o zmalloc.o lzf_c.o lzf_d.o pqsort.o zipmap.o sha1.o \
		ziplist.o release.o networking.o util.o object.o db.o replication.o rdb.o t_string.o \
		t_list.o t_set.o t_zset.o t_hash.o config.o aof.o pubsub.o multi.o debug.o sort.o \
		intset.o syncio.o cluster.o crc16.o endianconv.o slowlog.o scripting.o bio.o rio.o \
		rand.o memtest.o crcspeed.o crc64.o bitops.o sentinel.o notify.o setproctitle.o \
		blocked.o hyperloglog.o latency.o sparkline.o redis-check-rdb.o redis-check-aof.o \
		geo.o lazyfree.o module.o evict.o expire.o geohash.o geohash_helper.o childinfo.o \
		defrag.o siphash.o rax.o t_stream.o listpack.o localtime.o lolwut.o lolwut5.o \
		lolwut6.o acl.o gopher.o tracking.o connection.o tls.o sha256.o timeout.o setcpuaffinity.o \
		monotonic.o mt19937-64.o ../deps/hiredis/libhiredis.a ../deps/lua/src/liblua.a \
		--start-group --whole-archive $(MATH_LIB) $(RT_LIB) $(PTHREAD_LIB) $(C_LIB) --no-whole-archive \
		$(SYS_LIBS) --end-group $(CRT_ENDS)
	cp redis/src/redis-server redis-server.ukl
	ar cr UKL.a redis-server.ukl undefined_sys_hack.o
	objcopy --prefix-symbols=ukl_ UKL.a
	objcopy --redefine-syms=redef_sym_names UKL.a

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

#libeevnt
libevent:
	- make clean -C libevent/
	cd libevent && ./autogen.sh && ./configure --disable-shared CFLAGS='-ggdb -mno-red-zone -mcmodel=kernel' \
		&& make --trace |& tee ../elog
	- rm -rf mylibevent
	ar cr mylibevent libevent/*.o

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

#memcached
memcached: gcc-build glibc-build undefined_sys_hack.o
	- rm -rf memcached.ukl UKL.a memcached/memcached.ukl
	- make clean -C memcached/
	cd memcached && ./autogen.sh && ./configure CFLAGS='-ggdb -mno-red-zone -mcmodel=kernel'
	make -C memcached --trace |& tee mlog
	cd memcached && ld -r -o memcached.ukl --allow-multiple-definition $(CRT_STARTS) \
		memcached-memcached.o memcached-hash.o \
		memcached-jenkins_hash.o memcached-murmur3_hash.o memcached-slabs.o memcached-items.o \
		memcached-assoc.o memcached-thread.o memcached-daemon.o memcached-stats_prefix.o \
		memcached-util.o memcached-cache.o memcached-bipbuffer.o memcached-logger.o \
		memcached-crawler.o memcached-itoa_ljust.o memcached-slab_automove.o memcached-authfile.o \
		memcached-restart.o memcached-extstore.o \
		memcached-crc32c.o memcached-storage.o memcached-slab_automove_extstore.o \
		--start-group ../mylibevent --whole-archive $(PTHREAD_LIB) $(C_LIB) --no-whole-archive \
                $(SYS_LIBS) --end-group $(CRT_ENDS)
	cp memcached/memcached.ukl .
	ar cr UKL.a memcached.ukl undefined_sys_hack.o
	objcopy --prefix-symbols=ukl_ UKL.a
	objcopy --redefine-syms=redef_sym_names UKL.a

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

#LINUX
linux-dir:
	git clone git@github.com:unikernelLinux/Linux-Configs.git
	git clone git@github.com:torvalds/linux.git
	cp Linux-Configs/ukl/golden_config-5.7-broadcom linux/.config
	#make -C linux listnewconfig


linux-clean:
	make distclean -C linux/
	cp saveconfig linux/.config
	make -C linux menuconfig
	cat linux/.config

linux-build:
	- rm -rf linux/vmlinux
	make -C linux $(PARALLEL) |& tee out

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

#MIN_INITRD
min-initrd-dir:
	git clone git@github.com:unikernelLinux/min-initrd.git

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

#GCC
gcc-dir:
	git clone --depth 1 --branch releases/gcc-9.3.0 'https://github.com/gcc-mirror/gcc.git'
	cd ./gcc; ./contrib/download_prerequisites

gcc-build:
	- mkdir $@
	- mkdir gcc-install
	cd $@; \
	  TARGET=x86_64-elf ../gcc/configure --target=$(TARGET) \
	  --disable-nls --enable-languages=c,c++ --without-headers \
	  --prefix=/home/tommyu/localInstall/gcc-install/ --with-multilib-list=m64 --disable-multilib
	make -C $@ all-gcc $(PARALLEL)
	- make -C $@ all-target-libgcc CFLAGS_FOR_TARGET='-ggdb -O2 -mno-red-zone -mcmodel=kernel' $(PARALLEL)
	- make -C $@ all-target-libgcc CFLAGS_FOR_TARGET='-gggdb -O2 -mno-red-zone -mcmodel=kernel'
	sed -i 's/PICFLAG/DISABLED_PICFLAG/g' gcc-build/x86_64-pc-linux-gnu/libgcc/Makefile
	- make -C $@ all-target-libgcc CFLAGS_FOR_TARGET='-ggdb -O2 -mcmodel=kernel -mno-red-zone'

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

#GLIBC
glibc-dir:
	git clone --depth 1 --branch ukl git@github.com:unikernelLinux/glibc.git

glibc-build:
	./cleanbuild.sh

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

#RUN
run:
	make runU -C min-initrd

debug:
	make debugU -C min-initrd

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

#MOUNT/UNMOUNT
mnt:
	mount min-initrd/min-initrd.d/root mntpt

umnt:
	umount mntpt
#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

#CLEAN
glibc-clean:
	rm -rf glibc-build

gcc-clean:
	rm -rf gcc-build

dist-clean: glibc-clean gcc-clean
	rm -rf gcc glibc 
