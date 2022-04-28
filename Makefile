.PHONY: redis

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
CPP_LIB=$(DIR)/gcc-build-cpp/x86_64-pc-linux-gnu/libstdc++-v3/src/.libs/libstdc++.a

UKL_FLAGS=-ggdb -mno-red-zone -mcmodel=kernel

redis: cloneRepos
	./build_ukl.sh 0 redis

cloneRepos:
	make gcc-dir
	make gpp-dir
	make glibc-dir
	make min-initrd-dir
	make all -C min-initrd/
	make linux-dir
	make new_lebench-dir
	make gapbs_dir

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

#Example Projects
new_lebench-dir:
	git clone git@github.com:unikernelLinux/new_lebench.git

gapbs-dir:
	git clone -b ukl git@github.com:unikernelLinux/gapbs.git

libsodium-1.0.18:
	curl https://download.libsodium.org/libsodium/releases/libsodium-1.0.18.tar.gz --output libsodium-1.0.18.tar.gz
	tar xf libsodium-1.0.18.tar.gz

libsodium.a: libsodium-1.0.18
	cd libsodium-1.0.18 && ./configure CFLAGS='-ggdb -O2 -mno-red-zone -mcmodel=kernel' \
		--enable-static --disable-shared
	make -C libsodium-1.0.18 $(PARALLEL)
	cp libsodium-1.0.18/src/libsodium/.libs/libsodium.a .

secrecy: libsodium.a
	-rm -f secrecy.ukl
	-rm -f UKL.a
	cd ec528_secrecy/src && CFLAGS='-ggdb -O3 -mno-red-zone -mcmodel=kernel -std=c99' make \
		$(PARALLEL) libsecrecy.a
	cp ec528_secrecy/src/libsecrecy.a .
	cd ec528_secrecy/experiments && gcc -o ../../exp_group_by.o -c -ggdb -O3 -mno-red-zone \
		-mcmodel=kernel -std=c99 exp_group_by.c
	ld -r -o secrecy.ukl --allow-multiple-definition $(CRT_STARTS) \
		exp_group_by.o --start-group --whole-archive libsecrecy.a libsodium.a \
		$(RT_LIB) $(PTHREAD_LIB) $(MATH_LIB) $(C_LIB) --no-whole-archive \
		$(SYS_LIBS) --end-group $(CRT_ENDS)
	gcc -c -o undefined_sys_hack.o undefined_sys_hack.c -mcmodel=kernel -ggdb -mno-red-zone
	ar cr UKL.a secrecy.ukl undefined_sys_hack.o
	objcopy --prefix-symbols=ukl_ UKL.a
	objcopy --redefine-syms=redef_sym_names UKL.a

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

#LINUX
linux-dir:
	git clone --depth 1 --branch ukl-main-5.14 git@github.com:unikernelLinux/linux.git

linux-clean:
	make distclean -C linux/
	cp saveconfig linux/.config
	make -C linux menuconfig
	cat linux/.config |& tee -a log_build

linux-build:
	- rm -rf linux/vmlinux
	make -C linux $(PARALLEL) |& tee out

linux-copy:
	cp linux/arch/x86/boot/bzImage /var/lib/tftpboot/ukl/

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
	- make -C $@ all-target-libgcc CFLAGS_FOR_TARGET='-ggdb -O2 -mno-red-zone -mcmodel=kernel'
	sed -i 's/PICFLAG/DISABLED_PICFLAG/g' gcc-build/x86_64-pc-linux-gnu/libgcc/Makefile
	- make -C $@ all-target-libgcc CFLAGS_FOR_TARGET='-ggdb -O2 -mcmodel=kernel -mno-red-zone'

gpp-dir:
	git clone --depth 1 --branch releases/gcc-9.3.0 'https://github.com/gcc-mirror/gcc.git' gpp
	cd ./gpp; ./contrib/download_prerequisites

gcc-build-cpp:
	- mkdir $@
	- mkdir gcc-install
	cd $@; \
	  TARGET=x86_64-elf ../gpp/configure --target=$(TARGET) \
	  --disable-shared --disable-nls --enable-languages=c,c++ --without-headers \
	  --prefix=/home/tommyu/localInstall/gcc-install/ \
	  --with-multilib-list=m64 --disable-multilib
	make -C $@ all-gcc $(PARALLEL)
	- make -C $@ all-target-libgcc CFLAGS_FOR_TARGET='-ggdb -O2 -mno-red-zone -mcmodel=kernel' $(PARALLEL)
	- make -C $@ all-target-libgcc CFLAGS_FOR_TARGET='-ggdb -O2 -mno-red-zone -mcmodel=kernel'
	- make -C $@ all-target-libstdc++-v3 CFLAGS_FOR_TARGET='-ggdb -O2 -mcmodel=kernel -mno-red-zone' \
		CXXFLAGS_FOR_TARGET='-ggdb -O2 -mcmodel=kernel -mno-red-zone' $(PARALLEL)
	- make -C $@ all-target-libstdc++-v3 CFLAGS_FOR_TARGET='-ggdb -O2 -mcmodel=kernel -mno-red-zone' \
		CXXFLAGS_FOR_TARGET='-ggdb -O2 -mcmodel=kernel -mno-red-zone'
	sed -i 's/PICFLAG/DISABLED_PICFLAG/g' $@/x86_64-pc-linux-gnu/libgcc/Makefile
	make -C $@ all-target-libgcc CFLAGS_FOR_TARGET='-ggdb -O2 -mcmodel=kernel -mno-red-zone'
	make -C $@ all-target-libstdc++-v3 CFLAGS_FOR_TARGET='-ggdb -O2 -mcmodel=kernel -mno-red-zone' \
		CXXFLAGS_FOR_TARGET='-ggdb -O2 -mcmodel=kernel -mno-red-zone'

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

run1:
	make runU1 -C min-initrd
run2:
	make runU2 -C min-initrd
run3:
	make runU3 -C min-initrd
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
	- rm -rf glibc-build

gcc-clean:
	- rm -rf gcc-build
	- rm -rf gcc-build-cpp

dist-clean: glibc-clean gcc-clean
	- rm -rf gcc glibc 
