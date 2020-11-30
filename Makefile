
.PHONY: glibc cj utb4 barriercheck client lebench

glibc:
	./extractglibc.sh
	rm -rf UKL.a
	ld -r -o glibcfinal --unresolved-symbols=ignore-all --allow-multiple-definition --whole-archive crti.o libc.a libpthread.a start.o crtn.o --no-whole-archive

fstest: glibc
	gcc -c -o fsbringup.o fsbringup.c -mcmodel=kernel -ggdb
	ld -r -o fsfinal.o --unresolved-symbols=ignore-all --allow-multiple-definition fsbringup.o --start-group glibcfinal --end-group 
	ar cr UKL.a fsfinal.o
	rm -rf ../linux/vmlinux 
	make -C ../linux -j$(shell nproc)

memmoveCheck: glibc
	gcc -c -o memmovecheck.o memmovecheck.c -mcmodel=kernel -ggdb
	make -C ../linux M=$(PWD)
	ld -r -o mmcfinal.o --unresolved-symbols=ignore-all --allow-multiple-definition memmovecheck.o --start-group glibcfinal --end-group 
	ar cr UKL.a ukl.o interface.o mmcfinal.o
	rm -rf *.ko *.mod.* .H* .tm* .*cmd Module.symvers modules.order built-in.a 
	rm -rf ../linux/vmlinux 
	make -C ../linux -j$(shell nproc)

gettimecheck: glibc
	gcc -c -o gettimecheck.o gettimecheck.c -mcmodel=kernel -ggdb
	make -C ../linux M=$(PWD)
	ld -r -o gtfinal.o --unresolved-symbols=ignore-all --allow-multiple-definition gettimecheck.o --start-group glibcfinal --end-group 
	ar cr UKL.a ukl.o interface.o gtfinal.o
	rm -rf *.ko *.mod.* .H* .tm* .*cmd Module.symvers modules.order built-in.a 
	rm -rf ../linux/vmlinux 
	make -C ../linux -j$(shell nproc)

# lebench: glibc
# 	gcc -c -o lebench.o OS_Eval.c -mcmodel=kernel -ggdb -mno-red-zone
# 	gcc -c -o fsb.o fsbringup.c -mcmodel=kernel -ggdb -mno-red-zone
# 	ld -r -o lebenchfinal.o --unresolved-symbols=ignore-all --allow-multiple-definition lebench.o --start-group fsb.o glibcfinal --end-group 
# 	ar cr UKL.a lebenchfinal.o
# 	rm -rf ../linux/vmlinux 
# 	make -C ../linux -j$(shell nproc)

lebench: lebench
# Remove all old state
	make -C lebench clean

# Build lebench app lib
	make -C lebench

# Linux link script expects to find UKL.a here.
# cp lebench/lebench_partial.o ./UKL.a
	cp lebench/UKL.a ./UKL.a

# Force rebuild of vmlinux, pulls in UKL.a in link script
	rm -rf ../linux/vmlinux && make -C ../linux -j$(shell nproc)

malloctest: glibc
	gcc -c -o malloctest.o malloctest.c -mcmodel=kernel -ggdb -mno-red-zone
	ld -r -o malloctestfinal.o --unresolved-symbols=ignore-all --allow-multiple-definition malloctest.o --start-group glibcfinal --end-group 
	ar cr UKL.a malloctestfinal.o
	rm -rf ../linux/vmlinux 
	make -C ../linux -j$(shell nproc)

writetest: glibc
	gcc -c -o wt.o write_test.c -mcmodel=kernel -ggdb -mno-red-zone
	make -C ../linux M=$(PWD)
	ld -r -o wtfinal.o --unresolved-symbols=ignore-all --allow-multiple-definition wt.o --start-group glibcfinal --end-group 
	ar cr UKL.a ukl.o interface.o wtfinal.o
	rm -rf *.ko *.mod.* .H* .tm* .*cmd Module.symvers modules.order built-in.a 
	rm -rf ../linux/vmlinux 
	make -C ../linux -j$(shell nproc)

signaltest: glibc
	gcc -c -o signaltest.o sigtest.c -mcmodel=kernel -ggdb -mno-red-zone
	make -C ../linux M=$(PWD)
	ld -r -o signalfinal.o --unresolved-symbols=ignore-all --allow-multiple-definition signaltest.o --start-group glibcfinal --end-group 
	ar cr UKL.a ukl.o interface.o signalfinal.o
	rm -rf *.ko *.mod.* .H* .tm* .*cmd Module.symvers modules.order built-in.a 
	rm -rf ../linux/vmlinux 
	make -C ../linux -j$(shell nproc)

test: glibc
	gcc -c -o ${case}.o ${case}.c -mcmodel=kernel -ggdb -Wno-implicit
	ld -r -o testfinal.o --unresolved-symbols=ignore-all --allow-multiple-definition interface.o ${case}.o --start-group glibcfinal --end-group 
	ar cr UKL.a testfinal.o
	rm -rf ../linux/vmlinux 
	make -C ../linux -j$(shell nproc)

stackextend: glibc
	gcc -c -o stackextend.o stackextend.c -mcmodel=kernel -ggdb -Wno-implicit
	make -C ../linux M=$(PWD)
	ld -r -o sefinal.o --unresolved-symbols=ignore-all --allow-multiple-definition interface.o stackextend.o --start-group glibcfinal --end-group 
	ar cr UKL.a ukl.o sefinal.o
	rm -rf *.ko *.mod.* .H* .tm* .*cmd Module.symvers modules.order built-in.a 
	rm -rf ../linux/vmlinux 
	make -C ../linux -j$(shell nproc)

cj:
	gcc -o cj cj.c -lpthread -ggdb

canceljoin: glibc
	gcc -c -o canceljoin.o canceljoin.c -mcmodel=kernel -ggdb
	make -C ../linux M=$(PWD)
	ld -r -o cjfinal.o --unresolved-symbols=ignore-all --allow-multiple-definition canceljoin.o --start-group glibcfinal --end-group 
	ar cr UKL.a ukl.o interface.o cjfinal.o
	rm -rf *.ko *.mod.* .H* .tm* .*cmd Module.symvers modules.order built-in.a 
	rm -rf ../linux/vmlinux 
	make -C ../linux -j$(shell nproc)

stack: glibc
	gcc -c -o rspcheck.o rspcheck.S -mcmodel=kernel -ggdb
	gcc -c -o stackcheck.o stackcheck.c -mcmodel=kernel -ggdb
	make -C ../linux M=$(PWD)
	ld -r -o stackfinal.o --unresolved-symbols=ignore-all --allow-multiple-definition stackcheck.o --start-group glibcfinal --end-group 
	ar cr UKL.a ukl.o interface.o stackfinal.o rspcheck.o
	rm -rf *.ko *.mod.* .H* .tm* .*cmd Module.symvers modules.order built-in.a 
	rm -rf ../linux/vmlinux 
	make -C ../linux -j$(shell nproc)

uklfutex: glibc
	gcc -c -o rspcheck.o rspcheck.S -mcmodel=kernel -ggdb
	gcc -c -o stackcheck.o stackcheck.c -mcmodel=kernel -ggdb
	gcc -c -o uklfutex.o uklfutex.c -mcmodel=kernel -ggdb
	make -C ../linux M=$(PWD)
	ld -r -o uklfutexfinal.o --unresolved-symbols=ignore-all --allow-multiple-definition uklfutex.o --start-group glibcfinal --end-group 
	ar cr UKL.a ukl.o interface.o uklfutexfinal.o rspcheck.o stackcheck.o
	rm -rf *.ko *.mod.* .H* .tm* .*cmd Module.symvers modules.order built-in.a 
	rm -rf ../linux/vmlinux 
	make -C ../linux -j$(shell nproc)

memcached: glibc
	rm -f UKLmemcached
	rm -f UKLlibevent
	cp ../memcached/UKLmemcached .
	cp ../libevent/UKLlibevent .
	ld -r -o memcachedfinal.o --unresolved-symbols=ignore-all --allow-multiple-definition --whole-archive UKLmemcached --start-group glibcfinal UKLlibevent --end-group --no-whole-archive
	ar cr UKL.a memcachedfinal.o
	rm -rf ../linux/vmlinux 
	make -C ../linux -j$(shell nproc)
	#make -C ../linux -j$(shell nproc)

multithreaded-tcp-server: glibc
	gcc multithreadedserver.c -c -o multithreadedserver.o -mcmodel=kernel -ggdb
	ld -r -o multcp.o --unresolved-symbols=ignore-all --allow-multiple-definition --whole-archive multithreadedserver.o --start-group glibcfinal --end-group --no-whole-archive
	ar cr UKL.a multcp.o
	rm -rf *.ko *.mod.* .H* .tm* .*cmd Module.symvers modules.order built-in.a 
	rm -rf ../linux/vmlinux 
	make -C ../linux -j$(shell nproc)

multithreaded-printing: glibc
	gcc printer.c -c -o printer.o -mcmodel=kernel -ggdb
	make -C ../linux M=$(PWD)
	ld -r -o mulprint.o --unresolved-symbols=ignore-all --allow-multiple-definition printer.o --start-group glibcfinal --end-group 
	ar cr UKL.a ukl.o interface.o mulprint.o
	rm -rf *.ko *.mod.* .H* .tm* .*cmd Module.symvers modules.order built-in.a 
	rm -rf ../linux/vmlinux 
	make -C ../linux -j$(shell nproc)

client:
	gcc -o client client.c -lpthread -ggdb

userstack:
	gcc -o userstack userstack.c rspcheck.S -lpthread -ggdb --static

ufutex:
	gcc -o ufutex ufutex.c -lpthread -ggdb --static


singlethreaded-tcp-server: glibc
	gcc tcpsingle.c -c -o tcpsingle.o -mcmodel=kernel -ggdb
	make -C ../linux M=$(PWD)
	ld -r -o mytcp.o --unresolved-symbols=ignore-all --allow-multiple-definition tcpsingle.o --start-group glibcfinal --end-group 
	ar cr UKL.a ukl.o interface.o mytcp.o 
	rm -rf *.ko *.mod.* .H* .tm* .*cmd Module.symvers modules.order built-in.a 
	rm -rf ../linux/vmlinux 
	make -C ../linux -j$(shell nproc)

run:
	make -C ../min-initrd runU

debug:
	make -C ../min-initrd debugU

mon:
	make -C ../min-initrd monU

utb4:
	rm utb4
	gcc -o utb4 utb4.c -lpthread -ggdb

barriercheck:
	rm -rf barriercheck
	gcc -o barriercheck barriercheck.c -lpthread -ggdb
