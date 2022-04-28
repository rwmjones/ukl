SOURCES := $(wildcard *.c)
DEPS := $(SOURCES:%.c=%.d)
OBJS := $(SOURCES:%.c=%.o)

all : libsecrecy.a

-include $(DEPS)

libsecrecy.a: $(OBJS)
	$(AR) rs libsecrecy.a $(OBJS)

%.d : %.c
	$(CC) $(CFLAGS) -MM -MG -o $@ $<

%.o : %.c
	$(CC) $(CFLAGS) -c -o $@ $<

.PHONY: clean
clean:
	rm -f $(DEPS) $(OBJS) libsecrecy.a

