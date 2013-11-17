TARGETS = hello

CC ?= gcc
CFLAGS_OSXFUSE = -D_FILE_OFFSET_BITS=64 -DFUSE_USE_VERSION=26 -I/usr/local/include/osxfuse/fuse
CFLAGS_EXTRA = -Wall -g $(CFLAGS) --std=c99 -I mongoc/src
LIBS = -losxfuse

.c:
	$(CC) $(CFLAGS_OSXFUSE) $(CFLAGS_EXTRA) -o $@ $< $(LIBS)

all: $(TARGETS)

hello: hello.c

clean:
	rm -f $(TARGETS) *.o
