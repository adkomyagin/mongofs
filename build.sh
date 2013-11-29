cc -D_FILE_OFFSET_BITS=64 -DFUSE_USE_VERSION=26 -I/usr/local/include/osxfuse/fuse -Wall -g  --std=c99 -I mongoc/src -o hello mongoc/src/*.c imongo.c hello.c -losxfuse
