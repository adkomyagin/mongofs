mongofs
=======

FUSE-based FS backed by MongoDB GridFS

to build: sh build.sh

to mount: ./hello -f /tmp/hello -o direct_io

Notes:

1. mongod should be running on localhost:27017

2. direct_io is needed so data is not cached in the kernel

3. subdirectories support is implemented through the special "dir"
contentType
