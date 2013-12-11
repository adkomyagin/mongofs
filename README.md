mongofs
=======

Experimental FUSE-based FS backed by MongoDB GridFS

to build: sh build.sh

to mount: ./hello -f /tmp/hello -o direct_io

Notes:

1. mongod should be running on localhost:27017

2. direct_io is needed so data is not cached in the kernel

3. subdirectories support is implemented through the special "dir"
contentType

4. versioning support is very experimental
  - each process that writes to the file creates a new version of the file
  - on open/getattr we are always accessing the latest version
  - there is no way to view all versions

5. I'm using OS X FUSE that should be compatible with Linux, but
I have not tested the app on any other OS yet.
