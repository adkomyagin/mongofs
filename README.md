mongofs
=======

Experimental FUSE-based FS backed by MongoDB GridFS

to build: sh build.sh

to mount: ./hello -f /tmp/hello -o direct\_io

Notes:

1. mongod should be running on localhost:27017

2. direct\_io is needed so data is not cached in the kernel

3. subdirectories support is implemented through the special "dir"
contentType

4. versioning support is very experimental
  - each process that writes to the file creates a new version of the file
  - on open/getattr we are always accessing the latest version
  - all versions for all the files within a dir are available in the 'versions' subdirectory
    - each file is named {filename}\_{\_id}, where \_id is the unique id in mongodb
    - you have normal read access to the files there (sorry, no writes)
    - you can issue a touch command on the particular version to make it the current
    - you can even remove unwanted versions

5. I'm using OS X FUSE that should be compatible with Linux, but
I have not tested the app on any other OS yet.
