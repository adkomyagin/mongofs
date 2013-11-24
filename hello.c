#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <fuse.h>
#include "imongo.h"

static const char  *file_path      = "/hello.txt";
static char   file_content[] = "Hello World!\n";
static size_t file_size      = sizeof(file_content)/sizeof(char) - 1;

static char new_file_path[] = "/dummy.dummy.dummy.dummy";
//static char   new_file_content[] = "Hello World!\n";
//static size_t new_file_size      = sizeof(new_file_content)/sizeof(char) - 1;

static int
hello_getattr(const char *path, struct stat *stbuf)
{
    printf("file getattr: %s\n", path);

    memset(stbuf, 0, sizeof(struct stat));

    if (strcmp(path, "/") == 0) { /* The root directory of our file system. */
        stbuf->st_mode = S_IFDIR | 0777;
        stbuf->st_nlink = 3;
    } else if (strcmp(path, file_path) == 0) { /* The only file we have. */
        stbuf->st_mode = S_IFREG | 0777;
        stbuf->st_nlink = 1;
        stbuf->st_size = file_size;
    } else if (strcmp(path, new_file_path) == 0) { /* The newly created file */
        stbuf->st_mode = S_IFREG | 0777;
        stbuf->st_nlink = 1;
        stbuf->st_size = file_size;
    } else
        return -ENOENT;

    return 0;
}

static int
hello_open(const char *path, struct fuse_file_info *fi)
{
    //if (strcmp(path, file_path) != 0) /* We only recognize one file. */
    //    return -ENOENT;

    //if ((fi->flags & O_ACCMODE) != O_RDONLY) /* Only reading allowed. */
    //    return -EACCES;

    printf("file open: %s\n", path);

    file_size = mongoread("file", file_content);
    if (file_size >0)
    {
       file_content[file_size] = '\n';
       file_size++;
    }

    return 0;
}

static int
hello_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
              off_t offset, struct fuse_file_info *fi)
{
    printf("readdir: %s\n", path);

    if (strcmp(path, "/") != 0) /* We only recognize the root directory. */
        return -ENOENT;

    filler(buf, ".", NULL, 0);           /* Current directory (.)  */
    filler(buf, "..", NULL, 0);          /* Parent directory (..)  */
    filler(buf, file_path + 1, NULL, 0); /* The only file we have. */
    filler(buf, new_file_path + 1, NULL, 0); /* The newly created file */

    return 0;
}

static int
hello_read(const char *path, char *buf, size_t size, off_t offset,
           struct fuse_file_info *fi)
{
    printf("read requested: %s, size: %d, offset: %d\n", path, size, offset);

    //if (strcmp(path, file_path) != 0)
    //    return -ENOENT;

    if (offset >= file_size) /* Trying to read past the end of file. */
        return 0;

    if (offset + size > file_size) /* Trim the read to the file size. */
        size = file_size - offset;

    memcpy(buf, file_content + offset, size); /* Provide the content. */

    return size;
}

static int
hello_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    printf("file create: %s\n", path);

    strcpy(new_file_path, path);

    return 0;
}

static int
hello_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    printf("write requested: %s, size: %d, offset: %d\n", path, size, offset);

    return size;
}

static int
hello_unlink(const char *path)
{
    printf("file unlink: %s\n", path);

    return 0;
}

static int
hello_release(const char *path, struct fuse_file_info *fi)
{
    printf("file release: %s\n", path);

    return 0;
}

static int
hello_fsync(const char *path, int isdatasync,
                     struct fuse_file_info *fi)
{
    printf("file fsync: %s\n", path);
  
    return 0;
}



static struct fuse_operations hello_filesystem_operations = {
    .getattr = hello_getattr, /* To provide size, permissions, etc. */
    .open    = hello_open,    /* To enforce read-only access.       */
    .read    = hello_read,    /* To provide file content.           */
    .readdir = hello_readdir, /* To provide directory listing.      */
    .create  = hello_create,
    .write   = hello_write,
    .unlink  = hello_unlink,
    .release = hello_release,
    .fsync   = hello_fsync
};

int
main(int argc, char **argv)
{
    return fuse_main(argc, argv, &hello_filesystem_operations, NULL);
}
