#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <fuse.h>
#include "imongo.h"
#include <fuse_opt.h>
#include <libgen.h>

/** options for fuse_opt.h */
struct options {
   char* server;
   int port;
} options;

/** macro to define options */
#define offsetof(type,field) __builtin_offsetof(type, field)
#define TESTFS_OPT_KEY(t, p, v) { t, offsetof(struct options, p), v }

static struct fuse_opt hello_opts[] =
{
    TESTFS_OPT_KEY("server=%s", server, 0),
    TESTFS_OPT_KEY("port=%i", port, 0),

    FUSE_OPT_END
};

/*
 * All access operations are trying to access the latest copy of the file (sort by lastModified and _id)
 */

static int
hello_getattr(const char *path, struct stat *stbuf)
{
    int64_t len;
    time_t ctime;

    printf("file getattr: %s\n", path);
    //printf("file basename: %s\n", basename(path));

    memset(stbuf, 0, sizeof(struct stat));

    if (strcmp(path, "/") == 0) { /* The root directory of our file system. */
        stbuf->st_mode = S_IFDIR | 0777;
        stbuf->st_nlink = 3;
    } else if ((len = mongo_file_exists_(path, &ctime)) != -1) { /* The file that we have. */
        stbuf->st_mode = S_IFREG | 0777;
        stbuf->st_nlink = 1;
        stbuf->st_size = len;
        stbuf->st_ctime = stbuf->st_mtime = ctime;
    } else if ((len = mongo_dir_exists_(path, &ctime)) != -1) { /* The dir that we have. */
        stbuf->st_mode = S_IFDIR | 0777;
        stbuf->st_nlink = 3;
        stbuf->st_ctime = stbuf->st_mtime = ctime;
        //char buff[20];
        //strftime(buff, 20, "%Y-%m-%d %H:%M:%S", localtime(&ctime));
        //printf("CTIME: %s\n", buff);
    } else
        return -ENOENT;
    /*{ // The file that we don't have. 
        stbuf->st_mode = S_IFREG | 0777;
        stbuf->st_nlink = 1;
        stbuf->st_size = 0;
    }*/

    return 0;
}

static int
hello_open(const char *path, struct fuse_file_info *fi)
{
    mongo_fs_handle *fh = mongo_get_file_handle(path);
    if (fh == NULL) /* We only recognize files we have */
        return -ENOENT;

    fi->fh = (int64_t)fh;
    printf("file open: %s. handle: 0x%X\n", path, fi->fh);

    return 0;
}

static int
hello_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
              off_t offset, struct fuse_file_info *fi)
{
    printf("readdir: %s\n", path);

    filler(buf, ".", NULL, 0);           /* Current directory (.)  */
    filler(buf, "..", NULL, 0);          /* Parent directory (..)  */

    mongo_find_file_names_distinct(path, filler, buf);

    return 0;
}

static int
hello_read(const char *path, char *buf, size_t size, off_t offset,
           struct fuse_file_info *fi)
{
    printf("read requested: %s, size: %d, offset: %d. handle: 0x%X\n", path, size, offset, fi->fh);

    mongo_fs_handle *fh = (mongo_fs_handle *)fi->fh;

    return mongo_read(fh, buf, size, offset);
}

static int
hello_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    printf("file create: %s\n", path);

    mongo_fs_handle *fh = mongo_create_file_handle();
    fh->is_creator = 1;

    mongo_write(fh, path, NULL, 0, 0);

    fi->fh = (int64_t)fh;

    return 0;
}

static int
hello_mkdir(const char *path, mode_t mode)
{
    printf("mkdir requested: %s\n", path);

    return mongo_mkdir(path);
}

static int
hello_rmdir(const char *path)
{
    printf("rmdir requested: %s\n", path);

    //printf("DID EMPTY: %d\n", mongo_dir_empty( path ));

    if (mongo_dir_empty( path ) == 1)
    {
        mongo_unlink(path);
        return 0;
    }
    else
        return -ENOTEMPTY;
}

static int
hello_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    printf("write requested: %s, size: %d, offset: %d. handle: 0x%X\n", path, size, offset, fi->fh);

    mongo_fs_handle *fh = (mongo_fs_handle *)fi->fh;

    if (fh->is_creator == 0)
    {
        mongo_reset_file_handle(fh);
        fh->is_creator = 1;
        printf("write had set creator mode\n");
    }

    return mongo_write(fh, path, buf, size, offset);
}

static int
hello_unlink(const char *path)
{
    printf("file unlink: %s\n", path);

    mongo_unlink(path);

    return 0;
}

static int
hello_release(const char *path, struct fuse_file_info *fi)
{
    printf("file release: %s\n", path);
    mongo_destroy_file_handle((void*)fi->fh);

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
    .fsync   = hello_fsync,
    .mkdir   = hello_mkdir,
    .rmdir   = hello_rmdir
};

int
main(int argc, char **argv)
{
    //return fuse_main(argc, argv, &hello_filesystem_operations, NULL);
    int ret;
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    /* clear structure that holds our options */
    options.server = "127.0.0.1";
    options.port = 27017;

    if (fuse_opt_parse(&args, &options, hello_opts, NULL) == -1)
    {
        printf("error parsing options\n");
        return -1;
    }

    //connect gridfs
    if (mongo_init_gfs(options.server, options.port) < 0)
    {
        printf("init gridfs failed\n");
        return -1;
    }

    ret = fuse_main(args.argc, args.argv, &hello_filesystem_operations, NULL);

    /** free arguments */
    fuse_opt_free_args(&args);

    mongo_destroy_gfs();

    return ret;
}
