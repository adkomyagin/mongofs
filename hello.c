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

#define VERSION_DIR "versions"

static int is_version_dir(const char *path)
{
    return strcmp(basename(path), VERSION_DIR) == 0;
}

static int is_versioned_file(const char *path) //we check if it's something like /../../{VERSION_DIR}/file
{
    int off = strlen(path) - 1;

    //locate first "/"
    while ((off >= 0) && (path[off] != '/'))
        off--;

    if (off == 0)
        return 0;

    //locate second "/"
    off--;
    while ((off >= 0) && (path[off] != '/'))
        off--;

    if (path[off] != '/') //sanity check
        return 0;

    return (0 == strncmp(&(path[off+1]),VERSION_DIR,strlen(VERSION_DIR)));
}

static int extract_file_version_info(const char *path, char *id, char *real_name) 
//we extract the id part from {name}_{id}
//also we extract real path here
{
    char *name = basename(path);

    int off = strlen(name) - 1;

    //locate the "_"
    while ((off >= 0) && (name[off] != '_'))
        off--;

    if (off == 0)
        return 0;

    if (strlen(&(name[off + 1])) != 24)
        return 0;

    //fill id
    memcpy(id,&(name[off + 1]), 24);
    id[24] = '\0';

    //fill full qualified real path ( /../{VERSION_DIR}/{name}_{id} -> /../{name} )
    int off_dir = strlen(path) - strlen(name) - 1 - strlen(VERSION_DIR);
    memcpy(real_name, path, off_dir);
    memcpy(real_name + off_dir, name, off);
    real_name[off_dir + off] = '\0';

    return 1;
}

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
    } else if (is_version_dir(path)) { /* It's a versioning subdir */
        stbuf->st_mode = S_IFDIR | 0555;
        stbuf->st_nlink = 3;
    } else if (is_versioned_file(path)) {
        //it should be the {name}_{id} form, and we just need the id
        char id[25];
        char name[strlen(path)+1];

        if (extract_file_version_info(path, id, name) && ((len = mongo_file_id_exists_(id, name, &ctime)) != -1))
        {
            stbuf->st_mode = S_IFREG | 0555;
            stbuf->st_nlink = 1;
            stbuf->st_size = len;
            stbuf->st_ctime = stbuf->st_mtime = ctime;
        }
        else
        {
            printf("Failed to extract verion for the file: %s\n", path);
            return -ENOENT; 
        }
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

    if (! is_version_dir(path))
    {
        mongo_find_file_names_distinct(path, filler, buf);
        filler(buf, VERSION_DIR, NULL, 0);           /* versioning directory  */
    } else
    {
        int real_len = strlen(path) - strlen(VERSION_DIR) - 1; // - "/{VERSION_DIR}"
        char vp[real_len + 1];
        memcpy(vp, path, real_len);
        vp[real_len] = '\0';

        printf("Requested version listing for: %s\n", vp);

        mongo_find_file_names_versions(vp, filler, buf);
    }

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

    mongo_create_file(fh, path);

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
    int cow = 0;

    printf("write requested: %s, size: %d, offset: %d. handle: 0x%X\n", path, size, offset, fi->fh);

    mongo_fs_handle *fh = (mongo_fs_handle *)fi->fh;

    if (fh->is_creator == 0)
    {
        fh->is_creator = 1;
        cow = 1;
        printf("write had set creator mode\n");
    }

    return mongo_write(fh, path, buf, size, offset, cow/*copy_on_write*/);
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
