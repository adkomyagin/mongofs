#include "mongo.h"
#include "gridfs.h"
#include <fuse.h>

typedef struct {
	gridfile gfile[1];
	char is_creator;
} mongo_fs_handle;


#define FILE_CT "text/html"
#define DIR_CT	"dir"

#define CHUNKS_NS "ctest.fs.chunks"
#define METADATA_NS "ctest.fs.files"

int64_t mongoread(const char *file_name, char *data);

int mongo_destroy_gfs();
int mongo_init_gfs(const char *server, int port);

mongo_fs_handle* mongo_get_file_handle(const char *file_name);
mongo_fs_handle* mongo_create_file_handle();
void mongo_reset_file_handle(mongo_fs_handle *fh);
void mongo_destroy_file_handle(mongo_fs_handle *fh);

int64_t mongo_write(mongo_fs_handle *fh, const char *filename, const char *buf, size_t size, off_t offset, int copy_on_write);
int64_t mongo_read(const mongo_fs_handle *fh, char *data, size_t size, off_t offset);

int64_t mongo_file_exists_(const char *file_name, time_t *ctime);
int mongo_find_names( fuse_fill_dir_t filler, void *buf );
int mongo_find_file_names_distinct( const char *path, fuse_fill_dir_t filler, void *buf );
//int64_t mongo_write(const char *filename, const char *buf, size_t size, off_t offset);
int mongo_unlink(const char *filename);
int mongo_mkdir(const char *dirname);

int64_t mongo_dir_exists_(const char *dir_name, time_t *ctime);
int mongo_dir_empty( const char *path );

int mongo_create_file(mongo_fs_handle *fh, const char *filename);


