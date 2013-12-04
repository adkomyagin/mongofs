#include "mongo.h"
#include "gridfs.h"
#include <fuse.h>

int64_t mongoread(const char *file_name, char *data);


#define CHUNKS_NS "ctest.fs.chunks"
#define METADATA_NS "ctest.fs.files"

int mongo_destroy_gfs();
int mongo_init_gfs(const char *server, int port);

int64_t mongo_file_exists(const char *file_name);
int64_t mongo_file_exists_(const char *file_name, time_t *ctime);
int mongo_find_names( fuse_fill_dir_t filler, void *buf );
int64_t mongo_read(const char *file_name, char *data, size_t size, off_t offset);
int mongo_find_file_names_distinct( const char *path, fuse_fill_dir_t filler, void *buf );
int64_t mongo_write(const char *filename, const char *buf, size_t size, off_t offset);
int mongo_unlink(const char *filename);
int mongo_mkdir(const char *dirname);

int64_t mongo_dir_exists_(const char *dir_name, time_t *ctime);

#define FILE_CT "text/html"
#define DIR_CT	"dir"
