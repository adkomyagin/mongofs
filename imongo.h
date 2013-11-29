#include "mongo.h"
#include "gridfs.h"
#include <fuse.h>

int64_t mongoread(const char *file_name, char *data);


#define CHUNKS_NS "ctest.fs.chunks"
#define METADATA_NS "ctest.fs.files"

int mongo_destroy_gfs();
int mongo_init_gfs(const char *server, int port);

int64_t mongo_file_exists(const char *file_name);
int mongo_find_names( fuse_fill_dir_t filler, void *buf );
int64_t mongo_read(const char *file_name, char *data, size_t size, off_t offset);
int mongo_find_names_distinct( fuse_fill_dir_t filler, void *buf );

