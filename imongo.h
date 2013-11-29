#include "mongo.h"
#include "gridfs.h"

int64_t mongoread(const char *file_name, char *data);


#define CHUNKS_NS "ctest.fs.chunks"
#define METADATA_NS "ctest.fs.files"

int mongo_destroy_gfs();
int mongo_init_gfs(const char *server, int port);
