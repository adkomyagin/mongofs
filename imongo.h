#include "mongo.h"
#include "gridfs.h"

int64_t mongoread(const char *file_name, char *data);


#define CHUNKS_NS "ctest.fs.chunks"
#define METADATA_NS "ctest.fs.files"

extern bson_timestamp_t milestone;
int fetch_metadata(const char *server, const char *client, bson_timestamp_t start);
