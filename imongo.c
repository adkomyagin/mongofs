#include <stdio.h>
#include "mongo.h"
#include "gridfs.h"
#include "imongo.h"

#define ASSERT(x) \
    do{ \
        if(!(x)){ \
            printf("\nFailed ASSERT [%s] (%d):\n     %s\n\n", __FILE__,  __LINE__,  #x); \
            exit(1); \
        }\
    }while(0)

int64_t mongoread(const char *file_name, char *data) {
  mongo conn[1];
  int64_t len = 0;
  int status = mongo_client( conn, "127.0.0.1", 27017 );

  if( status != MONGO_OK ) {
      switch ( conn->err ) {
        case MONGO_CONN_NO_SOCKET:  printf( "no socket\n" ); return -1;
        case MONGO_CONN_FAIL:       printf( "connection failed\n" ); return -1;
        case MONGO_CONN_NOT_MASTER: printf( "not master\n" ); return -1;
      }
  }

  printf("connected ok\n");

  gridfs gfs[1];
  gridfile gfile[1];
  gridfs_init( conn, "ctest", "fs", gfs );

  if ((gridfs_find_filename( gfs, file_name, gfile ) == MONGO_OK) && (gridfile_exists( gfile )))
  {
    len = gridfile_get_contentlength(gfile);

    gridfs_offset bytes_read = gridfile_read_buffer(gfile,data,len);
    ASSERT(bytes_read == len);
  }
  else
    printf("file not found\n");

  gridfs_destroy( gfs );
  mongo_destroy( conn );

  return len;
}
