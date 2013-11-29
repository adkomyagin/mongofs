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

static mongo g_conn[1];
static gridfs gfs[1];

int mongo_init_gfs(const char *server, int port)
{
  int status = mongo_client( g_conn, server, port );

  if( status != MONGO_OK ) {
      switch ( g_conn->err ) {
        case MONGO_CONN_NO_SOCKET:  printf( "no socket\n" ); return -1;
        case MONGO_CONN_FAIL:       printf( "connection failed\n" ); return -1;
        case MONGO_CONN_NOT_MASTER: printf( "not master\n" ); return -1;
      }
  }

  printf("connected ok\n");
 
  gridfs_init( g_conn, "ctest", "fs", gfs );

  return 0;
}

int mongo_destroy_gfs()
{
  gridfs_destroy( gfs );
  mongo_destroy( g_conn );
}

int64_t mongoread(const char *file_name, char *data) {
  int64_t len = 0;
  gridfile gfile[1];

  if ((gridfs_find_filename( gfs, file_name, gfile ) == MONGO_OK) && (gridfile_exists( gfile )))
  {
    len = gridfile_get_contentlength(gfile);

    gridfs_offset bytes_read = gridfile_read_buffer(gfile,data,len);
    ASSERT(bytes_read == len);
  }
  else
    printf("file not found\n");

  return len;
}
