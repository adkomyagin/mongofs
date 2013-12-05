#include <stdio.h>
#include "mongo.h"
#include "gridfs.h"
#include "imongo.h"
#include <libgen.h>

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

  return 0;
}

int64_t mongo_file_exists(const char *file_name)
{
  /*int64_t len = -1;
  gridfile gfile[1];

  if ((gridfs_find_filename( gfs, file_name, gfile ) == MONGO_OK) && (gridfile_exists( gfile )))
  {
    len = gridfile_get_contentlength(gfile);
  }
  else
    printf("file not found: %s\n", file_name);

  return len;*/
  return mongo_file_exists_(file_name, NULL);
}

int64_t mongo_file_exists_(const char *file_name, time_t *ctime)
{
  int64_t len = -1;
  gridfile gfile[1];

  if ((gridfs_find_filename( gfs, file_name, FILE_CT, gfile ) == MONGO_OK) && (gridfile_exists( gfile )))
  {
    len = gridfile_get_contentlength(gfile);
    if (ctime != NULL)
      *ctime = (time_t)gridfile_get_uploaddate(gfile)/1000;
  }
  else
    printf("file not found: %s\n", file_name);

  return len;
}

int64_t mongo_dir_exists_(const char *dir_name, time_t *ctime)
{
  int64_t len = -1;
  gridfile gfile[1];

  if ((gridfs_find_filename( gfs, dir_name, DIR_CT, gfile ) == MONGO_OK) && (gridfile_exists( gfile )))
  {
    len = gridfile_get_contentlength(gfile);
    if (ctime != NULL)
      *ctime = (time_t)gridfile_get_uploaddate(gfile)/1000;
  }
  else
    printf("dir not found: %s\n", dir_name);

  return len;
}

int mongo_find_file_names_distinct( const char *path, fuse_fill_dir_t filler, void *buf )
{
    bson b[1];
    bson out[1];
    bson query[1];

    static char file_regex[] = "[^/?*:;{}\]+$";
    char regex[1 + strlen(path) + 1 + strlen(file_regex) + 1];
    int regex_off = 0;

    regex[regex_off++] = '^';
    memcpy(&(regex[regex_off]) , path, strlen(path)); regex_off+=strlen(path);
    if (strcmp(path, "/") != 0)
    {
      regex[regex_off++] = '/';
    }
    memcpy(&(regex[regex_off]) , file_regex, strlen(file_regex)); regex_off+=strlen(file_regex);
    regex[regex_off++] = '\0';

    bson_init( b );
    bson_append_string( b, "distinct", "fs.files" );
    bson_append_string( b, "key", "filename" );
    bson_init(query);
      //bson_append_string(query, "contentType", FILE_CT);
       bson_append_start_object( query, "filename" );
       bson_append_string( query, "$regex", regex );
       bson_append_finish_object( query );
    bson_finish(query);
    bson_append_bson(b, "query", query);
    bson_destroy(query);

    bson_finish( b );

    if (mongo_run_command( g_conn, "ctest", b, out ) != MONGO_OK)
    {
      bson_destroy( b );
      return -1;
    }

    bson_destroy( b );

    bson_iterator iterator[1], sub[1];

    if ( bson_find( iterator, out, "values" )) {
      bson_iterator_subiterator( iterator, sub );

      while( bson_iterator_next(sub) )
      {
        //printf("GOT ID: %s\n", bson_iterator_string( sub ));
        filler(buf, basename(bson_iterator_string( sub )), NULL, 0);
      }

    }

    bson_destroy( out );

    return 0;
}

int mongo_dir_empty( const char *path )
{
    static char file_regex[] = "[^/?*:;{}\]+$";
    char regex[1 + strlen(path) + 1 + strlen(file_regex) + 1];
    int regex_off = 0;

    regex[regex_off++] = '^';
    memcpy(&(regex[regex_off]) , path, strlen(path)); regex_off+=strlen(path);
    if (strcmp(path, "/") != 0)
    {
      regex[regex_off++] = '/';
    }
    memcpy(&(regex[regex_off]) , file_regex, strlen(file_regex)); regex_off+=strlen(file_regex);
    regex[regex_off++] = '\0';


    bson query[1];

    bson_init(query);
    bson_append_start_object( query, "filename" );
      bson_append_string( query, "$regex", regex );
    bson_append_finish_object( query );
    bson_finish(query);

    return (mongo_find_one(gfs->client, gfs->files_ns, query, NULL, NULL) != MONGO_OK);
}

int mongo_find_names( fuse_fill_dir_t filler, void *buf )
{
  bson names[1];
  bson query[1];
  bson finalQuery[1];

  mongo_cursor cursor[1];

  bson_init(query);
  bson_finish(query);

  bson_init(names);
  bson_append_int(names, "filename",  1);
  bson_finish(names);

  bson_init(finalQuery);
  bson_append_bson(finalQuery, "query", query);
  bson_append_bson(finalQuery, "project", names);
  bson_finish(finalQuery);

  mongo_cursor_init( cursor, gfs->client, gfs->files_ns );
  mongo_cursor_set_query( cursor, finalQuery );

  while( mongo_cursor_next( cursor ) == MONGO_OK ) {
    bson_iterator iterator[1];
    if ( bson_find( iterator, mongo_cursor_bson( cursor ), "filename" )) {
      filler(buf, bson_iterator_string( iterator ), NULL, 0);  
    }
  }

  bson_destroy( query );
  mongo_cursor_destroy( cursor );

  return 0;
}


int64_t mongo_read(const char *file_name, char *data, size_t size, off_t offset) {
  int64_t len = 0;
  gridfile gfile[1];

  if ((gridfs_find_filename( gfs, file_name, FILE_CT, gfile ) == MONGO_OK) && (gridfile_exists( gfile ))) //sorts by uploadDate:-1
  {
    len = gridfile_get_contentlength(gfile);

    if (offset >= len) /* Trying to read past the end of file. */
        return 0;

    if (offset + size > len) /* Trim the read to the file size. */
        size = len - offset;

    gridfile_seek(gfile, offset);
    ASSERT( gridfile_read_buffer( gfile, data, size ) ==  (size_t)(size));
    //ridfs_offset bytes_read = gridfile_read_buffer(gfile,data,len);
    //ASSERT(bytes_read == len);
  }
  else
    printf("file not found\n");

  return len;
}

int mongo_unlink(const char *filename)
{
    return gridfs_remove_filename(gfs, filename);
}

int64_t mongo_write(const char *filename, const char *buf, size_t size, off_t offset)
{
    if (offset != 0) //don't support partial writes yet
    {
        printf("EASY BRO!\n");
        return -1;
    }

    if (MONGO_OK == gridfs_store_buffer( gfs, buf, size, filename, FILE_CT, GRIDFILE_DEFAULT ))
      return size;
    else
      return -1;
}

int mongo_mkdir(const char *dirname)
{
    if (MONGO_OK == gridfs_store_buffer( gfs, NULL, 0, dirname, DIR_CT, GRIDFILE_DEFAULT ))
      return 0;
    else
      return -1;
}
