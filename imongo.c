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

  return 0;
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

    gridfile_destroy( gfile );
  }
  else
    printf("file not found: %s\n", file_name);

  return len;
}

int64_t mongo_file_id_exists_(const char *file_id, const char *file_name, time_t *ctime)
{
  int64_t len = -1;
  gridfile gfile[1];
  bson_oid_t oid[1];

  bson_oid_from_string(oid, file_id);

  if ((gridfs_find_filename_with_id( gfs, oid, file_name, FILE_CT, gfile ) == MONGO_OK) && (gridfile_exists( gfile )))
  {
    len = gridfile_get_contentlength(gfile);
    if (ctime != NULL)
      *ctime = (time_t)gridfile_get_uploaddate(gfile)/1000;

    gridfile_destroy( gfile );
  }
  else
    printf("file name:id not found: %s : %s\n", file_name, file_id);

  return len;
}

mongo_fs_handle* mongo_get_file_handle(const char *file_name)
{
  mongo_fs_handle *handle = (mongo_fs_handle *)calloc(1, sizeof(mongo_fs_handle));

  if ((gridfs_find_filename( gfs, file_name, FILE_CT, handle->gfile ) == MONGO_OK) && (gridfile_exists( handle->gfile )))
  {
    //it's ok
  }
  else
  {
    printf("file not found: %s\n", file_name);
    free(handle);
    handle = NULL;
  }

  return handle;
}

mongo_fs_handle* mongo_get_file_handle_with_id(const char *file_id, const char *file_name)
{
  mongo_fs_handle *handle = (mongo_fs_handle *)calloc(1, sizeof(mongo_fs_handle));

  bson_oid_t oid[1];
  bson_oid_from_string(oid, file_id);

  if ((gridfs_find_filename_with_id( gfs, oid, file_name, FILE_CT, handle->gfile ) == MONGO_OK) && (gridfile_exists( handle->gfile )))
  {
    //it's ok
  }
  else
  {
    printf("file not found: %s\n", file_name);
    free(handle);
    handle = NULL;
  }

  return handle;
}

int mongo_update_time(const char *file_name, const char *file_id, const struct timespec ts[2])
{
  int64_t len = -1;
  gridfile gfile[1];

  bson_oid_t oid[1];

  if (file_id == NULL)
  {
    if ((gridfs_find_filename( gfs, file_name, FILE_CT, gfile ) == MONGO_OK) && (gridfile_exists( gfile )))
      *oid = gridfile_get_id( gfile );
    else
      return -1;
  }
  else
  {
    bson_oid_from_string(oid, file_id);

    if ((gridfs_find_filename_with_id( gfs, oid, file_name, FILE_CT, gfile ) == MONGO_OK) && (gridfile_exists( gfile )))
    {
      //ok
    }
    else
      return -1;
  }

  gridfile_destroy( gfile );

  bson ret[1];
  bson q[1];
  int result;
  int64_t d;

  bson_init(ret);
  bson_append_start_object( ret, "$set" );
    //get mtime
    d = ts[1].tv_sec*1000;// + ts[0].tv_nsec / 1000;
    bson_append_date(ret, "uploadDate", d);
  bson_append_finish_object( ret );
  bson_finish(ret);

  bson_init(q);
  bson_append_oid(q, "_id", oid);
  bson_finish(q);

  result = mongo_update(gfs->client, gfs->files_ns, q, ret, 0, NULL);

  bson_destroy(ret);
  bson_destroy(q);

  return (result == MONGO_OK) ? 0 : -1;
}

void mongo_destroy_file_handle(mongo_fs_handle *fh)
{
  if (fh == NULL)
  {
    printf("File handle is NULL in mongo_destroy_file_handle!\n");
    return;
  }

  gridfile_destroy(fh->gfile);
  free(fh);
}

mongo_fs_handle* mongo_create_file_handle()
{
  mongo_fs_handle *handle = (mongo_fs_handle *)calloc(1, sizeof(mongo_fs_handle));
  //gridfile_init( gfs, NULL, handle->gfile );

  return handle;
}

void mongo_reset_file_handle(mongo_fs_handle *fh)
{
  gridfile_destroy(fh->gfile);
  //gridfile_init( gfs, NULL, fh->gfile );
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

    gridfile_destroy( gfile );
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

int mongo_find_file_names_versions( const char *path, fuse_fill_dir_t filler, void *buf )
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
    mongo_cursor cursor[1];

    bson_init(query);
       bson_append_string(query, "contentType", FILE_CT);
       bson_append_start_object( query, "filename" );
       bson_append_string( query, "$regex", regex );
       bson_append_finish_object( query );
    bson_finish(query);

    mongo_cursor_init( cursor, gfs->client, gfs->files_ns );
    mongo_cursor_set_query( cursor, query );

    char version_name[MAX_FILENAME_LENGTH + 1 + 24 + 1]; //"name_id" form

    while( mongo_cursor_next( cursor ) == MONGO_OK ) {
      bson_iterator it_name[1], it_id[1];
      if ( bson_find( it_name, mongo_cursor_bson( cursor ), "filename" ) &&
           bson_find( it_id, mongo_cursor_bson( cursor ), "_id" )) {

        char *vp = version_name;

        char *name = basename( bson_iterator_string( it_name ) );
        memcpy(vp, name, strlen(name)); vp += strlen(name);

        *vp = '_'; vp++;
        bson_oid_t *id = bson_iterator_oid( it_id );
        bson_oid_to_string( id, vp ); //it will add trailing '\0' 

        filler(buf, version_name, NULL, 0);  
      }
    }

    bson_destroy( query );
    mongo_cursor_destroy( cursor );

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


int64_t mongo_read(const mongo_fs_handle *fh, char *data, size_t size, off_t offset) {
  int64_t len = 0;
  gridfile *gfile = fh->gfile;

  len = gridfile_get_contentlength(gfile);

  if (offset >= len) /* Trying to read past the end of file. */
      return 0;

  if (offset + size > len) /* Trim the read to the file size. */
      size = len - offset;

  gridfile_seek(gfile, offset);
  ASSERT( gridfile_read_buffer( gfile, data, size ) ==  (size_t)(size));
    //ridfs_offset bytes_read = gridfile_read_buffer(gfile,data,len);
    //ASSERT(bytes_read == len);
  gridfile_seek(gfile, 0); //restore the initial position

  return size;
}

int mongo_unlink(const char *filename)
{
    return gridfs_remove_filename(gfs, filename);
}

int mongo_unlink_id(const char *file_id, const char *filename)
{
    bson b[1];
    int res;

    bson_oid_t oid[1];
    bson_oid_from_string(oid, file_id);

    /* Remove the file with the specified id */
    bson_init(b);
    bson_append_oid(b, "_id", oid);
    bson_append_string(b, "filename", filename);
    bson_finish(b);
    res = (mongo_remove(gfs->client, gfs->files_ns, b, NULL) == MONGO_OK);
    bson_destroy(b);

    if (! res)
    {
      printf ("failed to remove %s:%s. Does it exist?\n", file_id, filename);
      return -1;
    }

    /* Remove all chunks from the file with the specified id */
    bson_init(b);
    bson_append_oid(b, "files_id", oid);
    bson_finish(b);
    res = (mongo_remove(gfs->client, gfs->chunks_ns, b, NULL) == MONGO_OK);
    bson_destroy(b);

    return res;
}

/*
 * If copy_on_write is provided, we will create a copy and reinitialize fh->gridfile 
 */
int64_t mongo_write(mongo_fs_handle *fh, const char *filename, const char *buf, size_t size, off_t offset, int copy_on_write)
{
    int res;

    if (copy_on_write) {
      //TODO: we should be taking some lock on the original file here so it doesn't get modified by the owner in the process of duplicating

      printf("duplicating file\n");
      gridfile gfile_new[1];

      if (gridfile_duplicate(gfs, fh->gfile, gfile_new) != 0)
      {
        printf("gridfile_duplicate failed!\n");
        return -1;
      }

      printf("done\n");

      gridfile_destroy(fh->gfile);
      *(fh->gfile) = *gfile_new;
    }

    gridfile_seek(fh->gfile, offset);
    res = gridfile_write_buffer_warp(fh->gfile, buf, size);
    gridfile_seek(fh->gfile, 0);

    return res;
}

int mongo_mkdir(const char *dirname)
{
    if (MONGO_OK == gridfs_store_buffer( gfs, NULL, 0, dirname, DIR_CT, GRIDFILE_DEFAULT, 1/*safe to always overwrite*/ ))
      return 0;
    else
      return -1;
}

int mongo_create_file(mongo_fs_handle *fh, const char *filename)
{
    if (MONGO_OK == gridfs_store_buffer_advanced( gfs, NULL, 0, fh->gfile, filename, FILE_CT, GRIDFILE_DEFAULT))
      return 0;
    else
      return -1;
}
