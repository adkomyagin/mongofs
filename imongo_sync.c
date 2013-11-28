#include "imongo.h"

bson_timestamp_t milestone = {0,0};

int ts_valid(bson_timestamp_t ts)
{
  return ((ts.i != 0) || (ts.t != 0));
}

int apply_metadata_change(mongo_cursor *server_op_cursor, mongo *client_conn)
{
  return 1;
}

int apply_chunks_change(mongo_cursor *server_op_cursor, mongo *client_conn)
{
  return 1;
}

int skip_to_next(mongo_cursor *server_op_cursor)
{
  return 1;
}

int local_avail(bson_oid_t *file_id, mongo *client_conn)
{
  return 1;
}

int fetch_metadata(const char *server, const char *client, bson_timestamp_t start)
{
   mongo_cursor server_op_cursor[1];
   mongo client_conn[1];
   mongo server_conn[1];

   printf( "establish client connection\n" );
   int status = mongo_client( client_conn, client, 27017 );

   if( status != MONGO_OK ) {
       switch ( client_conn->err ) {
         case MONGO_CONN_NO_SOCKET:  printf( "no socket\n" ); return -1;
         case MONGO_CONN_FAIL:       printf( "connection failed\n" ); return -1;
         case MONGO_CONN_NOT_MASTER: printf( "not master\n" ); return -1;
       }
   }

   printf( "establish server connection\n" );
   status = mongo_client( server_conn, server, 27017 );

   if( status != MONGO_OK ) {
       mongo_destroy( client_conn );
       switch ( server_conn->err ) {
         case MONGO_CONN_NO_SOCKET:  printf( "no socket\n" ); return -1;
         case MONGO_CONN_FAIL:       printf( "connection failed\n" ); return -1;
         case MONGO_CONN_NOT_MASTER: printf( "not master\n" ); return -1;
       }
   }

   printf("connected ok");

   //create query
   bson query[1];
   bson_init( query );
   bson_append_start_object( query, "$query" );
     bson_append_start_object( query, "$or" );
       bson_append_string( query, "ns", METADATA_NS );
       bson_append_string( query, "ns", CHUNKS_NS );
     bson_append_finish_object( query );

   if (ts_valid(start))
   {
     bson_append_start_object( query, "ts" );
       bson_append_timestamp( query, "$gte", &start );
     bson_append_finish_object( query );
   }

   bson_append_finish_object( query );

   bson_append_start_object( query, "$orderby" );
     bson_append_int( query, "$natural", 1);
   bson_append_finish_object( query );
   bson_finish( query );

   //create cursor
   mongo_cursor_init( server_op_cursor, server_conn, "local.oplog.rs" );
   mongo_cursor_set_query( server_op_cursor, query );
   int options = MONGO_TAILABLE | MONGO_AWAIT_DATA;
   if (ts_valid(start))
     options |= ( 1<<8 ); //oplog replay
   mongo_cursor_set_options( server_op_cursor, options);

   //query the server
   while( mongo_cursor_next( server_op_cursor ) == MONGO_OK ) {
      bson_iterator iterator[1];

      char *action = NULL;
      char *ns = NULL;
      bson_oid_t file_id[1];

      if ( bson_find( iterator, mongo_cursor_bson( server_op_cursor ), "op" )) {
        action = strdup(bson_iterator_string( iterator ) );
      }

      if ( bson_find( iterator, mongo_cursor_bson( server_op_cursor ), "ns" )) {
        ns = strdup(bson_iterator_string( iterator ) );
      }

      if ( bson_find( iterator, mongo_cursor_bson( server_op_cursor ), "ts" )) {
        milestone = bson_iterator_timestamp( iterator );
      }

      if (strcmp(ns, METADATA_NS) == 0) { //updates&removals start with metadata
        //get the file _id
        if ( bson_find( iterator, mongo_cursor_bson( server_op_cursor ), "op" )) {
            bson sub[1];
            bson_iterator_subobject_init( iterator, sub, 1 );
            if ( bson_find( iterator, sub, "_id" )) {
              *file_id = *(bson_iterator_oid( iterator ));
            }
            bson_destroy(sub);
        }

        apply_metadata_change(server_op_cursor, client_conn);
        if (local_avail(file_id, client_conn)) {
            apply_chunks_change(server_op_cursor, client_conn);
        } else
          skip_to_next(server_op_cursor);

      } else if ((strcmp(ns, CHUNKS_NS) == 0) && (strcmp(action, "i") == 0)) { //insert
        //get the file _id
        if ( bson_find( iterator, mongo_cursor_bson( server_op_cursor ), "op" )) {
            bson sub[1];
            bson_iterator_subobject_init( iterator, sub, 1 );
            if ( bson_find( iterator, sub, "file_id" )) {
              *file_id = *(bson_iterator_oid( iterator ));
            }
            bson_destroy(sub);
        }

        if (local_avail(file_id, client_conn)) {
            apply_chunks_change(server_op_cursor, client_conn);
        } else
          skip_to_next(server_op_cursor);
        apply_metadata_change(server_op_cursor, client_conn);

      } else
          skip_to_next(server_op_cursor);

      if (action)
        free(action);
      if (ns)
        free(ns);
   }

   bson_destroy (query);
   mongo_cursor_destroy (server_op_cursor);
   mongo_destroy( client_conn );
   mongo_destroy( server_conn );

   return 0;
}
