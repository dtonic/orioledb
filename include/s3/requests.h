/*-------------------------------------------------------------------------
 *
 * requests.h
 *		Declarations for S3 requests.
 *
 * Copyright (c) 2024-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/requests.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_REQUESTS_H__
#define __S3_REQUESTS_H__

#include "datatype/timestamp.h"
#include "nodes/pg_list.h"

#define S3_RESPONSE_OK					200
#define S3_RESPONSE_NOT_FOUND			404
#define S3_RESPONSE_CONDITION_CONFLICT	409
#define S3_RESPONSE_CONDITION_FAILED	412

extern long s3_put_file(char *objectname, char *filename, bool ifNoneMatch);
extern long s3_put_object_fenced(char *objectname, Pointer data,
								 uint64 dataSize, char *dataChecksum);
extern long s3_put_object_fenced_until(char *objectname, Pointer data,
									   uint64 dataSize, char *dataChecksum,
									   TimestampTz deadline);
extern void s3_get_file(char *objectname, char *filename);
extern void s3_put_empty_dir(char *objectname);
extern long s3_put_file_part(char *objectname, char *filename, int partnum);
extern void s3_get_file_part(char *objectname, char *filename, int partnum);
extern long s3_put_object_with_contents(char *objectname, Pointer data,
										uint64 dataSize, char *dataChecksum,
										bool ifNoneMatch);
extern long s3_put_object_conditional(char *objectname, Pointer data,
									  uint64 dataSize, const char *ifMatch,
									  bool ifNoneMatch, char **etag);
extern long s3_request_once(const char *method, char *objectname, Pointer data,
							uint64 dataSize, const char *ifMatch,
							bool ifNoneMatch, long timeout, StringInfo body,
							char **etag, char **date);
extern long s3_get_object(char *objectname, StringInfo str, bool missing_ok);
extern long s3_get_object_etag(char *objectname, StringInfo str,
							   bool missing_ok, char **etag);
extern void s3_delete_object(char *objectname);
extern bool s3_object_exists(char *objectname);
extern void (*s3_request_wait_hook) (void);
extern int	s3_auth_retry_time;
extern void s3_check_bucket_versioning(void);
extern void s3_delete_objects(char **objectnames, int count);
extern List *s3_list_objects(const char *prefix, const char *startAfter);

extern Pointer read_file(const char *filename, uint64 *size);
extern void s3_write_file(const char *filename, Pointer data, uint64 size);

#endif							/* __S3_REQUESTS_H__ */
