/*-------------------------------------------------------------------------
 *
 * requests.c
 *		Implementation for S3 requests.
 *
 * Copyright (c) 2024-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/requests.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <stdbool.h>
#include <unistd.h>
#include <sys/stat.h>

#include "orioledb.h"

#include "s3/lease.h"
#include "s3/requests.h"

#include "common/base64.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/latch.h"
#include "lib/stringinfo.h"
#include "utils/guc.h"
#include "utils/wait_event.h"

#include "curl/curl.h"
#include "openssl/hmac.h"
#include "openssl/sha.h"

PG_FUNCTION_INFO_V1(s3_get);
PG_FUNCTION_INFO_V1(s3_put);

static void
hmac_sha256(char *input, char *output, char *secretkey, int secretkeylen)
{
	HMAC_CTX   *ctx;
	unsigned int len;

	ctx = HMAC_CTX_new();
	HMAC_Init_ex(ctx, secretkey, secretkeylen, EVP_sha256(), NULL);
	HMAC_Update(ctx, (unsigned char *) input, strlen(input));
	HMAC_Final(ctx, (unsigned char *) output, &len);
	HMAC_CTX_free(ctx);

	Assert(len == 32);
}

/*
 * Make the hex representation of the binary string.
 */
static char *
hex_string(Pointer data, int len)
{
	char	   *result = palloc(len * 2 + 1);

	hex_encode(data, len, result);

	result[len * 2] = '\0';
	return result;
}

#define SIGNED_HEADERS			"host;x-amz-content-sha256;x-amz-date"
#define SIGNED_HEADERS_TOKEN	SIGNED_HEADERS ";x-amz-meta-token"

/*
 * Calculate the checksum of canonical request according to AWS4-HMAC-SHA256.
 */
static char *
canonical_request_checksum(char *method, char *datetime, char *objectname,
						   char *contentchecksum, const char *metaToken)
{
	StringInfoData buf;
	unsigned char checksumbuf[32];

	const char *query = strchr(objectname, '?');

	initStringInfo(&buf);
	appendStringInfo(&buf, "%s\n", method);
	/* a query string comes canonical already: sorted and encoded */
	if (query)
		appendStringInfo(&buf, "/%.*s\n%s\n", (int) (query - objectname),
						 objectname, query + 1);
	else
		appendStringInfo(&buf, "/%s\n\n", objectname);
	appendStringInfo(&buf, "host:%s\n", s3_host);
	appendStringInfo(&buf, "x-amz-content-sha256:%s\n", contentchecksum);
	appendStringInfo(&buf, "x-amz-date:%s\n", datetime);
	if (metaToken)
		appendStringInfo(&buf, "x-amz-meta-token:%s\n", metaToken);
	appendStringInfo(&buf, "\n");
	appendStringInfo(&buf, "%s\n", metaToken ? SIGNED_HEADERS_TOKEN : SIGNED_HEADERS);
	appendStringInfo(&buf, "%s", contentchecksum);

	(void) SHA256((unsigned char *) buf.data, buf.len, checksumbuf);
	pfree(buf.data);

	return hex_string((Pointer) checksumbuf, sizeof(checksumbuf));
}

/*
 * Construct signed string for the Authorization header,
 * following the Amazon S3 REST API spec.
 */
static char *
s3_signature(char *method, char *datetimestring, char *datestring,
			 char *objectname, char *secretkey, char *checksumstring,
			 const char *metaToken)
{
	StringInfoData buf;
	char	   *key;
	char		checksumbuf[32];
	char	   *canonical_checksum;

	canonical_checksum = canonical_request_checksum(method, datetimestring,
													objectname, checksumstring,
													metaToken);

	key = psprintf("AWS4%s", s3_secretkey);
	hmac_sha256(datestring, checksumbuf, key, strlen(key));
	hmac_sha256(s3_region, checksumbuf, checksumbuf, sizeof(checksumbuf));
	hmac_sha256("s3", checksumbuf, checksumbuf, sizeof(checksumbuf));
	hmac_sha256("aws4_request", checksumbuf, checksumbuf, sizeof(checksumbuf));

	initStringInfo(&buf);
	appendStringInfo(&buf, "AWS4-HMAC-SHA256\n");
	appendStringInfo(&buf, "%s\n", datetimestring);
	appendStringInfo(&buf, "%s/%s/s3/aws4_request\n", datestring, s3_region);
	appendStringInfo(&buf, "%s", canonical_checksum);

	hmac_sha256(buf.data, checksumbuf, checksumbuf, sizeof(checksumbuf));

	pfree(key);
	pfree(canonical_checksum);
	pfree(buf.data);

	return hex_string(checksumbuf, 32);
}

/*
 * Constructs GMT-style string for date.
 */
static char *
httpdate(time_t *timer)
{
	char	   *datetimestring;
	time_t		t;
	struct tm  *gt;

	t = time(timer);
	gt = gmtime(&t);
	datetimestring = (char *) palloc0(256 * sizeof(char));
	strftime(datetimestring, 256 * sizeof(char), "%Y%m%d", gt);
	return datetimestring;
}

/*
 * Constructs GMT-style string for date and time.
 */
static char *
httpdatetime(time_t *timer)
{
	char	   *datetimestring;
	time_t		t;
	struct tm  *gt;

	t = time(timer);
	gt = gmtime(&t);
	datetimestring = (char *) palloc0(256 * sizeof(char));
	strftime(datetimestring, 256 * sizeof(char), "%Y%m%dT%H%M%SZ", gt);
	return datetimestring;
}

/*
 * Curl callback, which appends data to String Info.
 */
static size_t
write_data_to_buf(void *buffer, size_t size, size_t nmemb, void *userp)
{
	size_t		segsize = size * nmemb;
	StringInfo	info = (StringInfo) userp;

	appendBinaryStringInfo(info, (const char *) buffer, segsize);

	return segsize;
}

/*
 * Response headers a caller may want: the ETag, and the store's clock.
 */
typedef struct
{
	char	   *etag;
	char	   *date;
	char	   *metaToken;
	long		status;			/* the last status line's code, 0 before it */
} S3ResponseHeaders;

/*
 * A GET is read into memory up to this much.  What a server process reads
 * whole (control, root, maps, manifests, lists, a part) is far smaller; a WAL
 * segment, up to 1 GB, is compared with the local bytes as it arrives instead
 * (s3_object_holds).  Past 1 GB the StringInfo failed inside the curl
 * callback, where an ERROR cannot safely leave the transfer.
 */
#define S3_MAX_BUFFERED_GET	(UINT64CONST(256) * 1024 * 1024)

/*
 * What a request does with the response body: keeps it, up to the bound, or
 * compares a 200's body with the bytes given.
 */
typedef struct
{
	StringInfo	body;
	S3ResponseHeaders *headers;
	bool		overflow;		/* the body passed S3_MAX_BUFFERED_GET */
	Pointer		compareData;	/* when set: compare, do not keep */
	uint64		compareSize;
	uint64		received;
	bool		mismatch;
} S3ResponseBody;

static size_t
write_response_body(void *buffer, size_t size, size_t nmemb, void *userp)
{
	size_t		segsize = size * nmemb;
	S3ResponseBody *resp = (S3ResponseBody *) userp;

	if (resp->compareData && resp->headers->status == S3_RESPONSE_OK)
	{
		if (!resp->mismatch &&
			(resp->received + segsize > resp->compareSize ||
			 memcmp(buffer, resp->compareData + resp->received, segsize) != 0))
			resp->mismatch = true;
		resp->received += segsize;
		return segsize;
	}

	if ((uint64) resp->body->len + segsize > S3_MAX_BUFFERED_GET)
	{
		/* stop the transfer; the caller reports it once curl is done */
		resp->overflow = true;
		return 0;
	}
	appendBinaryStringInfo(resp->body, (const char *) buffer, segsize);
	return segsize;
}

static char *
header_value(char *buffer, size_t len, size_t namelen)
{
	char	   *start = buffer + namelen,
			   *end = buffer + len;

	while (start < end && (*start == ' ' || *start == '\t'))
		start++;
	while (end > start && (end[-1] == '\r' || end[-1] == '\n' || end[-1] == ' '))
		end--;
	return pnstrdup(start, end - start);
}

static size_t
read_response_headers(char *buffer, size_t size, size_t nitems, void *userp)
{
	size_t		len = size * nitems;
	S3ResponseHeaders *headers = (S3ResponseHeaders *) userp;

	if (len > 5 && pg_strncasecmp(buffer, "HTTP/", 5) == 0)
	{
		char	   *space = memchr(buffer, ' ', len);

		if (space && space + 1 < buffer + len)
			headers->status = strtol(space + 1, NULL, 10);
	}
	else if (len > 5 && pg_strncasecmp(buffer, "etag:", 5) == 0)
	{
		if (headers->etag)
			pfree(headers->etag);
		headers->etag = header_value(buffer, len, 5);
	}
	else if (len > 5 && pg_strncasecmp(buffer, "date:", 5) == 0)
	{
		if (headers->date)
			pfree(headers->date);
		headers->date = header_value(buffer, len, 5);
	}
	else if (len > 17 && pg_strncasecmp(buffer, "x-amz-meta-token:", 17) == 0)
	{
		if (headers->metaToken)
			pfree(headers->metaToken);
		headers->metaToken = header_value(buffer, len, 17);
	}
	return len;
}

/*
 * Whether a failed request is worth repeating: the request did not reach the
 * store or the store did not handle it (a transport error, 5xx, 429).  Any
 * other answer is the store's verdict on this request and repeating it
 * changes nothing.
 */
static bool
s3_request_is_transient(int sc, long http_code)
{
	return sc != 0 || http_code >= 500 || http_code == 429;
}

/*
 * Pause before the attempt-th retry: exponential from 100 ms, capped at
 * 60 s; a store asking to slow down (503, 429) starts at 1 s.  Interrupts are
 * served while waiting, so a process told to stop does not sit out an
 * outage.
 */
/*
 * Called while a request waits to be retried: the WAL shipper answers its
 * stream's keepalives from it, or the walsender would time out on it.
 */
void		(*s3_request_wait_hook) (void) = NULL;

int			s3_auth_retry_time = 300;
bool		s3_postmaster_retries = false;

/*
 * The S3 bgworkers (lease, GC, S3 workers, WAL shipper) own their reloads:
 * they take a pending one here, on every pass of their loops and while a
 * request waits to be retried.
 */
void
s3_take_config_reload(void)
{
	if (ConfigReloadPending)
	{
		ConfigReloadPending = false;
		ProcessConfigFile(PGC_SIGHUP);
	}
}

/*
 * A client backend takes a pending reload here as the bgworkers do: new
 * credentials are what a request refused as unauthorized waits for, and the
 * backend read them only between commands, so it sat out the whole retry
 * time with the revoked key (ProcessConfigFile is safe inside a command, as
 * autovacuum's delay point shows).  Any other server process waiting to retry
 * (the checkpointer, the archiver) reads the new configuration, so new
 * credentials reach it, but leaves the reload pending: its own loop then
 * does what it does on a reload as well (the checkpointer updates shared
 * memory from it).  Read once per reload.
 */
static void
request_wait_reload(void)
{
	static bool readPending = false;

	if (!ConfigReloadPending)
	{
		readPending = false;
		return;
	}
	if (MyBackendType == B_BG_WORKER || MyBackendType == B_BACKEND)
		s3_take_config_reload();
	else if (MyBackendType != B_BACKEND && !readPending)
	{
		ProcessConfigFile(PGC_SIGHUP);
		readPending = true;
	}
}

static void
s3_request_backoff(int attempt, long http_code)
{
	long		base = (http_code == 503 || http_code == 429) ? 1000L : 100L;
	long		delay = base;
	int			i;

	for (i = 0; i < attempt && delay < 60000L; i++)
		delay *= 2;
	delay = Min(delay, 60000L);

	if (IsUnderPostmaster)
	{
		/* in slices, so a waiting process can keep up its own duties */
		while (delay > 0)
		{
			long		slice = Min(delay, 5000L);

			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 slice, PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
			request_wait_reload();
			delay -= slice;
			if (s3_request_wait_hook)
				s3_request_wait_hook();
		}
	}
	else
		pg_usleep(delay * 1000L);
}

/*
 * One S3 request, repeated while the failure is transient (a lost
 * connection, a 5xx, a 429): an outage of the store is waited out, not turned
 * into an error.  The answer of the last attempt is returned; the caller
 * judges it.
 *
 * method: "GET", "PUT" or "DELETE".  data/dataSize/dataChecksum: the PUT body
 * (dataChecksum may be NULL).  ifMatch: an ETag the object must still have,
 * ifNoneMatch: the object must not exist (PUT only).  body receives the
 * response body; etag, when not NULL, the response's ETag (palloc'd, or NULL).
 */
/*
 * A fresh connection per request, and the store is asked to close it: the
 * closing side keeps the connection in TIME_WAIT, and a WAL shipper putting
 * a chunk per group of commits ran this machine out of ephemeral ports when
 * it closed them itself (connects failing with curl error 7 until the lease
 * ran out).  Connections are not reused: a store may answer a PUT before it
 * reads its body and keep the connection, and the next request on it then
 * gets an answer that is not its own (a DELETE answered 412).
 */
static CURL *
request_handle(struct curl_slist **slist)
{
	CURL	   *curl;

	*slist = curl_slist_append(*slist, "Connection: close");
	curl = curl_easy_init();
	if (curl == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("could not initialize curl")));
	return curl;
}

/*
 * When set, no request attempt runs past it, and a failure is not retried
 * once it is reached: the caller holds something (the writer lease) only
 * until then.
 */
static TimestampTz requestDeadline = 0;
static long fenced_put(char *objectname, Pointer data, uint64 dataSize,
					   char *dataChecksum);

static bool
request_deadline_passed(void)
{
	return requestDeadline != 0 && GetCurrentTimestamp() >= requestDeadline;
}

/*
 * What a request does beyond s3_request()'s arguments.  authRetry: a 403 is
 * retried for orioledb.s3_auth_retry_time (off for a request nobody waits
 * for).  compareData: a 200's body is compared with these bytes as it arrives
 * instead of being kept; same says whether it was them.
 */
typedef struct
{
	bool		authRetry;
	Pointer		compareData;
	uint64		compareSize;
	bool		same;
} S3RequestOpts;

/*
 * A transfer that makes no progress for this long is given up
 * (CURLE_OPERATION_TIMEDOUT) and retried as a transport failure: a store that
 * accepts a request and never answers, or a connection that died without a
 * reset, held the request for ever, as nothing bounded a request without a
 * deadline.  Keepalives tell a dead peer apart sooner.
 */
#define S3_LOW_SPEED_TIME	60L

static long
s3_request_ext(const char *method, char *objectname,
			   Pointer data, uint64 dataSize, char *dataChecksum,
			   const char *ifMatch, bool ifNoneMatch, bool retry, long timeout,
			   StringInfo body, char **etag, char **date, int *curlCode,
			   const char *metaToken, char **respMetaToken,
			   S3RequestOpts *opts)
{
	S3ResponseHeaders headers = {NULL, NULL, NULL, 0};
	S3ResponseBody resp = {body, &headers, false, NULL, 0, 0, false};
	char	   *objectpath = objectname;
	char	   *checksumstringbuf;
	long		http_code = 0;
	int			sc = 0;
	int			attempt;
	TimestampTz authSince = 0;

	if (opts)
	{
		resp.compareData = opts->compareData;
		resp.compareSize = opts->compareSize;
		opts->same = false;
	}

	if (dataChecksum == NULL)
	{
		unsigned char checksumbuf[SHA256_DIGEST_LENGTH];

		(void) SHA256((unsigned char *) data, data ? dataSize : 0, checksumbuf);
		checksumstringbuf = hex_string((Pointer) checksumbuf, sizeof(checksumbuf));
	}
	else
		checksumstringbuf = dataChecksum;

	/* a path from the store's root: a bucket-level request */
	if (objectname[0] == '/')
		objectpath = objectname + 1;
	else if (s3_prefix && objectname[0] != '?')
	{
		int			prefix_len = strlen(s3_prefix);

		if (prefix_len != 0)
		{
			if (s3_prefix[prefix_len - 1] == '/')
				prefix_len--;
			objectpath = psprintf("%.*s/%s", prefix_len, s3_prefix, objectname);
		}
	}

	for (attempt = 0;; attempt++)
	{
		CURL	   *curl;
		char	   *url;
		char	   *datestring;
		char	   *datetimestring;
		char	   *signature;
		struct curl_slist *slist = NULL;
		char	   *tmp;

		url = psprintf("%s://%s/%s",
					   s3_use_https ? "https" : "http", s3_host, objectpath);
		datestring = httpdate(NULL);
		datetimestring = httpdatetime(NULL);
		signature = s3_signature((char *) method, datetimestring, datestring,
								 objectpath, s3_secretkey, checksumstringbuf,
								 metaToken);

		slist = curl_slist_append(slist, (tmp = psprintf("x-amz-date: %s", datetimestring)));
		pfree(tmp);
		slist = curl_slist_append(slist, (tmp = psprintf("x-amz-content-sha256: %s", checksumstringbuf)));
		pfree(tmp);
		if (metaToken)
		{
			slist = curl_slist_append(slist, (tmp = psprintf("x-amz-meta-token: %s", metaToken)));
			pfree(tmp);
		}
		if (strcmp(method, "PUT") == 0)
		{
			slist = curl_slist_append(slist,
									  (tmp = psprintf("Content-Length: " UINT64_FORMAT,
													  dataSize)));
			pfree(tmp);
		}
		slist = curl_slist_append(slist,
								  (tmp = psprintf("Authorization: AWS4-HMAC-SHA256 Credential=%s/%s/%s/s3/aws4_request, SignedHeaders=%s, Signature=%s",
												  s3_accesskey, datestring, s3_region,
												  metaToken ? SIGNED_HEADERS_TOKEN : SIGNED_HEADERS,
												  signature)));
		pfree(tmp);
		if (strcmp(method, "PUT") == 0)
		{
			slist = curl_slist_append(slist, "Content-Type: application/octet-stream");

			/*
			 * No "Expect: 100-continue": on a kept-alive connection a late
			 * interim answer was taken for the answer of the next request.
			 */
			slist = curl_slist_append(slist, "Expect:");
		}
		if (ifNoneMatch)
			slist = curl_slist_append(slist, "If-None-Match: *");
		if (ifMatch)
		{
			slist = curl_slist_append(slist, (tmp = psprintf("If-Match: %s", ifMatch)));
			pfree(tmp);
		}

		resetStringInfo(body);
		if (headers.etag)
			pfree(headers.etag);
		if (headers.date)
			pfree(headers.date);
		if (headers.metaToken)
			pfree(headers.metaToken);
		headers.etag = headers.date = headers.metaToken = NULL;
		headers.status = 0;
		resp.overflow = false;
		resp.received = 0;
		resp.mismatch = false;

		curl = request_handle(&slist);
		if (strcmp(method, "HEAD") == 0)
			curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
		else if (strcmp(method, "GET") != 0)
			curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, slist);
		curl_easy_setopt(curl, CURLOPT_URL, url);
		if (s3_cainfo)
			curl_easy_setopt(curl, CURLOPT_CAINFO, s3_cainfo);
		if (strcmp(method, "PUT") == 0)
		{
			curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data);
			curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, dataSize);
		}
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_response_body);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
		curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, read_response_headers);
		curl_easy_setopt(curl, CURLOPT_HEADERDATA, &headers);
		curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1L);
		curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, S3_LOW_SPEED_TIME);
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
		if (timeout > 0)
			curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout);
		if (requestDeadline != 0)
		{
			long		left = (long) ((requestDeadline - GetCurrentTimestamp()) / 1000);

			curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, Max(left, 1L));
		}

		sc = curl_easy_perform(curl);
		http_code = 0;
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
		curl_easy_cleanup(curl);

		curl_slist_free_all(slist);
		pfree(url);
		pfree(datestring);
		pfree(datetimestring);
		pfree(signature);

		if (resp.overflow)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("S3 object \"%s\" is too large to read into memory",
							objectname),
					 errdetail("Its body passed %llu MB; a request reads at most that much.",
							   (unsigned long long) (S3_MAX_BUFFERED_GET / (1024 * 1024)))));

		/*
		 * At startup (the postmaster) an unreachable store is reported at
		 * once: it is as likely to be a wrong setting as an outage, and the
		 * service manager restarts us.  A running server waits it out.  A
		 * crash restart takes the lease again in the postmaster: a setting
		 * that worked a moment ago, so a transient failure is retried for
		 * about a minute rather than end the postmaster.
		 */
		/*
		 * A body the store received cut short (BadDigest, IncompleteBody) or
		 * not in time (RequestTimeout) was lost on the way and refused: send
		 * it again.
		 */
		/*
		 * Refused as unauthorized: the keys may be being rotated (the new
		 * ones arrive with a configuration reload), or the store's view of
		 * them is catching up.  Retried for orioledb.s3_auth_retry_time from
		 * the first refusal, then the refusal is the answer.
		 */
		if (http_code == 403 && sc == 0)
		{
			if (authSince == 0)
				authSince = GetCurrentTimestamp();
		}
		else
			authSince = 0;
		if (!retry ||
			(!IsUnderPostmaster && (!s3_postmaster_retries || attempt >= 6)) ||
			request_deadline_passed() ||
			!(s3_request_is_transient(sc, http_code) ||
			  (authSince != 0 && s3_auth_retry_time > 0 &&
			   (opts == NULL || opts->authRetry) &&
			   !TimestampDifferenceExceeds(authSince, GetCurrentTimestamp(),
										   s3_auth_retry_time * 1000)) ||
			  (http_code == 400 && body->data &&
			   (strstr(body->data, "<Code>BadDigest</Code>") != NULL ||
				strstr(body->data, "<Code>IncompleteBody</Code>") != NULL ||
				strstr(body->data, "<Code>RequestTimeout</Code>") != NULL))))
			break;

		if (attempt % 10 == 0)
			ereport(WARNING,
					(errmsg("S3 %s of \"%s\" failed, retrying", method, objectname),
					 errdetail("attempt %d, return code = %d, http code = %ld",
							   attempt + 1, sc, http_code)));
		s3_request_backoff(attempt, http_code);
	}

	if (objectpath != objectname && objectpath != objectname + 1)
		pfree(objectpath);
	if (checksumstringbuf != dataChecksum)
		pfree(checksumstringbuf);
	if (curlCode)
		*curlCode = sc;
	if (etag)
		*etag = headers.etag;
	else if (headers.etag)
		pfree(headers.etag);
	if (date)
		*date = headers.date;
	else if (headers.date)
		pfree(headers.date);
	if (respMetaToken)
		*respMetaToken = headers.metaToken;
	else if (headers.metaToken)
		pfree(headers.metaToken);
	if (opts && opts->compareData)
		opts->same = sc == 0 && http_code == S3_RESPONSE_OK &&
			!resp.mismatch && resp.received == resp.compareSize;

	return http_code;
}

static long
s3_request(const char *method, char *objectname,
		   Pointer data, uint64 dataSize, char *dataChecksum,
		   const char *ifMatch, bool ifNoneMatch, bool retry, long timeout,
		   StringInfo body, char **etag, char **date, int *curlCode,
		   const char *metaToken, char **respMetaToken)
{
	return s3_request_ext(method, objectname, data, dataSize, dataChecksum,
						  ifMatch, ifNoneMatch, retry, timeout, body, etag,
						  date, curlCode, metaToken, respMetaToken, NULL);
}

/* URI-encode for a query string: everything but the unreserved characters */
static void
append_uri_encoded(StringInfo buf, const char *s)
{
	for (; *s; s++)
	{
		unsigned char c = (unsigned char) *s;

		if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' ||
			c == '~')
			appendStringInfoChar(buf, c);
		else
			appendStringInfo(buf, "%%%02X", c);
	}
}

/* the text of each <tag>...</tag> of an XML answer, from *pos on */
static char *
xml_next(const char *body, const char *tag, const char **pos)
{
	char	   *open = psprintf("<%s>", tag);
	char	   *close = psprintf("</%s>", tag);
	const char *start = strstr(*pos, open);
	const char *end;
	char	   *result = NULL;

	if (start)
	{
		start += strlen(open);
		end = strstr(start, close);
		if (end)
		{
			result = pnstrdup(start, end - start);
			*pos = end + strlen(close);
		}
	}
	pfree(open);
	pfree(close);
	return result;
}

/*
 * Where a bucket-level request (a listing, the versioning state) goes.  An
 * object's key is orioledb.s3_prefix/<name> after orioledb.s3_host, and that
 * prefix holds either key prefix only (the bucket is in the host: a virtual
 * host, or a host given with the bucket's path) or, first, the bucket itself
 * (path-style addressing with the host alone).  The store's root answers a
 * listing with its buckets in the second case: then the bucket is the
 * prefix's first segment.
 */
static bool bucketResolved = false;
static char bucketPath[MAXPGPATH];	/* "" or "/<bucket>/" */
static char keyPrefix[MAXPGPATH];	/* "" or "<key prefix>/" */

static void
resolve_bucket_address(void)
{
	StringInfoData query;
	StringInfoData body;
	long		http_code;
	int			sc;
	int			len;

	if (bucketResolved)
		return;
	bucketPath[0] = '\0';
	keyPrefix[0] = '\0';
	len = s3_prefix ? strlen(s3_prefix) : 0;
	while (len > 0 && s3_prefix[len - 1] == '/')
		len--;
	if (len == 0)
	{
		bucketResolved = true;
		return;
	}
	snprintf(keyPrefix, sizeof(keyPrefix), "%.*s/", len, s3_prefix);

	initStringInfo(&query);
	appendStringInfoString(&query, "?list-type=2&max-keys=1&prefix=");
	append_uri_encoded(&query, keyPrefix);
	initStringInfo(&body);
	http_code = s3_request("GET", query.data, NULL, 0, NULL, NULL, false,
						   true, 0, &body, NULL, NULL, &sc, NULL, NULL);
	if (!(sc == 0 && http_code == S3_RESPONSE_OK &&
		  strstr(body.data, "<ListBucketResult") != NULL))
	{
		const char *slash = memchr(s3_prefix, '/', len);
		int			bucketLen = slash ? (int) (slash - s3_prefix) : len;

		snprintf(bucketPath, sizeof(bucketPath), "/%.*s/", bucketLen, s3_prefix);
		if (slash)
			snprintf(keyPrefix, sizeof(keyPrefix), "%.*s/",
					 len - bucketLen - 1, slash + 1);
		else
			keyPrefix[0] = '\0';
	}
	pfree(query.data);
	pfree(body.data);
	bucketResolved = true;
}

/*
 * The keys under prefix that sort after startAfter (NULL: all of them), in
 * order, without orioledb.s3_prefix: ListObjectsV2, page by page.
 */
List *
s3_list_objects(const char *prefix, const char *startAfter)
{
	List	   *keys = NIL;
	char	   *token = NULL;
	int			skip;

	resolve_bucket_address();
	skip = strlen(keyPrefix);

	for (;;)
	{
		StringInfoData query;
		StringInfoData body;
		const char *pos;
		char	   *key;
		char	   *truncated;
		long		http_code;
		int			sc;

		/* the parameters in their canonical order */
		initStringInfo(&query);
		appendStringInfo(&query, "%s?", bucketPath);
		if (token)
		{
			appendStringInfoString(&query, "continuation-token=");
			append_uri_encoded(&query, token);
			appendStringInfoChar(&query, '&');
		}
		appendStringInfoString(&query, "list-type=2&prefix=");
		append_uri_encoded(&query, keyPrefix);
		append_uri_encoded(&query, prefix);
		if (startAfter && !token)
		{
			appendStringInfoString(&query, "&start-after=");
			append_uri_encoded(&query, keyPrefix);
			append_uri_encoded(&query, startAfter);
		}

		initStringInfo(&body);
		http_code = s3_request("GET", query.data, NULL, 0, NULL, NULL, false,
							   true, 0, &body, NULL, NULL, &sc, NULL, NULL);
		if (sc != 0 || http_code != S3_RESPONSE_OK ||
			strstr(body.data, "<ListBucketResult") == NULL)
			ereport(FATAL,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("could not list objects in S3"),
					 errdetail("prefix \"%s\", return code = %d, http code = %ld, response = %s",
							   prefix, sc, http_code, body.data)));

		pos = body.data;
		while ((key = xml_next(body.data, "Key", &pos)) != NULL)
		{
			keys = lappend(keys, pstrdup(key + skip));
			pfree(key);
		}
		pos = body.data;
		truncated = xml_next(body.data, "IsTruncated", &pos);
		if (token)
			pfree(token);
		pos = body.data;
		token = (truncated && strcmp(truncated, "true") == 0) ?
			xml_next(body.data, "NextContinuationToken", &pos) : NULL;
		if (truncated)
			pfree(truncated);
		pfree(query.data);
		pfree(body.data);
		if (token == NULL)
			break;
	}
	return keys;
}

/*
 * Refuse a bucket with versioning on or suspended: a delete there leaves a
 * marker and the old version, so the GC frees nothing, and a conditional
 * put's "the key is free" is judged against the delete markers.
 */
void
s3_check_bucket_versioning(void)
{
	StringInfoData query;
	StringInfoData body;
	long		http_code;
	int			sc;

	resolve_bucket_address();
	initStringInfo(&query);
	appendStringInfo(&query, "%s?versioning=", bucketPath);
	initStringInfo(&body);
	http_code = s3_request("GET", query.data, NULL, 0, NULL, NULL, false,
						   true, 0, &body, NULL, NULL, &sc, NULL, NULL);
	if (sc == 0 && http_code == S3_RESPONSE_OK)
	{
		if (strstr(body.data, "<Status>Enabled</Status>") ||
			strstr(body.data, "<Status>Suspended</Status>"))
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("the S3 bucket has versioning on or suspended"),
					 errhint("Use a bucket whose versioning was never enabled.")));
	}
	else
		ereport(WARNING,
				(errmsg("could not read the versioning state of the S3 bucket"),
				 errdetail("return code = %d, http code = %ld", sc, http_code)));
	pfree(query.data);
	pfree(body.data);
}

#define DELETE_CONCURRENCY	16

typedef struct
{
	CURL	   *curl;
	struct curl_slist *slist;
	char	   *url;
	StringInfoData body;
	int			index;
} DeleteSlot;

static void
delete_slot_start(CURLM *multi, DeleteSlot *slot, char *objectname,
				  int index, char *emptyChecksum)
{
	char	   *objectpath = objectname;
	char	   *datestring = httpdate(NULL);
	char	   *datetimestring = httpdatetime(NULL);
	char	   *signature;
	char	   *tmp;

	if (s3_prefix && s3_prefix[0])
	{
		int			prefix_len = strlen(s3_prefix);

		if (s3_prefix[prefix_len - 1] == '/')
			prefix_len--;
		objectpath = psprintf("%.*s/%s", prefix_len, s3_prefix, objectname);
	}
	signature = s3_signature("DELETE", datetimestring, datestring, objectpath,
							 s3_secretkey, emptyChecksum, NULL);
	slot->slist = NULL;
	slot->slist = curl_slist_append(slot->slist, (tmp = psprintf("x-amz-date: %s", datetimestring)));
	pfree(tmp);
	slot->slist = curl_slist_append(slot->slist, (tmp = psprintf("x-amz-content-sha256: %s", emptyChecksum)));
	pfree(tmp);
	slot->slist = curl_slist_append(slot->slist,
									(tmp = psprintf("Authorization: AWS4-HMAC-SHA256 Credential=%s/%s/%s/s3/aws4_request, SignedHeaders=%s, Signature=%s",
													s3_accesskey, datestring, s3_region,
													SIGNED_HEADERS, signature)));
	pfree(tmp);
	slot->url = psprintf("%s://%s/%s", s3_use_https ? "https" : "http",
						 s3_host, objectpath);
	initStringInfo(&slot->body);
	slot->index = index;
	slot->curl = request_handle(&slot->slist);
	curl_easy_setopt(slot->curl, CURLOPT_CUSTOMREQUEST, "DELETE");
	curl_easy_setopt(slot->curl, CURLOPT_HTTPHEADER, slot->slist);
	curl_easy_setopt(slot->curl, CURLOPT_URL, slot->url);
	if (s3_cainfo)
		curl_easy_setopt(slot->curl, CURLOPT_CAINFO, s3_cainfo);
	curl_easy_setopt(slot->curl, CURLOPT_WRITEFUNCTION, write_data_to_buf);
	curl_easy_setopt(slot->curl, CURLOPT_WRITEDATA, &slot->body);
	curl_easy_setopt(slot->curl, CURLOPT_TIMEOUT, 60L);
	curl_easy_setopt(slot->curl, CURLOPT_PRIVATE, slot);
	curl_multi_add_handle(multi, slot->curl);

	if (objectpath != objectname)
		pfree(objectpath);
	pfree(datestring);
	pfree(datetimestring);
	pfree(signature);
}

/*
 * Delete many objects, DELETE_CONCURRENCY requests at a time.  A request that
 * fails is done again on its own with s3_delete_object(), which retries; an
 * object already gone counts as deleted.  An interrupt stops starting new
 * requests and is served once those in flight are done; when serving it does
 * not end the call, the rest are deleted.
 */
void
s3_delete_objects(char **objectnames, int count)
{
	CURLM	   *multi;
	DeleteSlot	slots[DELETE_CONCURRENCY];
	bool		busy[DELETE_CONCURRENCY] = {0};
	List	   *failed = NIL;
	ListCell   *lc;
	int			next = 0,
				active = 0,
				stoppedAt = -1;
	unsigned char checksumbuf[SHA256_DIGEST_LENGTH];
	char	   *emptyChecksum;

	if (count == 0)
		return;
	(void) SHA256((unsigned char *) "", 0, checksumbuf);
	emptyChecksum = hex_string((Pointer) checksumbuf, sizeof(checksumbuf));
	multi = curl_multi_init();

	while (next < count || active > 0)
	{
		int			running,
					queued;
		CURLMsg    *msg;

		if (InterruptPending && INTERRUPTS_CAN_BE_PROCESSED() && next < count)
		{
			stoppedAt = next;
			next = count;
		}
		for (int i = 0; i < DELETE_CONCURRENCY && next < count; i++)
		{
			if (busy[i])
				continue;
			delete_slot_start(multi, &slots[i], objectnames[next], next,
							  emptyChecksum);
			busy[i] = true;
			next++;
			active++;
		}

		(void) curl_multi_perform(multi, &running);
		(void) curl_multi_poll(multi, NULL, 0, 1000, NULL);

		while ((msg = curl_multi_info_read(multi, &queued)) != NULL)
		{
			DeleteSlot *slot;
			long		http_code = 0;

			if (msg->msg != CURLMSG_DONE)
				continue;
			curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE, (char **) &slot);
			curl_easy_getinfo(msg->easy_handle, CURLINFO_RESPONSE_CODE, &http_code);
			if (msg->data.result != CURLE_OK ||
				(http_code != 204 && http_code != 200 &&
				 http_code != S3_RESPONSE_NOT_FOUND))
				failed = lappend_int(failed, slot->index);
			curl_multi_remove_handle(multi, slot->curl);
			curl_easy_cleanup(slot->curl);
			slot->curl = NULL;
			curl_slist_free_all(slot->slist);
			pfree(slot->url);
			pfree(slot->body.data);
			busy[slot - slots] = false;
			active--;
		}
	}
	curl_multi_cleanup(multi);
	pfree(emptyChecksum);

	foreach(lc, failed)
		s3_delete_object(objectnames[lfirst_int(lc)]);
	list_free(failed);

	if (stoppedAt >= 0)
	{
		CHECK_FOR_INTERRUPTS();
		/* the interrupt did not end this process: go on */
		s3_delete_objects(objectnames + stoppedAt, count - stoppedAt);
	}
}

/*
 * Get the binary content of an object from S3 into 'str', and its ETag into
 * *etag when etag is not NULL.
 *
 * Returns HTTP status code.
 */
long
s3_get_object_etag(char *objectname, StringInfo str, bool missing_ok,
				   char **etag)
{
	long		http_code;
	int			sc;

	http_code = s3_request("GET", objectname, NULL, 0, NULL, NULL, false, true, 0,
						   str, etag, NULL, &sc, NULL, NULL);

	if (sc != 0 || http_code != S3_RESPONSE_OK)
	{
		if (missing_ok && http_code == S3_RESPONSE_NOT_FOUND)
		{
			/* Do nothing just return http_code */
		}
		else
			ereport(FATAL, (errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("could not get object from S3"),
							errdetail("object \"%s\", return code = %d, http code = %ld, response = %s",
									  objectname, sc, http_code, str->data)));
	}

	return http_code;
}

long
s3_get_object(char *objectname, StringInfo str, bool missing_ok)
{
	return s3_get_object_etag(objectname, str, missing_ok, NULL);
}

/*
 * A SQL function to get object from S3.  Currently only used for debugging
 * purposes.
 */
Datum
s3_get(PG_FUNCTION_ARGS)
{
	StringInfoData buf;

	initStringInfo(&buf);

	s3_get_object(text_to_cstring(PG_GETARG_TEXT_PP(0)), &buf, false);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * Delete an object from an S3 bucket.
 */
void
s3_delete_object(char *objectname)
{
	StringInfoData buf;
	long		http_code;
	int			sc;

	initStringInfo(&buf);
	http_code = s3_request("DELETE", objectname, NULL, 0, NULL, NULL, false, true, 0,
						   &buf, NULL, NULL, &sc, NULL, NULL);

	/* an object already gone is deleted */
	if (sc != 0 ||
		(http_code != 204 && http_code != 200 && http_code != S3_RESPONSE_NOT_FOUND))
		ereport(FATAL, (errcode(ERRCODE_CONNECTION_EXCEPTION),
						errmsg("could not delete object from S3"),
						errdetail("return code = %d, http code = %ld, response = %s",
								  sc, http_code, buf.data)));

	pfree(buf.data);
}

/*
 * Reads the part of the file 'filename' from 'offset' with length 'maxSize'.
 * The actual length might appear to be lower, it's to be written to '*size'.
 */
static Pointer
read_file_part(const char *filename, uint64 offset,
			   uint64 maxSize, uint64 *size)
{
	int			file;
	Pointer		buffer,
				ptr;
	uint64		totalSize;

	file = BasicOpenFile(filename, O_RDONLY | PG_BINARY);
	if (file < 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", filename)));
		return NULL;
	}

	totalSize = lseek(file, 0, SEEK_END);
	totalSize = Min(totalSize, offset + maxSize);
	*size = Max(totalSize, offset) - offset;
	buffer = (Pointer) MemoryContextAllocHuge(CurrentMemoryContext, *size);

	ptr = buffer;
	while (offset < totalSize)
	{
		int			amount = Min(totalSize - offset, BLCKSZ);
		int			rc;

		pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
		rc = pg_pread(file, ptr, amount, offset);
		pgstat_report_wait_end();

		if (rc < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", filename)));
			return NULL;
		}

		if (rc != amount)
		{
			amount = rc;
			*size = (ptr - buffer) + amount;
			break;
		}

		offset += amount;
		ptr += amount;
	}

	if (close(file) != 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", filename)));

	return buffer;
}

/*
 * Writes the part of the file 'filename' from 'offset' with length 'size'.
 */
static void
write_file_part(const char *filename, uint64 offset,
				Pointer data, uint64 size)
{
	File		file;
	int			rc;

	file = PathNameOpenFile(filename, O_CREAT | O_RDWR | PG_BINARY);
	if (file < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", filename)));
		return;
	}

	rc = FileWrite(file, data, size, offset, WAIT_EVENT_DATA_FILE_WRITE);

	if (rc < 0 || rc != size)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", filename)));
		return;
	}

	FileWriteback(file, offset, size, WAIT_EVENT_DATA_FILE_FLUSH);

	FileClose(file);
}

/*
 * Read the whole file.
 */
Pointer
read_file(const char *filename, uint64 *size)
{
	return read_file_part(filename, 0, UINT64_MAX, size);
}

/*
 * Write the whole file.
 */
static void
write_file(const char *filename, Pointer data, uint64 size)
{
	write_file_part(filename, 0, data, size);
}

void
s3_write_file(const char *filename, Pointer data, uint64 size)
{
	write_file(filename, data, size);
}

/*
 * Put an object under a condition: ifMatch, the ETag the object must still
 * have; ifNoneMatch, the object must not exist.  Returns the HTTP status: 200,
 * or the store's refusal of the condition (412, 409; 404 for ifMatch on an
 * object that is gone) for the caller to judge; anything else is an error.
 * The new object's ETag goes to *etag when etag is not NULL.
 */
long
s3_put_object_conditional(char *objectname, Pointer data, uint64 dataSize,
						  const char *ifMatch, bool ifNoneMatch, char **etag)
{
	StringInfoData buf;
	long		http_code;
	int			sc;

	initStringInfo(&buf);
	http_code = s3_request("PUT", objectname, data, dataSize, NULL,
						   ifMatch, ifNoneMatch, true, 0, &buf, etag, NULL, &sc,
						   NULL, NULL);

	if (sc != 0 || http_code != S3_RESPONSE_OK || strlen(buf.data) != 0)
	{
		if ((ifNoneMatch || ifMatch) &&
			(http_code == S3_RESPONSE_CONDITION_FAILED ||
			 http_code == S3_RESPONSE_CONDITION_CONFLICT ||
			 (ifMatch && http_code == S3_RESPONSE_NOT_FOUND)))
		{
			/* the caller judges a refused condition */
		}
		else
			ereport(FATAL, (errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("could not put object to S3"),
							errdetail("return code = %d, http code = %ld, response = %s",
									  sc, http_code, buf.data)));
	}

	pfree(buf.data);
	return http_code;
}

/*
 * One attempt of at most timeout seconds, nothing retried and nothing raised:
 * for a caller that has its own deadline (the writer lease).  Returns the
 * HTTP status, 0 when no answer came; the response body, ETag and the store's
 * Date header go to body, *etag and *date (palloc'd, NULL when absent).
 */
long
s3_request_once(const char *method, char *objectname, Pointer data,
				uint64 dataSize, const char *ifMatch, bool ifNoneMatch,
				long timeout, StringInfo body, char **etag, char **date)
{
	return s3_request(method, objectname, data, dataSize, NULL, ifMatch,
					  ifNoneMatch, false, timeout, body, etag, date, NULL,
					  NULL, NULL);
}

/*
 * Put object with given binary contents to S3.
 *
 * If dataChecksum is NULL the function calculates checksum of the content.
 *
 * Returns HTTP status code.
 */
long
s3_put_object_with_contents(char *objectname, Pointer data, uint64 dataSize,
							char *dataChecksum, bool ifNoneMatch)
{
	StringInfoData buf;
	long		http_code;
	int			sc;

	initStringInfo(&buf);
	http_code = s3_request("PUT", objectname, data, dataSize, dataChecksum,
						   NULL, ifNoneMatch, true, 0, &buf, NULL, NULL, &sc,
						   NULL, NULL);

	if (sc != 0 || http_code != S3_RESPONSE_OK || strlen(buf.data) != 0)
	{
		/*
		 * Return false if PUT failed to upload object it already exists in
		 * the bucket.
		 */
		if (ifNoneMatch && (http_code == S3_RESPONSE_CONDITION_FAILED ||
							http_code == S3_RESPONSE_CONDITION_CONFLICT))
		{
			/* Do nothing just return http_code */
		}
		else
			ereport(FATAL, (errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("could not put object to S3"),
							errdetail("return code = %d, http code = %ld, response = %s",
									  sc, http_code, buf.data)));
	}

	pfree(buf.data);
	return http_code;
}

/*
 * Whether the object holds exactly these bytes.
 */
static bool
s3_object_holds(char *objectname, Pointer data, uint64 dataSize)
{
	StringInfoData body;
	bool		same;
	long		code;
	int			sc;
	S3RequestOpts opts = {.authRetry = true,.compareData = data,
	.compareSize = dataSize};

	/* compared as it arrives: a WAL segment does not fit a StringInfo */
	initStringInfo(&body);
	code = s3_request_ext("GET", objectname, NULL, 0, NULL, NULL, false, true,
						  0, &body, NULL, NULL, &sc, NULL, NULL, &opts);
	if (sc == 0 && code == S3_RESPONSE_OK)
		same = opts.same;
	else if (code == S3_RESPONSE_NOT_FOUND || request_deadline_passed())
		same = false;
	else
		ereport(FATAL, (errcode(ERRCODE_CONNECTION_EXCEPTION),
						errmsg("could not get object from S3"),
						errdetail("object \"%s\", return code = %d, http code = %ld",
								  objectname, sc, code)));
	pfree(body.data);
	return same;
}

/*
 * Put an object that its writer writes once: under If-None-Match, tagged with
 * the writer's token.  A key that is taken by a larger token means another
 * writer took the bucket over, and this instance stops.  Otherwise a key that
 * already holds these bytes is done: an earlier attempt of ours, or the same
 * content committed before.  A key holding other bytes is replaced only when
 * a writer we fenced off left it (a smaller token), or when it is our own
 * token's -- an attempt cut short before a crash restart, which keeps the
 * token.  An untagged object is not ours to remove.
 */
long
s3_put_object_fenced(char *objectname, Pointer data, uint64 dataSize,
					 char *dataChecksum)
{
	return s3_put_object_fenced_until(objectname, data, dataSize,
									  dataChecksum, 0);
}

/*
 * The same, but given up at deadline (0: never): returns 0 then, having put
 * nothing past it.
 */
long
s3_put_object_fenced_until(char *objectname, Pointer data, uint64 dataSize,
						   char *dataChecksum, TimestampTz deadline)
{
	long		result;

	requestDeadline = deadline;
	PG_TRY();
	{
		result = fenced_put(objectname, data, dataSize, dataChecksum);
	}
	PG_FINALLY();
	{
		requestDeadline = 0;
	}
	PG_END_TRY();
	return result;
}

static long
fenced_put(char *objectname, Pointer data, uint64 dataSize,
		   char *dataChecksum)
{
	StringInfoData buf;
	char		token[32];
	uint64		mine = s3_lease_token();

	snprintf(token, sizeof(token), UINT64_FORMAT, mine);
	initStringInfo(&buf);

	for (int attempt = 0;; attempt++)
	{
		long		http_code;
		int			sc;
		char	   *author = NULL;
		uint64		authorToken = 0;

		if (request_deadline_passed())
		{
			pfree(buf.data);
			return 0;
		}
		if (attempt > 0)
			s3_request_backoff(attempt - 1, 0);
		http_code = s3_request("PUT", objectname, data, dataSize, dataChecksum,
							   NULL, true, true, 0, &buf, NULL, NULL, &sc,
							   token, NULL);
		if (sc == 0 && http_code == S3_RESPONSE_OK)
			break;
		if (sc != 0 && request_deadline_passed())
			continue;
		if (sc != 0 || (http_code != S3_RESPONSE_CONDITION_FAILED &&
						http_code != S3_RESPONSE_CONDITION_CONFLICT))
			ereport(FATAL, (errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("could not put object to S3"),
							errdetail("return code = %d, http code = %ld, response = %s",
									  sc, http_code, buf.data)));

		http_code = s3_request("HEAD", objectname, NULL, 0, NULL, NULL, false,
							   true, 0, &buf, NULL, NULL, &sc, NULL, &author);
		if (http_code == S3_RESPONSE_NOT_FOUND ||
			(sc != 0 && request_deadline_passed()))
			continue;
		if (sc != 0 || http_code != S3_RESPONSE_OK)
			ereport(FATAL, (errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("could not read object metadata from S3"),
							errdetail("object \"%s\", return code = %d, http code = %ld",
									  objectname, sc, http_code)));
		if (author)
		{
			authorToken = strtou64(author, NULL, 10);
			pfree(author);
		}

		if (authorToken > mine)
			s3_lease_lost(psprintf("Object \"%s\" was written by writer token "
								   UINT64_FORMAT ", this instance holds "
								   UINT64_FORMAT ".",
								   objectname, authorToken, mine));
		if (s3_object_holds(objectname, data, dataSize))
			break;
		if (authorToken == 0)
			ereport(FATAL,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("S3 object \"%s\" holds other content and no writer token",
							objectname),
					 errdetail("An object no writer tagged is not replaced.")));

		ereport(LOG,
				(errmsg("replacing \"%s\" left by writer token " UINT64_FORMAT
						" with other content", objectname, authorToken)));
		s3_delete_object(objectname);
	}

	pfree(buf.data);
	return S3_RESPONSE_OK;
}

/*
 * Whether the object is in the bucket.  An answer the store does not give
 * (an outage past the retries, a refusal) counts as no.
 */
bool
s3_object_exists(char *objectname)
{
	StringInfoData buf;
	long		http_code;
	int			sc;

	initStringInfo(&buf);
	http_code = s3_request("HEAD", objectname, NULL, 0, NULL, NULL, false,
						   true, 0, &buf, NULL, NULL, &sc, NULL, NULL);
	pfree(buf.data);
	return sc == 0 && http_code == S3_RESPONSE_OK;
}

/*
 * Put the whole file as S3 object.
 */
#define S3_MAX_SINGLE_PUT	(UINT64CONST(5) * 1024 * 1024 * 1024)

long
s3_put_file(char *objectname, char *filename, bool ifNoneMatch)
{
	Pointer		data;
	uint64		dataSize = 0;
	long		res = -1;
	struct stat st;

	/*
	 * One PUT carries at most 5 GB.  A PostgreSQL data file is split at
	 * RELSEG_SIZE (1 GB by default): only a build with a larger one gets here.
	 */
	if (stat(filename, &st) == 0 && (uint64) st.st_size > S3_MAX_SINGLE_PUT)
		ereport(FATAL,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("file \"%s\" is too large to put to S3 in one request",
						filename),
				 errdetail("It holds %lld bytes; S3 takes at most 5 GB per PUT.",
						   (long long) st.st_size)));

	data = read_file(filename, &dataSize);
	if (data)
	{
		res = s3_put_object_with_contents(objectname, data, dataSize, NULL,
										  ifNoneMatch);
		pfree(data);
	}

	return res;
}

/*
 * Get the whole file from S3 object.
 */
void
s3_get_file(char *objectname, char *filename)
{
	StringInfoData buf;

	initStringInfo(&buf);
	s3_get_object(objectname, &buf, false);

	write_file(filename,
			   (Pointer) buf.data,
			   buf.len);

	pfree(buf.data);
}

/*
 * Put the file part as S3 object.
 */
long
s3_put_file_part(char *objectname, char *filename, int partnum)
{
	Pointer		data;
	uint64		dataSize;
	long		res = -1;

	data = read_file_part(filename,
						  partnum * ORIOLEDB_S3_PART_SIZE + ORIOLEDB_BLCKSZ,
						  ORIOLEDB_S3_PART_SIZE,
						  &dataSize);
	if (data)
	{
		res = s3_put_object_fenced(objectname, data, dataSize, NULL);
		pfree(data);
	}

	return res;
}

/*
 * Get the file part from S3 object.
 */
void
s3_get_file_part(char *objectname, char *filename, int partnum)
{
	StringInfoData buf;

	initStringInfo(&buf);
	s3_get_object(objectname, &buf, false);

	write_file_part(filename,
					partnum * ORIOLEDB_S3_PART_SIZE + ORIOLEDB_BLCKSZ,
					buf.data,
					buf.len);

	pfree(buf.data);
}

/*
 * Get the file part as s3_get_file_part() does, but answer false where the
 * store did not give it, instead of ending the process: a backend downloading
 * the part it needs leaves the part to the S3 workers then.  Transient
 * failures are retried as for any request.
 */
bool
s3_try_get_file_part(char *objectname, char *filename, int partnum)
{
	StringInfoData buf;
	long		http_code;
	int			sc;

	initStringInfo(&buf);
	http_code = s3_request("GET", objectname, NULL, 0, NULL, NULL, false, true,
						   0, &buf, NULL, NULL, &sc, NULL, NULL);
	if (sc != 0 || http_code != S3_RESPONSE_OK)
	{
		pfree(buf.data);
		return false;
	}

	write_file_part(filename,
					partnum * ORIOLEDB_S3_PART_SIZE + ORIOLEDB_BLCKSZ,
					buf.data,
					buf.len);

	pfree(buf.data);
	return true;
}

/*
 * Put empty dir as S3 object.
 */
void
s3_put_empty_dir(char *objectname)
{
	s3_put_object_with_contents(objectname, NULL, 0, NULL, false);
}

/*
 * A SQL function to put object to S3.  Currently only used for debugging
 * purposes.
 */
Datum
s3_put(PG_FUNCTION_ARGS)
{
	char	   *objectname,
			   *filename;

	objectname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	filename = text_to_cstring(PG_GETARG_TEXT_PP(1));

	s3_put_file(objectname, filename, false);

	PG_RETURN_NULL();
}
