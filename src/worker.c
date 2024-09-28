#include <postgres.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <storage/proc.h>
#include <fmgr.h>
#include <access/xact.h>
#include <executor/spi.h>
#include <utils/snapmgr.h>
#include <commands/extension.h>
#include <catalog/pg_extension.h>
#include <catalog/pg_type.h>
#include <miscadmin.h>
#include <utils/builtins.h>
#include <access/hash.h>
#include <utils/hsearch.h>
#include <utils/memutils.h>
#include <utils/jsonb.h>
#include <utils/guc.h>
#include <tcop/utility.h>

#include <curl/curl.h>
#include <curl/multi.h>

#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <inttypes.h>

#include "util.h"
#include "mem.h"

#define MIN_LIBCURL_VERSION_NUM 0x075300 // This is the 7.83.0 version in hex as defined in curl/curlver.h
_Static_assert(LIBCURL_VERSION_NUM, "libcurl >= 7.83.0 is required"); // test for older libcurl versions that don't even have LIBCURL_VERSION_NUM defined (e.g. libcurl 6.5).
_Static_assert(LIBCURL_VERSION_NUM >= MIN_LIBCURL_VERSION_NUM, "libcurl >= 7.83.0 is required");

#define RECYCLE_MULTI_HANDLE_AT 5000

PG_MODULE_MAGIC;

static char *guc_ttl;
static int guc_batch_size;
static char* guc_database_name;

void _PG_init(void);
PGDLLEXPORT void pg_net_worker(Datum main_arg) pg_attribute_noreturn();

typedef struct {
  int64 id;
  StringInfo body;
  struct curl_slist* request_headers;
  char* url;
  char *reqBody;
  char *method;
} CurlData;

typedef struct {
  int epfd;
  int timerfd;
  CURLM *curl_mhandle;
  size_t multi_handle_count;
} WorkerState;

typedef struct itimerspec itimerspec;
typedef struct epoll_event epoll_event;

static long worker_latch_timeout = 1000;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

static void
handle_sigterm(SIGNAL_ARGS)
{
  int save_errno = errno;
  got_sigterm = true;
  if (MyProc)
    SetLatch(&MyProc->procLatch);
  errno = save_errno;
}

static void
handle_sighup(SIGNAL_ARGS)
{
  int     save_errno = errno;
  got_sighup = true;
  if (MyProc)
    SetLatch(&MyProc->procLatch);
  errno = save_errno;
}

static size_t
body_cb(void *contents, size_t size, size_t nmemb, void *userp)
{
  CurlData *cdata = (CurlData*) userp;
  size_t realsize = size * nmemb;
  appendBinaryStringInfo(cdata->body, (const char*)contents, (int)realsize);
  return realsize;
}

static int multi_timer_cb(CURLM *multi, long timeout_ms, WorkerState *ws) {
  elog(DEBUG2, "multi_timer_cb: Setting timeout to %ld ms\n", timeout_ms);

  itimerspec its =
    timeout_ms > 0 ?
    // assign the timeout normally
    (itimerspec){
      .it_value.tv_sec = timeout_ms / 1000,
      .it_value.tv_nsec = (timeout_ms % 1000) * 1000 * 1000,
    }:
    timeout_ms == 0 ?
    /* libcurl wants us to timeout now, however setting both fields of
     * new_value.it_value to zero disarms the timer. The closest we can
     * do is to schedule the timer to fire in 1 ns. */
    (itimerspec){
      .it_value.tv_sec = 0,
      .it_value.tv_nsec = 1,
    }:
     // libcurl passes a -1 to indicate the timer should be deleted
    (itimerspec){};

  int no_flags = 0;
  if (timerfd_settime(ws->timerfd, no_flags, &its, NULL) < 0) {
    ereport(ERROR, errmsg("timerfd_settime failed"));
  }

  return 0;
}

static int multi_socket_cb(CURL *easy, curl_socket_t sockfd, int what, WorkerState *ws, bool *socket_exists) {
  static char *whatstrs[] = { "NONE", "CURL_POLL_IN", "CURL_POLL_OUT", "CURL_POLL_INOUT", "CURL_POLL_REMOVE" };
  elog(DEBUG2, "multi_socket_cb: received %s", whatstrs[what]);

  epoll_event ev = {.data.fd = sockfd};
  int epoll_op;

  if(socket_exists){
    epoll_op = EPOLL_CTL_MOD;
  } else {
    bool exists = true;
    curl_multi_assign(ws->curl_mhandle, sockfd, &exists);
    epoll_op = EPOLL_CTL_ADD;
  }

  switch (what){
  case CURL_POLL_OUT:
    ev.events = EPOLLOUT;
    break;
  case CURL_POLL_INOUT:
    break;
  case CURL_POLL_IN:
    ev.events = EPOLLIN;
    break;
  case CURL_POLL_REMOVE:
    epoll_op = EPOLL_CTL_DEL;
    bool exists = false;
    curl_multi_assign(ws->curl_mhandle, sockfd, &exists);
    break;
  default:
    ereport(ERROR, errmsg("Unexpected CURL_POLL symbol: %d\n", what));
  }

  if (epoll_ctl(ws->epfd, epoll_op, sockfd, &ev) < 0) {
    int e = errno;
    ereport(ERROR, errmsg("epoll_ctl failed for sockfd %d: %s", sockfd, strerror(e)));
  }

  return 0;
}

static bool is_extension_loaded(){
  Oid extensionOid;

  StartTransactionCommand();

  extensionOid = get_extension_oid("pg_net", true);

  CommitTransactionCommand();

  return OidIsValid(extensionOid);
}

static void delete_expired_responses(char *ttl){
  SetCurrentStatementStartTimestamp();
  StartTransactionCommand();
  PushActiveSnapshot(GetTransactionSnapshot());
  SPI_connect();

  int ret_code = SPI_execute_with_args("\
    WITH\
    rows AS (\
      SELECT ctid\
      FROM net._http_response\
      WHERE created < now() - $1\
      ORDER BY created\
      LIMIT $2\
    )\
    DELETE FROM net._http_response r\
    USING rows WHERE r.ctid = rows.ctid",
    2,
    (Oid[]){INTERVALOID, INT4OID},
    (Datum[]){
      DirectFunctionCall3(interval_in, CStringGetDatum(ttl), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1))
    , Int32GetDatum(guc_batch_size)
    }, NULL, false, 0);

  if (ret_code != SPI_OK_DELETE)
  {
    ereport(ERROR, errmsg("Error expiring response table rows: %s", SPI_result_code_string(ret_code)));
  }

  SPI_finish();
  PopActiveSnapshot();
  CommitTransactionCommand();
}

static void insert_failure_response(CURLcode return_code, int64 id){
  StartTransactionCommand();
  PushActiveSnapshot(GetTransactionSnapshot());
  SPI_connect();

  int ret_code = SPI_execute_with_args("\
      insert into net._http_response(id, error_msg) values ($1, $2)",
      2,
      (Oid[]){INT8OID, CSTRINGOID},
      (Datum[]){Int64GetDatum(id), CStringGetDatum(curl_easy_strerror(return_code))},
      NULL, false, 1);

  if (ret_code != SPI_OK_INSERT)
  {
    ereport(ERROR, errmsg("Error when inserting failed response: %s", SPI_result_code_string(ret_code)));
  }

  SPI_finish();
  PopActiveSnapshot();
  CommitTransactionCommand();
}

static void insert_success_response(CurlData *cdata, long http_status_code, char *contentType, Jsonb *jsonb_headers){
  StartTransactionCommand();
  PushActiveSnapshot(GetTransactionSnapshot());
  SPI_connect();

  int ret_code = SPI_execute_with_args("\
      insert into net._http_response(id, status_code, content, headers, content_type, timed_out) values ($1, $2, $3, $4, $5, $6)",
      6,
      (Oid[]){INT8OID, INT4OID, CSTRINGOID, JSONBOID, CSTRINGOID, BOOLOID},
      (Datum[]){
        Int64GetDatum(cdata->id)
      , Int32GetDatum(http_status_code)
      , CStringGetDatum(cdata->body->data)
      , JsonbPGetDatum(jsonb_headers)
      , CStringGetDatum(contentType)
      , BoolGetDatum(false) // timed_out is false here as it's a success
      },
      (char[6]){
        ' '
      , [2] = cdata->body->data[0] == '\0'? 'n' : ' '
      , [4] = !contentType? 'n' :' '
      },
      false, 1);

  if ( ret_code != SPI_OK_INSERT)
  {
    ereport(ERROR, errmsg("Error when inserting successful response: %s", SPI_result_code_string(ret_code)));
  }

  SPI_finish();
  PopActiveSnapshot();
  CommitTransactionCommand();
}

static void pfree_curl_data(CurlData *cdata){
  pfree(cdata->body->data);
  pfree(cdata->body);
  if(cdata->request_headers) //curl_slist_free_all already handles the NULL case, but be explicit about it
    curl_slist_free_all(cdata->request_headers);
  if(cdata->url)
    pfree(cdata->url);
  if(cdata->reqBody)
    pfree(cdata->reqBody);
  if(cdata->method)
    pfree(cdata->method);
  pfree(cdata);
}

static void init_curl_handle(CURLM *curl_mhandle, int64 id, Datum urlBin, NullableDatum bodyBin, NullableDatum headersBin, Datum methodBin, int32 timeout_milliseconds){
  MemoryContext old_ctx = MemoryContextSwitchTo(TopMemoryContext); // needs to switch context as the CurlData will not live the SPI context

  CurlData *cdata = palloc(sizeof(CurlData));
  cdata->id   = id;
  cdata->body = makeStringInfo();

  if (!headersBin.isnull) {
    ArrayType *pgHeaders = DatumGetArrayTypeP(headersBin.value);
    struct curl_slist *request_headers = NULL;

    request_headers = pg_text_array_to_slist(pgHeaders, request_headers);

    CURL_SLIST_APPEND(request_headers, "User-Agent: pg_net/" EXTVERSION);

    cdata->request_headers = request_headers;
  }

  cdata->url = TextDatumGetCString(urlBin);

  cdata->reqBody = !bodyBin.isnull ? TextDatumGetCString(bodyBin.value) : NULL;

  cdata->method = TextDatumGetCString(methodBin);

  if (strcasecmp(cdata->method, "GET") != 0 && strcasecmp(cdata->method, "POST") != 0 && strcasecmp(cdata->method, "DELETE") != 0) {
    ereport(ERROR, errmsg("Unsupported request method %s", cdata->method));
  }

  CURL *curl_ez_handle = curl_easy_init();
  if(!curl_ez_handle)
    ereport(ERROR, errmsg("curl_easy_init()"));

  if (strcasecmp(cdata->method, "GET") == 0) {
    if (cdata->reqBody) {
      CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_POSTFIELDS, cdata->reqBody);
      CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_CUSTOMREQUEST, "GET");
    }
  }

  if (strcasecmp(cdata->method, "POST") == 0) {
    if (cdata->reqBody) {
      CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_POSTFIELDS, cdata->reqBody);
    }
    else {
      CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_POST, 1);
      CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_POSTFIELDSIZE, 0);
    }
  }

  if (strcasecmp(cdata->method, "DELETE") == 0) {
    CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_CUSTOMREQUEST, "DELETE");
  }

  CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_WRITEFUNCTION, body_cb);
  CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_WRITEDATA, cdata);
  CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_HEADER, 0L);
  CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_URL, cdata->url);
  CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_HTTPHEADER, cdata->request_headers);
  CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_TIMEOUT_MS, timeout_milliseconds);
  CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_PRIVATE, cdata);
  CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_FOLLOWLOCATION, true);
  if (log_min_messages <= DEBUG2)
    CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_VERBOSE, 1L);
#if LIBCURL_VERSION_NUM >= 0x075500 /* libcurl 7.85.0 */
  CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_PROTOCOLS_STR, "http,https");
#else
  CURL_EZ_SETOPT(curl_ez_handle, CURLOPT_PROTOCOLS, CURLPROTO_HTTP | CURLPROTO_HTTPS);
#endif

  EREPORT_MULTI(
    curl_multi_add_handle(curl_mhandle, curl_ez_handle)
  );

  MemoryContextSwitchTo(old_ctx);
}

static void consume_request_queue(CURLM *curl_mhandle){
  StartTransactionCommand();
  PushActiveSnapshot(GetTransactionSnapshot());
  SPI_connect();

  int ret_code = SPI_execute_with_args("\
    WITH\
    rows AS (\
      SELECT id\
      FROM net.http_request_queue\
      ORDER BY id\
      LIMIT $1\
    )\
    DELETE FROM net.http_request_queue q\
    USING rows WHERE q.id = rows.id\
    RETURNING q.id, q.method, q.url, timeout_milliseconds, array(select key || ': ' || value from jsonb_each_text(q.headers)), q.body",
    1,
    (Oid[]){INT4OID},
    (Datum[]){Int32GetDatum(guc_batch_size)},
    NULL, false, 0);

  if (ret_code != SPI_OK_DELETE_RETURNING)
    ereport(ERROR, errmsg("Error getting http request queue: %s", SPI_result_code_string(ret_code)));


  for (int j = 0; j < SPI_processed; j++) {
    bool tupIsNull = false;

    int64 id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[j], SPI_tuptable->tupdesc, 1, &tupIsNull));
    EREPORT_NULL_ATTR(tupIsNull, id);

    int32 timeout_milliseconds = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[j], SPI_tuptable->tupdesc, 4, &tupIsNull));
    EREPORT_NULL_ATTR(tupIsNull, timeout_milliseconds);

    Datum method = SPI_getbinval(SPI_tuptable->vals[j], SPI_tuptable->tupdesc, 2, &tupIsNull);
    EREPORT_NULL_ATTR(tupIsNull, method);

    Datum url = SPI_getbinval(SPI_tuptable->vals[j], SPI_tuptable->tupdesc, 3, &tupIsNull);
    EREPORT_NULL_ATTR(tupIsNull, url);

    NullableDatum headersBin = {
      .value = SPI_getbinval(SPI_tuptable->vals[j], SPI_tuptable->tupdesc, 5, &tupIsNull),
      .isnull = tupIsNull
    };

    NullableDatum bodyBin = {
      .value = SPI_getbinval(SPI_tuptable->vals[j], SPI_tuptable->tupdesc, 6, &tupIsNull),
      .isnull = tupIsNull
    };

    init_curl_handle(curl_mhandle, id, url, bodyBin, headersBin, method, timeout_milliseconds);
  }

  SPI_finish();
  PopActiveSnapshot();
  CommitTransactionCommand();
}

static Jsonb *jsonb_headers_from_curl_handle(CURL *ez_handle){
  struct curl_header *header, *prev = NULL;

  JsonbParseState *headers = NULL;
  (void)pushJsonbValue(&headers, WJB_BEGIN_OBJECT, NULL);

  while((header = curl_easy_nextheader(ez_handle, CURLH_HEADER, 0, prev))) {
    JsonbValue key   = {.type = jbvString, .val = {.string = {.val = header->name,  .len = strlen(header->name)}}};
    JsonbValue value = {.type = jbvString, .val = {.string = {.val = header->value, .len = strlen(header->value)}}};
    (void)pushJsonbValue(&headers, WJB_KEY,   &key);
    (void)pushJsonbValue(&headers, WJB_VALUE, &value);
    prev = header;
  }

  Jsonb *jsonb_headers = JsonbValueToJsonb(pushJsonbValue(&headers, WJB_END_OBJECT, NULL));

  return jsonb_headers;
}

static void insert_curl_responses(WorkerState *ws){
  int msgs_left=0;
  CURLMsg *msg = NULL;
  CURLM *curl_mhandle = ws->curl_mhandle;

  while ((msg = curl_multi_info_read(curl_mhandle, &msgs_left))) {
    if (msg->msg == CURLMSG_DONE) {
      CURLcode return_code = msg->data.result;
      CURL *ez_handle= msg->easy_handle;
      CurlData *cdata = NULL;
      CURL_EZ_GETINFO(ez_handle, CURLINFO_PRIVATE, &cdata);

      if (return_code != CURLE_OK) {
        insert_failure_response(return_code, cdata->id);
      } else {
        char *contentType;
        CURL_EZ_GETINFO(ez_handle, CURLINFO_CONTENT_TYPE, &contentType);

        long http_status_code;
        CURL_EZ_GETINFO(ez_handle, CURLINFO_RESPONSE_CODE, &http_status_code);

        Jsonb *jsonb_headers = jsonb_headers_from_curl_handle(ez_handle);

        insert_success_response(cdata, http_status_code, contentType, jsonb_headers);

        pfree_curl_data(cdata);
      }

      int res = curl_multi_remove_handle(curl_mhandle, ez_handle);
      if(res != CURLM_OK)
        ereport(ERROR, errmsg("curl_multi_remove_handle: %s", curl_multi_strerror(res)));

      curl_easy_cleanup(ez_handle);
    } else {
      ereport(ERROR, errmsg("curl_multi_info_read(), CURLMsg=%d\n", msg->msg));
    }
  }

  ws->multi_handle_count++;
}

void pg_net_worker(Datum main_arg) {
  pqsignal(SIGTERM, handle_sigterm);
  pqsignal(SIGHUP, handle_sighup);
  pqsignal(SIGUSR1, procsignal_sigusr1_handler);

  BackgroundWorkerUnblockSignals();

  BackgroundWorkerInitializeConnection(guc_database_name, NULL, 0);

  elog(LOG, "pg_net_worker started with pid %d and config of: pg_net.ttl=%s, pg_net.batch_size=%d, pg_net.database_name=%s", MyProcPid, guc_ttl, guc_batch_size, guc_database_name);

  int curl_ret = curl_global_init_mem(CURL_GLOBAL_ALL, pg_net_curl_malloc, pg_net_curl_free, pg_net_curl_realloc, pstrdup, pg_net_curl_calloc);
  /*int curl_ret = curl_global_init(CURL_GLOBAL_ALL);*/
  if(curl_ret != CURLE_OK)
    ereport(ERROR, errmsg("curl_global_init() returned %s\n", curl_easy_strerror(curl_ret)));

  WorkerState ws = {
    .epfd = epoll_create1(0),
    .timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC),
    .curl_mhandle = curl_multi_init(),
  };

  if (ws.epfd < 0) {
    ereport(ERROR, errmsg("Failed to create epoll file descriptor"));
  }

  if (ws.timerfd < 0) {
    ereport(ERROR, errmsg("Failed to create timerfd"));
  }

  if(!ws.curl_mhandle)
    ereport(ERROR, errmsg("curl_multi_init()"));

  curl_multi_setopt(ws.curl_mhandle, CURLMOPT_SOCKETFUNCTION, multi_socket_cb);
  curl_multi_setopt(ws.curl_mhandle, CURLMOPT_SOCKETDATA, &ws);
  curl_multi_setopt(ws.curl_mhandle, CURLMOPT_TIMERFUNCTION, multi_timer_cb);
  curl_multi_setopt(ws.curl_mhandle, CURLMOPT_TIMERDATA, &ws);

  timerfd_settime(ws.timerfd, 0, &(itimerspec){}, NULL);

  epoll_ctl(ws.epfd, EPOLL_CTL_ADD, ws.timerfd, &(epoll_event){.events = EPOLLIN, .data.fd = ws.timerfd});

  while (!got_sigterm) {
    WaitLatch(&MyProc->procLatch,
          WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
          worker_latch_timeout,
          PG_WAIT_EXTENSION);
    ResetLatch(&MyProc->procLatch);

    CHECK_FOR_INTERRUPTS();

    if(!is_extension_loaded()){
      elog(DEBUG2, "pg_net_worker: extension not yet loaded");
      continue;
    }

    if (got_sighup) {
      got_sighup = false;
      ProcessConfigFile(PGC_SIGHUP);
    }

    delete_expired_responses(guc_ttl);

    consume_request_queue(ws.curl_mhandle);

    int running_handles = 0;

    epoll_event *events = palloc0(sizeof(epoll_event) * guc_batch_size);

    do {
      int maxevents = guc_batch_size + 1; // 1 extra for the timer
      int timeout = 1000; // 1 second
      int nfds = epoll_wait(ws.epfd, events, maxevents, timeout);
      if (nfds < 0) {
        int save_errno = errno;
        if(save_errno == EINTR) {
          continue;
        }
        else {
          ereport(ERROR, errmsg("epoll_wait() failed: %s", strerror(save_errno)));
          break;
        }
      }

      for (int i = 0; i < nfds; i++) {
        if (events[i].data.fd == ws.timerfd) {
          EREPORT_MULTI(
            curl_multi_socket_action(ws.curl_mhandle, CURL_SOCKET_TIMEOUT, 0, &running_handles)
          );
        } else {
          int ev_bitmask =
            events[i].events & EPOLLIN ? CURL_CSELECT_IN:
            events[i].events & EPOLLOUT ? CURL_CSELECT_OUT:
            CURL_CSELECT_ERR;

          EREPORT_MULTI(
            curl_multi_socket_action(
              ws.curl_mhandle, events[i].data.fd,
              ev_bitmask,
              &running_handles)
          );

          if(running_handles <= 0) {
            elog(DEBUG2, "last transfer done, kill timeout");
            timerfd_settime(ws.timerfd, 0, &(itimerspec){0}, NULL);
          }
        }

        insert_curl_responses(&ws);
      }

    } while (running_handles > 0); // run again while there are curl handles, this will prevent waiting for the latch_timeout (which will cause the cause the curl timeouts to be wrong)

    pfree(events);

    if (ws.multi_handle_count >= RECYCLE_MULTI_HANDLE_AT) {
      elog(LOG, "recycling curl multi handle at: %zu", ws.multi_handle_count);
      curl_ret = curl_multi_cleanup(ws.curl_mhandle);
      if(curl_ret != CURLM_OK)
        ereport(ERROR, errmsg("curl_multi_cleanup: %s", curl_multi_strerror(curl_ret)));

      curl_global_cleanup();

      close(ws.epfd);
      close(ws.timerfd);

      ws.epfd = epoll_create1(0);
      ws.timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);

      if (ws.epfd < 0) {
        ereport(ERROR, errmsg("Failed to create epoll file descriptor"));
      }

      if (ws.timerfd < 0) {
        ereport(ERROR, errmsg("Failed to create timerfd"));
      }

      timerfd_settime(ws.timerfd, 0, &(itimerspec){}, NULL);

      epoll_ctl(ws.epfd, EPOLL_CTL_ADD, ws.timerfd, &(epoll_event){.events = EPOLLIN, .data.fd = ws.timerfd});

      curl_ret = curl_global_init(CURL_GLOBAL_ALL);
      if(curl_ret != CURLE_OK)
        ereport(ERROR, errmsg("curl_global_init() returned %s\n", curl_easy_strerror(curl_ret)));

      ws.curl_mhandle = curl_multi_init(),
      curl_multi_setopt(ws.curl_mhandle, CURLMOPT_SOCKETFUNCTION, multi_socket_cb);
      curl_multi_setopt(ws.curl_mhandle, CURLMOPT_SOCKETDATA, &ws);
      curl_multi_setopt(ws.curl_mhandle, CURLMOPT_TIMERFUNCTION, multi_timer_cb);
      curl_multi_setopt(ws.curl_mhandle, CURLMOPT_TIMERDATA, &ws);

      ws.multi_handle_count = 0;
    }

  }

  close(ws.epfd);
  close(ws.timerfd);
  curl_global_cleanup();

  // causing a failure on exit will make the postmaster process restart the bg worker
  proc_exit(EXIT_FAILURE);
}

void _PG_init(void) {
  if (IsBinaryUpgrade) {
      return;
  }

  if (!process_shared_preload_libraries_in_progress) {
      ereport(ERROR, errmsg("pg_net is not in shared_preload_libraries"),
              errhint("Add pg_net to the shared_preload_libraries "
                      "configuration variable in postgresql.conf."));
  }

  RegisterBackgroundWorker(&(BackgroundWorker){
    .bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION,
    .bgw_start_time = BgWorkerStart_RecoveryFinished,
    .bgw_library_name = "pg_net",
    .bgw_function_name = "pg_net_worker",
    .bgw_name = "pg_net " EXTVERSION " worker",
    .bgw_restart_time = 1,
  });

  DefineCustomStringVariable("pg_net.ttl",
                 "time to live for request/response rows",
                 "should be a valid interval type",
                 &guc_ttl,
                 "6 hours",
                 PGC_SUSET, 0,
                 NULL, NULL, NULL);

  DefineCustomIntVariable("pg_net.batch_size",
                 "number of requests executed in one iteration of the background worker",
                 NULL,
                 &guc_batch_size,
                 200,
                 0, PG_INT16_MAX,
                 PGC_SUSET, 0,
                 NULL, NULL, NULL);

  DefineCustomStringVariable("pg_net.database_name",
                "Database where pg_net tables are located",
                NULL,
                &guc_database_name,
                "postgres",
                PGC_SIGHUP, 0,
                NULL, NULL, NULL);
}
