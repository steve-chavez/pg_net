#include <stddef.h>
#include <postgres.h>

#include <utils/memutils.h>
#include "mem.h"

void *pg_net_curl_calloc(size_t num, size_t size) {
  return (num > 0 && size > 0) ? palloc0(num * size) : NULL;
}

void *pg_net_curl_realloc(void *dst, size_t size) {
  if (dst && size)
    return repalloc(dst, size);
  else if (size)
    return palloc(size);
  else
    return dst;
}

void pg_net_curl_free(void *dst) {
  if (dst) pfree(dst);
}

void *pg_net_curl_malloc(size_t size) {
  return size ? palloc(size) : NULL;
}

