#ifndef MEM_H
#define MEM_H

void *pg_net_curl_calloc(size_t num, size_t size);

void *pg_net_curl_realloc(void *dst, size_t size);

void pg_net_curl_free(void *dst);

void *pg_net_curl_malloc(size_t size);

#endif
