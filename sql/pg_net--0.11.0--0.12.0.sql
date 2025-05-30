alter function net.http_get(text, jsonb, jsonb, integer) security invoker;

alter function net.http_post(text, jsonb, jsonb, jsonb, integer) security invoker;

alter function net.http_delete ( text, jsonb, jsonb, integer) security invoker;

alter function net._http_collect_response ( bigint, boolean) security invoker;

alter function net.http_collect_response ( bigint, boolean) security invoker;

create or replace function net.worker_restart()
  returns bool
  language 'c'
as 'pg_net';

grant usage on schema net to PUBLIC;
grant all on all sequences in schema net to PUBLIC;
grant all on all tables in schema net to PUBLIC;
