-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_save" to load this file. \quit

create or replace function etcd(location text, request jsonb) returns jsonb language plpgsql as $body$ <<local>> declare
    url text default 'http://localhost:2379/v3';
begin
    --perform curl.curl_easy_reset();
    perform curl.curl_easy_setopt_verbose(1);
    perform curl.curl_easy_setopt_timeout(10);
    perform curl.curl_easy_setopt_url(concat_ws('/', local.url, etcd.location));
    perform curl.curl_easy_setopt_copypostfields(convert_to(etcd.request, 'utf-8'));
    perform curl.curl_easy_perform(1);
    return convert_from(curl.curl_easy_getinfo_response(), 'utf-8');
end;$body$;

create or replace function etcd_kv_range(key text) returns text language plpgsql as $body$ <<local>> declare
    location text default 'kv/range';
    request jsonb;
    response jsonb;
begin
    local.request = jsonb_build_object('key', encode(etcd_kv_range.key, 'base64'));
    local.response = save.etcd(local.location, local.request);
    return decode(local.response->'kvs'->0->>'value', 'base64');
end;$body$;

create or replace function etcd_lease_grant(ttl bigint) returns text language plpgsql as $body$ <<local>> declare
    location text default 'lease/grant';
    request jsonb;
    response jsonb;
begin
    local.request = jsonb_build_object('ttl', etcd_lease_grant.ttl);
    local.response = save.etcd(local.location, local.request);
    return local.response->>'id';
end;$body$;

create or replace function etcd_kv_range(key text, value text, ttl bigint default null) returns text language plpgsql as $body$ <<local>> declare
    location text default 'kv/range';
    request jsonb;
    response jsonb;
begin
    local.request = jsonb_build_object('key', encode(etcd_kv_range.key, 'base64'), 'value', encode(etcd_kv_range.value, 'base64'));
    if etcd_kv_range.ttl is not null then
        local.request = local.request || jsonb_build_object('ttl', etcd.etcd_lease_grant(etcd_kv_range.ttl));
    end if;
    local.response = save.etcd(local.location, local.request);
    return save.etcd_kv_range(etcd_kv_range.key);
end;$body$;
