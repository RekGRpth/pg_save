-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_save" to load this file. \quit

create or replace function etcd(location text, request json) returns json language plpgsql as $body$ <<local>> declare
    url text default 'http://localhost:2379/v3';
    response json;
begin
    --perform curl.curl_easy_reset();
    --perform curl.curl_easy_setopt_verbose(1);
    perform curl.curl_easy_setopt_timeout(10);
    perform curl.curl_easy_setopt_url(concat_ws('/', local.url, etcd.location));
    perform curl.curl_easy_setopt_copypostfields(convert_to(etcd.request::text, 'utf-8'));
    perform curl.curl_easy_perform(1);
    local.response = convert_from(curl.curl_easy_getinfo_response(), 'utf-8');
    --raise debug 'location=%, request=%, response=%', location, request, response;
    return local.response;
end;$body$;

create or replace function etcd_kv_range(key text) returns text language plpgsql as $body$ <<local>> declare
    location text default 'kv/range';
    request json;
    response json;
begin
    local.request = json_build_object('key', encode(convert_to(etcd_kv_range.key, 'utf-8'), 'base64'));
    local.response = save.etcd(local.location, local.request);
    return convert_from(decode(local.response->'kvs'->0->>'value', 'base64'), 'utf-8');
end;$body$;

create or replace function etcd_lease_grant(ttl int) returns text language plpgsql as $body$ <<local>> declare
    location text default 'lease/grant';
    request json;
    response json;
begin
    local.request = json_build_object('TTL', etcd_lease_grant.ttl);
    local.response = save.etcd(local.location, local.request);
    return local.response->>'ID';
end;$body$;

create or replace function etcd_kv_put(key text, value text, ttl int default null) returns boolean language plpgsql as $body$ <<local>> declare
    location text default 'kv/put';
    request json;
    response json;
begin
    if etcd_kv_put.ttl is not null then
        local.request = json_build_object('key', encode(convert_to(etcd_kv_put.key, 'utf-8'), 'base64'), 'value', encode(convert_to(etcd_kv_put.value, 'utf-8'), 'base64'), 'lease', save.etcd_lease_grant(etcd_kv_put.ttl));
    else
        local.request = json_build_object('key', encode(convert_to(etcd_kv_put.key, 'utf-8'), 'base64'), 'value', encode(convert_to(etcd_kv_put.value, 'utf-8'), 'base64'));
    end if;
    local.response = save.etcd(local.location, local.request);
    return save.etcd_kv_range(etcd_kv_put.key) is not distinct from etcd_kv_put.value;
end;$body$;
