#include "include.h"

static Oid etcd_kv_put_oid;
static Oid etcd_kv_range_oid;

static Oid etcd_get_function_oid(const char *schema, const char *function, int nargs, const Oid *argtypes) {
    Oid oid;
    const char *schema_quote = schema ? quote_identifier(schema) : NULL;
    const char *function_quote = quote_identifier(function);
    List *funcname;
    StringInfoData buf;
    initStringInfo(&buf);
    if (schema) appendStringInfo(&buf, "%s.", schema_quote);
    appendStringInfoString(&buf, function_quote);
    funcname = stringToQualifiedNameList(buf.data);
    SPI_connect_my(buf.data);
    oid = LookupFuncName(funcname, nargs, argtypes, false);
    SPI_commit_my();
    SPI_finish_my();
    list_free_deep(funcname);
    if (schema && schema_quote != schema) pfree((void *)schema_quote);
    if (function_quote != function) pfree((void *)function_quote);
    return oid;
}

bool etcd_kv_put(const char *key, const char *value, int ttl) {
    Datum key_datum = CStringGetTextDatum(key);
    Datum value_datum = CStringGetTextDatum(value);
    Datum ok;
    SPI_connect_my("etcd_kv_put");
    ok = OidFunctionCall3(etcd_kv_put_oid, key_datum, value_datum, Int32GetDatum(ttl));
    SPI_commit_my();
    SPI_finish_my();
    pfree((void *)key_datum);
    pfree((void *)value_datum);
    return DatumGetBool(ok);
}

char *etcd_kv_range(const char *key) {
    Datum key_datum = CStringGetTextDatum(key);
    Datum value;
    SPI_connect_my("etcd_kv_range");
    value = OidFunctionCall1(etcd_kv_range_oid, key_datum);
    SPI_commit_my();
    SPI_finish_my();
    pfree((void *)key_datum);
    return TextDatumGetCStringMy(value);
}

void etcd_init(void) {
    etcd_kv_put_oid = etcd_get_function_oid("save", "etcd_kv_put", 3, (Oid []){TEXTOID, TEXTOID, INT4OID});
    etcd_kv_range_oid = etcd_get_function_oid("save", "etcd_kv_range", 1, (Oid []){TEXTOID});
}
