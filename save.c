#include <curl/curl.h>
#include "include.h"
#include <unistd.h>

extern int timeout;
static char *hostname;
static CURL *curl = NULL;
volatile sig_atomic_t sighup = false;
volatile sig_atomic_t sigterm = false;

static void init_sighup(SIGNAL_ARGS) {
    int save_errno = errno;
    sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void init_sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static bytea *pg_save_convert(bytea *string, const char *src_encoding_name, const char *dest_encoding_name) {
    int src_encoding = pg_char_to_encoding(src_encoding_name);
    int dest_encoding = pg_char_to_encoding(dest_encoding_name);
    const char *src_str = VARDATA_ANY(string);
    char *dest_str;
    bytea *retval;
    int len = VARSIZE_ANY_EXHDR(string);
    if (src_encoding < 0) E("invalid source encoding name \"%s\"", src_encoding_name);
    if (dest_encoding < 0) E("invalid destination encoding name \"%s\"", dest_encoding_name);
    pg_verify_mbstr_len(src_encoding, src_str, len, false);
    dest_str = (char *)pg_do_encoding_conversion((unsigned char *)unconstify(char *, src_str), len, src_encoding, dest_encoding);
    if (dest_str != src_str) len = strlen(dest_str);
    retval = (bytea *)palloc(len + VARHDRSZ);
    SET_VARSIZE(retval, len + VARHDRSZ);
    memcpy(VARDATA(retval), dest_str, len);
    if (dest_str != src_str) pfree(dest_str);
    return retval;
}

static bytea *pg_save_convert_to(text *string, const char *encoding_name) {
    const char *src_encoding_name = GetDatabaseEncodingName();
    const char *dest_encoding_name = encoding_name;
    return pg_save_convert(string, src_encoding_name, dest_encoding_name);
}

static text *pg_save_convert_from(bytea *string, const char *encoding_name) {
    const char *src_encoding_name = encoding_name;
    const char *dest_encoding_name = GetDatabaseEncodingName();
    return pg_save_convert(string, src_encoding_name, dest_encoding_name);
}

static CURLcode pg_save_easy_setopt_char(CURLoption option, const char *parameter) {
    CURLcode res = CURL_LAST;
    if ((res = curl_easy_setopt(curl, option, parameter)) != CURLE_OK) E("curl_easy_setopt(%i, %s): %s", option, parameter, curl_easy_strerror(res));
    return res;
}

static CURLcode pg_save_easy_setopt_copypostfields(bytea *parameter) {
    CURLcode res = CURL_LAST;
    if ((res = curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, VARSIZE_ANY_EXHDR(parameter))) != CURLE_OK) E("curl_easy_setopt(CURLOPT_POSTFIELDSIZE): %s", curl_easy_strerror(res));
    if ((res = curl_easy_setopt(curl, CURLOPT_COPYPOSTFIELDS, VARDATA_ANY(parameter))) != CURLE_OK) E("curl_easy_setopt(CURLOPT_COPYPOSTFIELDS): %s", curl_easy_strerror(res));
    PG_RETURN_BOOL(res == CURLE_OK);
    return res;
}

static void save_set(const char *state) {
    text *string = cstring_to_text(state);
    if (pg_save_easy_setopt_char(CURLOPT_URL, "http://localhost:2379/v3/kv/put") != CURLE_OK) return;
    if (pg_save_easy_setopt_copypostfields(pg_save_convert_to(string, "utf-8")) != CURLE_OK) return;
}

static void save_timeout(void) {
    if (!StandbyMode) {
        save_set("main");
    }
}

static void save_socket(void *data) {
}

/*#define SyncStandbysDefined() (SyncRepStandbyNames != NULL && SyncRepStandbyNames[0] != '\0')
static void save_check(void) {
    char name[1024];
    StringInfoData buf;
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    initStringInfo(&buf);
    name[sizeof(name) - 1] = '\0';
    if (gethostname(name, sizeof(name) - 1)) E("gethostname");
    hostname = pstrdup(name);
    MemoryContextSwitchTo(oldMemoryContext);
    D1("hostname = %s, timeout = %i", hostname, timeout);
    if (SyncStandbysDefined()) {
    }
    if (!StandbyMode) {
    }
}*/

static void save_init(void) {
    char name[1024];
    StringInfoData buf;
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    initStringInfo(&buf);
    name[sizeof(name) - 1] = '\0';
    if (gethostname(name, sizeof(name) - 1)) E("gethostname");
    hostname = pstrdup(name);
    MemoryContextSwitchTo(oldMemoryContext);
    D1("hostname = %s, timeout = %i", hostname, timeout);
    if (!EnableHotStandby) E("!EnableHotStandby");
    if (!MyProcPort && !(MyProcPort = (Port *)calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->user_name) MyProcPort->user_name = "postgres";
    if (!MyProcPort->database_name) MyProcPort->database_name = "postgres";
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    if (curl_global_init(CURL_GLOBAL_ALL)) E("curl_global_init");
    if (!(curl = curl_easy_init())) E("!curl_easy_init");
    set_config_option("application_name", MyBgworkerEntry->bgw_type, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pqsignal(SIGHUP, init_sighup);
    pqsignal(SIGTERM, init_sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    process_session_preload_libraries();
}

static void save_fini(void) {
    curl_global_cleanup();
}

static void save_reload(void) {
    sighup = false;
    ProcessConfigFile(PGC_SIGHUP);
}

static void save_latch(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
    if (sighup) save_reload();
}

void save_worker(Datum main_arg); void save_worker(Datum main_arg) {
    TimestampTz stop = GetCurrentTimestamp(), start = stop;
    save_init();
    while (!sigterm) {
        int nevents = 2;
        WaitEvent *events = palloc0(nevents * sizeof(*events));
        WaitEventSet *set = CreateWaitEventSet(TopMemoryContext, nevents);
        AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
        AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
        nevents = WaitEventSetWait(set, timeout, events, nevents, PG_WAIT_EXTENSION);
        for (int i = 0; i < nevents; i++) {
            WaitEvent *event = &events[i];
            if (event->events & WL_LATCH_SET) D1("WL_LATCH_SET");
            if (event->events & WL_SOCKET_READABLE) D1("WL_SOCKET_READABLE");
            if (event->events & WL_SOCKET_WRITEABLE) D1("WL_SOCKET_WRITEABLE");
            if (event->events & WL_POSTMASTER_DEATH) D1("WL_POSTMASTER_DEATH");
            if (event->events & WL_EXIT_ON_PM_DEATH) D1("WL_EXIT_ON_PM_DEATH");
            if (event->events & WL_LATCH_SET) save_latch();
            if (event->events & WL_SOCKET_MASK) save_socket(event->user_data);
        }
        stop = GetCurrentTimestamp();
        if (timeout > 0 && (TimestampDifferenceExceeds(start, stop, timeout) || !nevents)) {
            save_timeout();
            start = stop;
        }
        FreeWaitEventSet(set);
        pfree(events);
    }
    save_fini();
}
