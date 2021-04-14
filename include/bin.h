#ifndef _BIN_H_
#define _BIN_H_

#include "common.h"

#define D(fmt, ...) pg_log_debug(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)
#define E(fmt, ...) pg_log_error(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)
#define F(fmt, ...) pg_log_fatal(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)
#define I(fmt, ...) pg_log_info(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)
#define W(fmt, ...) pg_log_warning(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)

#include <postgres.h>

#include <common/logging.h>
#include <libpq-fe.h>

#endif // _BIN_H_
