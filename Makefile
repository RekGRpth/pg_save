MODULE_big = pg_save
OBJS = init.o save.o spi.o
CURL_CONFIG = curl-config
PG_CONFIG = pg_config
CFLAGS += $(shell $(CURL_CONFIG) --cflags)
LIBS += $(shell $(CURL_CONFIG) --libs)
SHLIB_LINK := $(LIBS)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
