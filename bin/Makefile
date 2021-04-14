PROGRAM = pg_save
OBJS = main.o
PG_CONFIG = pg_config
SHLIB_LINK = $(libpq)
PG_CPPFLAGS += -I../include
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
