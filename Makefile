MODULE_big = pg_save
OBJS = init.o save.o
PG_CONFIG = pg_config
SHLIB_LINK = $(libpq)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
