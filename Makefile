EXTENSION = pg_save
MODULE_big = $(EXTENSION)
OBJS = init.o save.o spi.o primary.o standby.o backend.o varlena.o stringinfo.o
PG_CONFIG = pg_config
SHLIB_LINK = $(libpq)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
