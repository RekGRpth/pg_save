$(OBJS): Makefile
EXTENSION = pg_save
MODULE_big = $(EXTENSION)
OBJS = init.o save.o spi.o primary.o standby.o backend.o ../fe-exec.o
PG_CONFIG = pg_config
PG_CPPFLAGS += -I$(libpq_srcdir)
PG_CPPFLAGS += -I../include
PGXS = $(shell $(PG_CONFIG) --pgxs)
SHLIB_LINK = $(libpq)
include $(PGXS)
