MODULE_big = pg_save
OBJS = init.o save.o spi.o
PG_CONFIG = pg_config
#LIBS += -lgrpc -lpq
#SHLIB_LINK := $(LIBS)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
