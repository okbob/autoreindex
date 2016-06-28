
MODULE_big = autoreindex
OBJS = utils.o controller.o reindexdb.o
EXTENSION = autoreindex

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
