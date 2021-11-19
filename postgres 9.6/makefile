MODULES		= q-flash
EXTENSION	= q-flash

#OBJS          = $(patsubst %.c,%.o,$(wildcard src/*.c)) # object files
# final shared library to be build from multiple source files (OBJS)
OBJS		= q-flash.o
MODULE_big	= $(EXTENSION)
PG_CONFIG	= pg_config
PGXS		= $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
