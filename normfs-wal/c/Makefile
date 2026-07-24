CC ?= cc
AR ?= ar
UINTN_INCLUDE ?= ../../uintn-rs/c/include
CFLAGS ?= -std=c99 -O2 -Wall -Wextra -Werror -pedantic
CPPFLAGS := -Iinclude -I$(UINTN_INCLUDE)

BUILD := build
LIB := $(BUILD)/libnormfs_wal_c.a
OBJ := $(BUILD)/wal_header.o
TEST := $(BUILD)/test_wal_header

.PHONY: all test clean

all: $(LIB)

$(BUILD):
	mkdir -p $(BUILD)

$(OBJ): src/wal_header.c include/normfs/wal_header.h | $(BUILD)
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

$(LIB): $(OBJ)
	$(AR) crs $@ $(OBJ)

$(TEST): tests/test_wal_header.c $(LIB)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(LIB) -o $@

test: $(TEST)
	$(TEST)

clean:
	rm -rf $(BUILD)
