CC ?= cc
AR ?= ar
UINTN_INCLUDE ?= ../../uintn-rs/c/include
CFLAGS ?= -std=c99 -O2 -Wall -Wextra -Werror -pedantic
CPPFLAGS := -Iinclude -I$(UINTN_INCLUDE)

BUILD := build
LIB := $(BUILD)/libnormfs_store_c.a
OBJ := $(BUILD)/store_header.o
TEST := $(BUILD)/test_store_header

.PHONY: all test clean

all: $(LIB)

$(BUILD):
	mkdir -p $(BUILD)

$(OBJ): src/store_header.c include/normfs/store_header.h | $(BUILD)
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

$(LIB): $(OBJ)
	$(AR) crs $@ $(OBJ)

$(TEST): tests/test_store_header.c $(LIB)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(LIB) -o $@

test: $(TEST)
	$(TEST)

clean:
	rm -rf $(BUILD)
