CC ?= cc
AR ?= ar
UINTN_INCLUDE ?= ../../uintn-rs/c/include
CFLAGS ?= -std=c99 -O2 -Wall -Wextra -Werror -pedantic
CPPFLAGS := -Iinclude -I$(UINTN_INCLUDE)

BUILD := build
LIB := $(BUILD)/libnormfs_store_c.a
OBJ := $(BUILD)/store_header.o
HEADERS := $(wildcard include/normfs/*.h) $(UINTN_INCLUDE)/uintn/varint.h \
           $(UINTN_INCLUDE)/uintn/le.h

.PHONY: all clean

all: $(LIB)

$(BUILD):
	mkdir -p $(BUILD)

$(OBJ): src/store_header.c $(HEADERS) | $(BUILD)
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

$(LIB): $(OBJ)
	$(AR) crs $@ $(OBJ)

clean:
	rm -rf $(BUILD)
