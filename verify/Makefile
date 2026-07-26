# Proof driver for the NormFS C layer.
#
# Usage from the repository root:
#
#   make -f verify/Makefile verify              # all modules, host machine model
#   make -f verify/Makefile verify MACHDEP=gcc_x86_32
#   make -f verify/Makefile verify-wal-header   # one module
#   make -f verify/Makefile docker-verify       # inside the pinned image
#
# Each module already defines its own `verify-*` CMake target with the
# -wp-fct list and flags. This file only configures the build directories
# and fans out, so the flags stay in one place: the module's CMakeLists.txt.

# pipefail matters here: the proof targets pipe frama-c through tee, and
# without it the exit status would be tee's rather than the build's.
SHELL      := /bin/bash
.SHELLFLAGS := -o pipefail -c

ROOT       := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
BUILD      := $(ROOT)/build-verify
MACHDEP    ?= gcc_x86_64
IMAGE      ?= normfs-verify:pinned

LOGS       := $(BUILD)/logs
CHECK      := $(ROOT)/verify/check-proved.sh
SELF       := $(lastword $(MAKEFILE_LIST))

# Discovered, not listed: a hand-maintained mirror of the tree goes stale the
# first time a module is added or renamed.
MODULES    := $(patsubst $(ROOT)/%/CMakeLists.txt,%,$(wildcard $(ROOT)/*/c/CMakeLists.txt))
BUILDDIRS   = $(subst /,_,$(1))

TARGETS    := verify-varint verify-wal-header verify-store-header
dir-verify-varint       := uintn-rs_c
dir-verify-wal-header   := normfs-wal_c
dir-verify-store-header := normfs-store_c

CMAKE_ARGS := -DCMAKE_BUILD_TYPE=Release -DNORMFS_MACHDEP=$(MACHDEP)

.PHONY: all verify $(TARGETS) configure build clean docker-image docker-verify

all: verify

configure:
	@test -n "$(MODULES)" || { echo "no c/CMakeLists.txt found under $(ROOT)" >&2; exit 1; }
	@for m in $(MODULES); do \
	    echo "== configure $$m =="; \
	    cmake -S "$(ROOT)/$$m" -B "$(BUILD)/$$(echo $$m | tr / _)" \
	          $(CMAKE_ARGS) >/dev/null || exit 1; \
	done

# -Werror is part of the claim: the C must compile clean at c99 pedantic.
build: configure
	@for m in $(MODULES); do \
	    echo "== build $$m =="; \
	    cmake --build "$(BUILD)/$$(echo $$m | tr / _)" || exit 1; \
	done

# frama-c exits 0 whether or not it discharges every goal, so each target tees
# its output and has check-proved.sh rule on it instead. One rule, so adding a
# module means adding a CMakeLists and a dir- line, not a fourth copy of this.
$(TARGETS): verify-%: configure
	@mkdir -p "$(LOGS)"
	cmake --build "$(BUILD)/$(dir-verify-$*)" --target verify-$* \
	    2>&1 | tee "$(LOGS)/verify-$*.log"
	@"$(CHECK)" "$(LOGS)/verify-$*.log"

# Runs every module and fails if any one fails, but runs them all first so a
# single broken module does not hide the state of the others.
verify: configure
	@rc=0; \
	for t in $(TARGETS); do \
	    echo "== $$t (MACHDEP=$(MACHDEP)) =="; \
	    $(MAKE) -f "$(SELF)" MACHDEP=$(MACHDEP) $$t || rc=1; \
	done; \
	exit $$rc

docker-image:
	docker build -t $(IMAGE) -f $(ROOT)/verify/Dockerfile $(ROOT)/verify

docker-verify: docker-image
	docker run --rm -v $(ROOT):/work -w /work $(IMAGE) \
	    make -f verify/Makefile verify MACHDEP=$(MACHDEP)

clean:
	rm -rf "$(BUILD)"
