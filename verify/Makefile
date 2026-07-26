# Proof driver. Run from the repository root:
#
#   make -f verify/Makefile verify | verify-varint | build | test | docker-verify
#
# Modules own their -wp-fct lists and flags in their CMakeLists.txt; this only
# configures build directories and fans out.

# pipefail: the targets pipe frama-c through tee, whose status is always 0.
SHELL      := /bin/bash
.SHELLFLAGS := -o pipefail -c

ROOT       := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
BUILD      := $(ROOT)/build-verify
LOGS       := $(BUILD)/logs
CHECK      := $(ROOT)/verify/check-proved.sh
SELF       := $(lastword $(MAKEFILE_LIST))
IMAGE      ?= normfs-verify:pinned

# Discovered, not listed, so it cannot go stale.
MODULES    := $(patsubst $(ROOT)/%/CMakeLists.txt,%,$(wildcard $(ROOT)/*/c/CMakeLists.txt))

# Proof targets are discovered the same way, by reading the verify-* targets a
# module declares, so a module that adds one is picked up without editing this
# file. Listing them here is what went stale before: a module landed, its build
# was compiled by the build target, and its proofs were never run.
TARGETS    :=
FIND_TARGETS := sed -n 's/^[[:space:]]*\(add_custom_target\|normfs_add_wp_target\)([[:space:]]*\(verify-[A-Za-z0-9_-]\+\).*/\2/p'
$(foreach m,$(MODULES),$(foreach t,$(shell $(FIND_TARGETS) $(ROOT)/$(m)/CMakeLists.txt),$(eval TARGETS += $(t))$(eval dir-$(t) := $(subst /,_,$(m)))))

CMAKE_ARGS := -DCMAKE_BUILD_TYPE=Release

.PHONY: all verify $(TARGETS) configure build test clean docker-image docker-verify

all: verify

configure:
	@test -n "$(MODULES)" || { echo "no c/CMakeLists.txt found under $(ROOT)" >&2; exit 1; }
	@for m in $(MODULES); do \
	    echo "== configure $$m =="; \
	    cmake -S "$(ROOT)/$$m" -B "$(BUILD)/$$(echo $$m | tr / _)" \
	          $(CMAKE_ARGS) >/dev/null || exit 1; \
	done

build: configure
	@for m in $(MODULES); do \
	    echo "== build $$m =="; \
	    cmake --build "$(BUILD)/$$(echo $$m | tr / _)" || exit 1; \
	done

# A module registering a test with no runner is the same failure as a proof
# target nobody invokes. Modules without tests are skipped, not failed.
test: build
	@for m in $(MODULES); do \
	    echo "== test $$m =="; \
	    ctest --test-dir "$(BUILD)/$$(echo $$m | tr / _)" \
	          --output-on-failure --no-tests=ignore || exit 1; \
	done

# frama-c exits 0 even with goals left open, so check-proved.sh rules instead.
ifneq ($(strip $(TARGETS)),)
$(TARGETS): verify-%: configure
	@mkdir -p "$(LOGS)"
	cmake --build "$(BUILD)/$(dir-verify-$*)" --target verify-$* \
	    2>&1 | tee "$(LOGS)/verify-$*.log"
	@"$(CHECK)" "$(LOGS)/verify-$*.log"
endif

# Runs all targets before failing, so one broken target does not hide the rest.
verify: configure
	@test -n "$(strip $(TARGETS))" || { echo "no verify-* targets found in $(MODULES)" >&2; exit 1; }
	@rc=0; \
	for t in $(TARGETS); do \
	    echo "== $$t =="; \
	    $(MAKE) -f "$(SELF)" $$t || rc=1; \
	done; \
	exit $$rc

docker-image:
	docker build -t $(IMAGE) -f $(ROOT)/verify/Dockerfile $(ROOT)/verify

docker-verify: docker-image
	docker run --rm -v $(ROOT):/work -w /work $(IMAGE) \
	    make -f verify/Makefile verify

clean:
	rm -rf "$(BUILD)"
