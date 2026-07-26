# Proof driver for the NormFS C layer.
#
# Usage from the repository root:
#
#   make -f verify/Makefile verify              # all modules
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
IMAGE      ?= normfs-verify:pinned

LOGS       := $(BUILD)/logs
CHECK      := $(ROOT)/verify/check-proved.sh
SELF       := $(lastword $(MAKEFILE_LIST))

# Discovered, not listed: a hand-maintained mirror of the tree goes stale the
# first time a module is added or renamed.
MODULES    := $(patsubst $(ROOT)/%/CMakeLists.txt,%,$(wildcard $(ROOT)/*/c/CMakeLists.txt))
BUILDDIRS   = $(subst /,_,$(1))

# Targets are discovered the same way, from the verify-* a module declares, so
# the module's CMakeLists stays the only place naming them. A list here is a
# second registry, and the failure when it falls behind is silent: no target
# means no log means nothing for check-proved.sh to rule on.
TARGETS    :=
FIND_TARGETS := sed -n 's/^[[:space:]]*\(add_custom_target\|normfs_add_wp_target\)([[:space:]]*\(verify-[A-Za-z0-9_-]\+\).*/\2/p'
$(foreach m,$(MODULES),$(foreach t,$(shell $(FIND_TARGETS) $(ROOT)/$(m)/CMakeLists.txt),$(eval TARGETS += $(t))$(eval dir-$(t) := $(call BUILDDIRS,$(m)))))

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

# -Werror is part of the claim: the C must compile clean at c99 pedantic.
build: configure
	@for m in $(MODULES); do \
	    echo "== build $$m =="; \
	    cmake --build "$(BUILD)/$$(echo $$m | tr / _)" || exit 1; \
	done

# The proofs cannot check their own gate: a report check-proved.sh wrongly
# accepts is indistinguishable from one it rightly accepts. Needs no frama-c,
# so it runs on the plain build job.
test: build
	@"$(ROOT)/verify/check-proved-test.sh"

# frama-c exits 0 whether or not it discharges every goal, so each target rules
# on its JSON report instead, which carries a verdict per goal. The log is kept
# for summarize.sh and for reading afterwards. One rule, so adding a module
# means adding a CMakeLists, not a fourth copy of this.
#
# The report is removed first: WP reads an existing one back to keep step
# ranges stable across runs, so a run that dies before writing would otherwise
# leave the previous verdicts in place to be checked a second time.
ifneq ($(strip $(TARGETS)),)
$(TARGETS): verify-%: configure
	@mkdir -p "$(LOGS)"
	@rm -f "$(BUILD)/$(dir-verify-$*)/verify-$*.json"
	cmake --build "$(BUILD)/$(dir-verify-$*)" --target verify-$* \
	    2>&1 | tee "$(LOGS)/verify-$*.log"
	@"$(CHECK)" "$(BUILD)/$(dir-verify-$*)/verify-$*.json"
endif

# Runs every module and fails if any one fails, but runs them all first so a
# single broken module does not hide the state of the others.
verify: configure
	@test -n "$(strip $(TARGETS))" || { echo "no verify-* targets declared by $(MODULES)" >&2; exit 1; }
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
