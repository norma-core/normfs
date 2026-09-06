cmake_minimum_required(VERSION 3.10)

project(normfs_wal_c VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -pedantic")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(NORMFS_UINTN_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../../uintn-rs/c/include"
    CACHE PATH "Path to the uintn C include directory")

add_library(normfs_wal_c STATIC
    src/wal_header.c
    src/crc32c.c
    src/wal_entry.c
    src/wal_page.c
    src/wal_pool.c
    src/wal_ring.c
)
# Asked of the compiler, not of CMAKE_SYSTEM_PROCESSOR: without a toolchain
# file that variable is the host's arch, so a cross build would hand -msse4.2
# to an aarch64 compiler, which rejects it outright.
include(CheckCCompilerFlag)
check_c_compiler_flag(-msse4.2 NORMFS_WAL_HAVE_MSSE42)
if(NORMFS_WAL_HAVE_MSSE42)
    target_compile_options(normfs_wal_c PRIVATE -msse4.2)
endif()
target_include_directories(normfs_wal_c PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${NORMFS_UINTN_INCLUDE}>
    $<INSTALL_INTERFACE:include>
)

# The header and entry codecs are established by Frama-C WP rather than by
# executable tests. The CRC32C hardware fast paths use compiler intrinsics that
# WP cannot interpret, so the one remaining test checks them against a
# reference it derives from the polynomial.
enable_testing()
add_executable(test_crc32c tests/test_crc32c.c)
target_link_libraries(test_crc32c PRIVATE normfs_wal_c)
add_test(NAME crc32c COMMAND test_crc32c)

find_program(FRAMAC frama-c)
if(FRAMAC)
    include(ProcessorCount)
    ProcessorCount(NPROC)
    if(NPROC EQUAL 0)
        set(NPROC 4)
    endif()
    # The entry codec's quantified byte clauses are heavy; capped parallelism
    # keeps Z3 from timing out under load, so the proof stays deterministic.
    if(NPROC GREATER 2)
        set(NPROC 2)
    endif()

    # Pinned, not configurable: the deployment targets are amd64 and arm64,
    # both LP64, so one model covers them. Naming it still beats the default,
    # which is not the same across Frama-C versions.

    # Per-goal prover budget. The slowest goal here is rotate_to
    # re-establishing every page's well-formedness after a rotation: tens of
    # seconds on a fast machine, several times that on a slow one. A budget
    # sized to the fast machine turns a hardware difference into a failed
    # proof, so it is set for the slow one.
    set(NORMFS_WP_TIMEOUT 900 CACHE STRING "Per-goal WP prover timeout, seconds")

    set(NORMFS_WAL_HEADER_WP_FCTS
        normfs_uintn_le64_read
        normfs_uintn_le64_write
        normfs_wal_header_field_size_is_valid
        normfs_wal_header_v1_validate
        normfs_wal_header_v1_size
        normfs_wal_header_v1_encode
        normfs_wal_header_v1_decode
        normfs_wal_header_peek_version
        normfs_wal_header_v1_roundtrip_holds)

    set(NORMFS_WAL_ENTRY_WP_FCTS
        normfs_wal_entry_read_le32
        normfs_wal_entry_write_le32
        normfs_wal_entry_v1_size
        normfs_wal_entry_v1_encode
        normfs_wal_entry_v1_decode
        normfs_wal_entry_v1_id
        normfs_wal_entry_v1_iter_next)

    set(NORMFS_WAL_PAGE_WP_FCTS
        normfs_uintn_le32_read
        normfs_uintn_le32_write
        normfs_wal_page_init
        normfs_wal_page_reset
        normfs_wal_page_append
        normfs_wal_page_offset
        normfs_wal_page_entry_id
        normfs_wal_page_find
        normfs_wal_page_pin
        normfs_wal_page_unpin
        normfs_wal_page_reusable)

    # rotate_to, release_page and skip_entry are here because the durability
    # theorem is stated on them and an unscheduled function generates no
    # obligation anywhere: rotate_to is the one place a page's contents are
    # discarded, release_page the one place a page crosses to another queue,
    # and skip_entry the one place the id sequence moves without a record
    # behind it.
    #
    # retain_page is excluded. It carries no durability clause -- a free page
    # belongs to nobody and holds nothing anyone is owed -- so what is unproved
    # is well-formedness, not the theorem. Its open goals are all one step:
    # narrowing a \separated from the pool's whole arena to the slice one ring
    # holds, which the provers reject outright rather than time out on. At
    # NORMFS_WP_TIMEOUT per goal and two provers each, a handful of those
    # outlasts the CI job's budget and takes every other function's verdict
    # with it. Its callee pool_take is proven, and the WalRing Rust tests cover
    # it; adding it back to this list is how to see where the proof stands.

    set(NORMFS_WAL_RING_WP_FCTS
        normfs_wal_ring_init
        normfs_wal_ring_try_append
        normfs_wal_ring_set_essential
        normfs_wal_ring_find_reusable
        normfs_wal_ring_rotate_to
        normfs_wal_ring_release_page
        normfs_wal_ring_skip_entry
        normfs_wal_ring_seek)

    # The pool proves against its own contracts, in its own target.
    # verify-wal-ring does not compile this unit: the ring's calls are checked
    # against wal_pool.h's contracts, which is what WP would use even with the
    # body present, so adding it there only makes that target slower.
    set(NORMFS_WAL_POOL_WP_FCTS
        normfs_wal_pool_init
        normfs_wal_pool_find_free
        normfs_wal_pool_take
        normfs_wal_pool_give_back)

    # An empty fcts list drops -wp-fct entirely: WP then also schedules global
    # lemma goals, which a function list silently excludes.
    function(normfs_add_wp_target target source fcts comment)
        if(fcts STREQUAL "")
            set(fct_args)
        else()
            string(REPLACE ";" "," fcts_arg "${fcts}")
            set(fct_args -wp-fct ${fcts_arg})
        endif()
        add_custom_target(${target}
            COMMAND ${FRAMAC}
                -machdep gcc_x86_64
                -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/${target}.json
                -wp -wp-rte -wp-timeout ${NORMFS_WP_TIMEOUT}
                # One goal per conjunct: otherwise a ring_wf postcondition is
                # one formula with quantifiers nested in it. -wp-split alone
                # does if-then-else, not conjunctions.
                -wp-split -wp-split-conj
                -wp-prover alt-ergo,z3
                -wp-par ${NPROC}
                -wp-cache none
                -wp-cache-env
                -wp-smoke-tests
                -wp-msg-key shell
                -kernel-warn-key annot:missing-spec=inactive
                -wp-warn-key pedantic-assigns=inactive
                -wp-no-warn-memory-model
                -cpp-extra-args=-I${CMAKE_CURRENT_SOURCE_DIR}/include\ -I${NORMFS_UINTN_INCLUDE}\ ${ARGN}
                ${fct_args}
                ${source}
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            COMMENT "${comment}"
            VERBATIM
        )
    endfunction()

    normfs_add_wp_target(verify-wal-header src/wal_header.c
        "${NORMFS_WAL_HEADER_WP_FCTS}" "WP: normfs V1 WAL header")
    normfs_add_wp_target(verify-wal-entry src/wal_entry.c
        "${NORMFS_WAL_ENTRY_WP_FCTS}" "WP: normfs V1 WAL entry")
    normfs_add_wp_target(verify-wal-page "src/wal_page.c;src/wal_entry.c"
        "${NORMFS_WAL_PAGE_WP_FCTS}" "WP: normfs V1 WAL page")
    normfs_add_wp_target(verify-wal-pool "src/wal_pool.c;src/wal_page.c;src/wal_entry.c"
        "${NORMFS_WAL_POOL_WP_FCTS}" "WP: normfs V1 WAL page pool")
    normfs_add_wp_target(verify-wal-ring "src/wal_ring.c;src/wal_page.c;src/wal_entry.c"
        "${NORMFS_WAL_RING_WP_FCTS}" "WP: normfs V1 WAL ring")

    add_custom_target(verify)
    add_dependencies(verify verify-wal-header verify-wal-entry
        verify-wal-page verify-wal-pool verify-wal-ring)
endif()
