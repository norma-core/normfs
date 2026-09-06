cmake_minimum_required(VERSION 3.10)

project(normfs_crypto_c VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -pedantic")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(normfs_crypto_c STATIC src/seed.c src/seed_sys.c)
target_include_directories(normfs_crypto_c PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# src/seed.c is established by Frama-C WP. These tests cover what WP cannot
# reach: the syscall shims in normfs/seed_sys.h carry contracts no prover can
# discharge, so they are checked against a real filesystem instead.
enable_testing()
add_executable(test_seed tests/test_seed.c)
target_link_libraries(test_seed PRIVATE normfs_crypto_c)
add_test(NAME seed COMMAND test_seed)

find_program(FRAMAC frama-c)
if(FRAMAC)
    include(ProcessorCount)
    ProcessorCount(NPROC)
    if(NPROC EQUAL 0)
        set(NPROC 4)
    endif()
    if(NPROC GREATER 4)
        set(NPROC 4)
    endif()

    # Every function with a body in src/seed.c, statics included. A function
    # left off this list is silently never scheduled, and check-proved.sh
    # cannot tell that apart from one that was proved.
    set(NORMFS_SEED_WP_FCTS
        normfs_seed_path
        normfs_seed_zero
        normfs_seed_read_exact
        normfs_seed_write_all
        normfs_seed_generate
        normfs_seed_load
        normfs_seed_save
        normfs_seed_exists)
    string(REPLACE ";" "," NORMFS_SEED_WP_FCTS_ARG "${NORMFS_SEED_WP_FCTS}")

    # Pinned, not configurable: the deployment targets are amd64 and arm64,
    # both LP64, so one model covers them. Naming it still beats the default,
    # which is not the same across Frama-C versions.
    #
    # src/seed_sys.c is absent from this command on purpose. Feeding it here
    # would replace the assumed contracts in normfs/seed_sys.h with bodies WP
    # cannot interpret, and would drag in Frama-C's own libc, whose syscall
    # contracts assign no errno at all -- every os_error postcondition below
    # would go from provable to impossible.
    add_custom_target(verify-seed
        COMMAND ${FRAMAC}
            -machdep gcc_x86_64
            -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/verify-seed.json
            -wp -wp-rte -wp-timeout 240
            -wp-prover alt-ergo,z3
            -wp-par ${NPROC}
            -wp-cache none
            -wp-cache-env
            -wp-smoke-tests
            -wp-msg-key shell
            -kernel-warn-key annot:missing-spec=inactive
            -wp-warn-key pedantic-assigns=inactive
            -wp-no-warn-memory-model
            -cpp-extra-args=-I${CMAKE_CURRENT_SOURCE_DIR}/include
            -wp-fct ${NORMFS_SEED_WP_FCTS_ARG}
            src/seed.c
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "WP: normfs crypto seed"
        VERBATIM
    )
endif()
