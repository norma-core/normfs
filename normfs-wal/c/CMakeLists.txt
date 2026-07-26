cmake_minimum_required(VERSION 3.10)

project(normfs_wal_c VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -pedantic")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(NORMFS_UINTN_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../../uintn-rs/c/include"
    CACHE PATH "Path to the uintn C include directory")

add_library(normfs_wal_c STATIC src/wal_header.c)
target_include_directories(normfs_wal_c PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${NORMFS_UINTN_INCLUDE}>
    $<INSTALL_INTERFACE:include>
)

# Correctness of this library is established by Frama-C WP rather than by
# executable unit tests: every function below is proved against a contract
# that pins the exact bytes it reads and writes.
find_program(FRAMAC frama-c)
if(FRAMAC)
    include(ProcessorCount)
    ProcessorCount(NPROC)
    if(NPROC EQUAL 0)
        set(NPROC 4)
    endif()

    set(NORMFS_WP_FCTS
        normfs_uintn_le64_read
        normfs_uintn_le64_write
        normfs_wal_header_field_size_is_valid
        normfs_wal_header_v1_validate
        normfs_wal_header_v1_size
        normfs_wal_header_v1_encode
        normfs_wal_header_v1_decode
        normfs_wal_header_peek_version
        normfs_wal_header_v1_roundtrip_holds)
    string(REPLACE ";" "," NORMFS_WP_FCTS_ARG "${NORMFS_WP_FCTS}")

    add_custom_target(verify-wal-header
        COMMAND ${FRAMAC}
            -machdep gcc_x86_64
            -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/verify-wal-header.json
            -wp -wp-rte -wp-timeout 120
            -wp-prover alt-ergo,z3
            -wp-par ${NPROC}
            -wp-cache none
            -wp-smoke-tests
            -wp-msg-key shell
            -kernel-warn-key annot:missing-spec=inactive
            -wp-warn-key pedantic-assigns=inactive
            -wp-no-warn-memory-model
            -cpp-extra-args=-I${CMAKE_CURRENT_SOURCE_DIR}/include\ -I${NORMFS_UINTN_INCLUDE}
            -wp-fct ${NORMFS_WP_FCTS_ARG}
            src/wal_header.c
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "WP: normfs V1 WAL header"
        VERBATIM
    )
endif()
