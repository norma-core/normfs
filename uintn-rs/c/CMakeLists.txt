cmake_minimum_required(VERSION 3.10)

project(normfs_uintn_c VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -pedantic")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(normfs_uintn_c STATIC src/varint.c)
target_include_directories(normfs_uintn_c PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

find_program(FRAMAC frama-c)
if(FRAMAC)
    include(ProcessorCount)
    ProcessorCount(NPROC)
    if(NPROC EQUAL 0)
        set(NPROC 4)
    endif()

    set(NORMFS_UINTN_VARINT_WP_FCTS
        normfs_uintn_varint32_size
        normfs_uintn_varint64_size
        normfs_uintn_varint32_encode
        normfs_uintn_varint64_encode
        normfs_uintn_varint32_decode_ok
        normfs_uintn_varint64_decode_ok
        normfs_uintn_varint32_decode
        normfs_uintn_varint64_decode
        normfs_uintn_varint32_size_ffi
        normfs_uintn_varint64_size_ffi
        normfs_uintn_varint32_encode_ffi
        normfs_uintn_varint64_encode_ffi
        normfs_uintn_varint32_decode_ffi
        normfs_uintn_varint64_decode_ffi)
    string(REPLACE ";" "," NORMFS_UINTN_VARINT_WP_FCTS_ARG
        "${NORMFS_UINTN_VARINT_WP_FCTS}")

    add_custom_target(verify-varint
        COMMAND ${FRAMAC}
            -machdep gcc_x86_64
            -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/verify-varint.json
            -wp -wp-rte -wp-timeout 60
            -wp-prover alt-ergo,z3
            -wp-par ${NPROC}
            -wp-cache none
            -wp-smoke-tests
            -wp-msg-key shell
            -kernel-warn-key annot:missing-spec=inactive
            -wp-warn-key pedantic-assigns=inactive
            -wp-no-warn-memory-model
            -cpp-extra-args=-I${CMAKE_CURRENT_SOURCE_DIR}/include
            -wp-fct ${NORMFS_UINTN_VARINT_WP_FCTS_ARG}
            src/varint.c
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "WP: uintn varint32/64"
        VERBATIM
    )
endif()
