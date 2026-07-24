cmake_minimum_required(VERSION 3.10)

project(normfs_store_c VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -pedantic")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(NORMFS_UINTN_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../../uintn-rs/c/include"
    CACHE PATH "Path to the uintn C include directory")

add_library(normfs_store_c STATIC src/store_header.c)
target_include_directories(normfs_store_c PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${NORMFS_UINTN_INCLUDE}>
    $<INSTALL_INTERFACE:include>
)

enable_testing()
add_executable(test_store_header tests/test_store_header.c)
target_link_libraries(test_store_header PRIVATE normfs_store_c)
add_test(NAME store_header COMMAND test_store_header)

find_program(FRAMAC frama-c)
if(FRAMAC)
    include(ProcessorCount)
    ProcessorCount(NPROC)
    if(NPROC EQUAL 0)
        set(NPROC 4)
    endif()

    set(NORMFS_STORE_HEADER_WP_FCTS
        normfs_store_header_v1_size
        normfs_store_header_v1_encode
        normfs_store_header_v1_decode
        normfs_store_header_peek_version)
    string(REPLACE ";" "," NORMFS_STORE_HEADER_WP_FCTS_ARG
        "${NORMFS_STORE_HEADER_WP_FCTS}")

    add_custom_target(verify-store-header
        COMMAND ${FRAMAC}
            -wp -wp-rte -wp-timeout 60
            -wp-prover alt-ergo,z3
            -wp-par ${NPROC}
            -wp-cache none
            -wp-smoke-tests
            -wp-msg-key shell
            -kernel-warn-key annot:missing-spec=inactive
            -wp-warn-key pedantic-assigns=inactive
            -wp-no-warn-memory-model
            -cpp-extra-args=-I${CMAKE_CURRENT_SOURCE_DIR}/include\ -I${NORMFS_UINTN_INCLUDE}
            -wp-fct ${NORMFS_STORE_HEADER_WP_FCTS_ARG}
            src/store_header.c
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "WP: normfs V1 store header"
        VERBATIM
    )
endif()
