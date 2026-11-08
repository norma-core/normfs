cmake_minimum_required(VERSION 3.10)

project(normfs_types_c VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -pedantic")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(normfs_types_c STATIC src/queue_name.c)
target_include_directories(normfs_types_c PUBLIC
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

    add_custom_target(verify-queue-name
        COMMAND ${FRAMAC}
            -machdep gcc_x86_64
            -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/verify-queue-name.json
            -wp -wp-rte -wp-timeout 240
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
            -cpp-extra-args=-I${CMAKE_CURRENT_SOURCE_DIR}/include
            -wp-fct normfs_queue_name_canonical,normfs_queue_name_word_at,normfs_queue_name_ok
            src/queue_name.c
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "WP: normfs queue names"
        VERBATIM
    )
endif()
