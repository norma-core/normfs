cmake_minimum_required(VERSION 3.10)

project(normfs_c VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -pedantic")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(normfs_c STATIC src/disk_monitor.c src/disk_monitor_sys.c)
target_include_directories(normfs_c PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# The tests cover the syscall shims, which no prover can discharge, and pin
# the layout bytes the ACSL only sizes.
enable_testing()
add_executable(test_disk_monitor tests/test_disk_monitor.c)
target_link_libraries(test_disk_monitor PRIVATE normfs_c)
add_test(NAME disk_monitor COMMAND test_disk_monitor)

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

    # Every function with a body in src/disk_monitor.c, statics included.
    set(NORMFS_DISK_WP_FCTS
        normfs_disk_is_hex
        normfs_disk_hex_val
        normfs_disk_hex_digit
        normfs_disk_id_ok
        normfs_disk_id_cmp
        normfs_disk_id_increment
        normfs_disk_id_copy
        normfs_disk_id_from_chunks
        normfs_disk_parse_name
        normfs_disk_set_bit
        normfs_disk_next_bit
        normfs_disk_write_chunks
        normfs_disk_write_ext
        normfs_disk_path
        normfs_disk_probe
        normfs_disk_file_size
        normfs_disk_list_dir
        normfs_disk_scan
        normfs_disk_evict_one
        normfs_disk_evict)
    string(REPLACE ";" "," NORMFS_DISK_WP_FCTS_ARG "${NORMFS_DISK_WP_FCTS}")

    # src/disk_monitor_sys.c is absent on purpose: with it WP would see the
    # shim bodies and Frama-C's own libc, whose syscall contracts assign no
    # errno. The machdep is pinned: both deployment targets are LP64.
    add_custom_target(verify-disk-monitor
        COMMAND ${FRAMAC}
            -machdep gcc_x86_64
            -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/verify-disk-monitor.json
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
            -wp-fct ${NORMFS_DISK_WP_FCTS_ARG}
            src/disk_monitor.c
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "WP: normfs disk monitor"
        VERBATIM
    )
endif()
