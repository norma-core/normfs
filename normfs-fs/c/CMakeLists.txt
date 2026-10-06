cmake_minimum_required(VERSION 3.10)

project(normfs_fs_c VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -pedantic")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(normfs_fs_c STATIC src/fs_plan.c src/fs_dir.c src/fs_sys.c)
target_include_directories(normfs_fs_c PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# src/fs_plan.c is established by Frama-C WP. The shims in normfs/fs_sys.h
# carry errno contracts no prover can discharge, so they are checked against
# a real filesystem instead.
enable_testing()
add_executable(test_fs tests/test_fs.c)
target_link_libraries(test_fs PRIVATE normfs_fs_c)
add_test(NAME fs COMMAND test_fs)

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

    # Every function with a body in the proved sources, statics included. A
    # function left off this list is silently never scheduled, and
    # check-proved.sh cannot tell that apart from one that was proved.
    set(NORMFS_FS_WP_FCTS
        normfs_fs_sync_created_dir
        normfs_fs_append_crash_prefix
        normfs_fs_publish_crash_safe
        normfs_fs_rename_equal_paths
        normfs_fs_publish_init
        normfs_fs_append_init
        normfs_fs_create_init
        normfs_fs_remove_init
        normfs_fs_restore_init
        normfs_fs_plan_next
        normfs_fs_publish_ok
        normfs_fs_publish_absent
        normfs_fs_publish_err
        normfs_fs_append_ok
        normfs_fs_append_err
        normfs_fs_create_ok
        normfs_fs_create_err
        normfs_fs_remove_ok
        normfs_fs_remove_err
        normfs_fs_restore_report
        normfs_fs_publish_done_durable
        normfs_fs_append_boundary_holds)
    string(REPLACE ";" "," NORMFS_FS_WP_FCTS_ARG "${NORMFS_FS_WP_FCTS}")

    # src/fs_sys.c is absent on purpose. With it WP would see the empty
    # completion bodies instead of the kernel model in their contracts, and
    # Frama-C's own libc, whose syscall contracts assign no errno. The
    # machdep is pinned: both deployment targets are LP64.
    add_custom_target(verify-fs-plan
        COMMAND ${FRAMAC}
            -machdep gcc_x86_64
            -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/verify-fs-plan.json
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
            -wp-fct ${NORMFS_FS_WP_FCTS_ARG}
            src/fs_plan.c src/fs_dir.c src/fs_crash.c
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "WP: normfs fs protocol planner"
        VERBATIM
    )
endif()
