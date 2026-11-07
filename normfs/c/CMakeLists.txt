cmake_minimum_required(VERSION 3.10)

project(normfs_c VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -pedantic")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(NORMFS_UINTN_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../../uintn-rs/c/include"
    CACHE PATH "Path to the uintn C include directory")

add_library(normfs_c STATIC src/disk_monitor.c src/disk_monitor_sys.c src/frame.c
    src/conn.c src/net.c src/net_sys.c src/request.c src/response.c src/walk.c)
target_include_directories(normfs_c PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${NORMFS_UINTN_INCLUDE}>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/gremlin>
    $<INSTALL_INTERFACE:include>
)

# The tests cover the syscall shims, which no prover can discharge, the
# layout bytes the ACSL only sizes, and gremlin's compiler-builtin varint size.
enable_testing()
add_executable(test_disk_monitor tests/test_disk_monitor.c)
target_link_libraries(test_disk_monitor PRIVATE normfs_c)
add_test(NAME disk_monitor COMMAND test_disk_monitor)
add_executable(test_net tests/test_net.c)
target_link_libraries(test_net PRIVATE normfs_c)
add_test(NAME net COMMAND test_net)
add_executable(test_gremlin_varint tests/test_gremlin_varint.c)
target_link_libraries(test_gremlin_varint PRIVATE normfs_c)
add_test(NAME gremlin_varint COMMAND test_gremlin_varint)

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
        normfs_disk_event_append
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

    set(NORMFS_FRAME_WP_FCTS
        normfs_uintn_le64_read
        normfs_uintn_le64_write
        normfs_frame_header_encode
        normfs_frame_header_decode
        normfs_frame_header_roundtrip_holds)
    string(REPLACE ";" "," NORMFS_FRAME_WP_FCTS_ARG "${NORMFS_FRAME_WP_FCTS}")

    add_custom_target(verify-frame
        COMMAND ${FRAMAC}
            -machdep gcc_x86_64
            -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/verify-frame.json
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
            -cpp-extra-args=-I${CMAKE_CURRENT_SOURCE_DIR}/include\ -I${NORMFS_UINTN_INCLUDE}
            -wp-fct ${NORMFS_FRAME_WP_FCTS_ARG}
            src/frame.c
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "WP: normfs TCP frame header"
        VERBATIM
    )

    # Every function with a body in src/net.c and what it calls in frame.c;
    # frame.c's copy of the inline le64 read is the one Frama-C renames _0.
    set(NORMFS_NET_WP_FCTS
        normfs_uintn_le64_read_0
        normfs_frame_header_decode
        normfs_net_reader_init
        normfs_net_result
        normfs_net_read_header
        normfs_net_read_payload
        normfs_net_write
        normfs_net_accept)
    string(REPLACE ";" "," NORMFS_NET_WP_FCTS_ARG "${NORMFS_NET_WP_FCTS}")

    add_custom_target(verify-net
        COMMAND ${FRAMAC}
            -machdep gcc_x86_64
            -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/verify-net.json
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
            -cpp-extra-args=-I${CMAKE_CURRENT_SOURCE_DIR}/include\ -I${NORMFS_UINTN_INCLUDE}
            -wp-fct ${NORMFS_NET_WP_FCTS_ARG}
            src/net.c src/frame.c
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "WP: normfs TCP socket reads and writes"
        VERBATIM
    )

    # Every function with a body in src/conn.c; net.c and frame.c are there for
    # their contracts, verify-net proves them. -pp-annot expands the invariant
    # macros in the contracts.
    set(NORMFS_CONN_WP_FCTS
        normfs_conn_emit
        normfs_conn_rx_init
        normfs_conn_close_read
        normfs_conn_read
        normfs_conn_give_buffer
        normfs_conn_ack
        normfs_conn_tick
        normfs_conn_stop
        normfs_conn_rx_interest
        normfs_conn_deadline
        normfs_conn_tx_init
        normfs_conn_submit
        normfs_conn_close_write
        normfs_conn_pop
        normfs_conn_pick
        normfs_conn_sent_head
        normfs_conn_write
        normfs_conn_tx_interest
        normfs_conn_tx_drained
        normfs_conn_rx_size
        normfs_conn_tx_size
        normfs_conn_events_room
        normfs_conn_frame_max
        normfs_conn_pending_max)
    string(REPLACE ";" "," NORMFS_CONN_WP_FCTS_ARG "${NORMFS_CONN_WP_FCTS}")

    add_custom_target(verify-conn
        COMMAND ${FRAMAC}
            -machdep gcc_x86_64
            -pp-annot
            -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/verify-conn.json
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
            -cpp-extra-args=-I${CMAKE_CURRENT_SOURCE_DIR}/include\ -I${NORMFS_UINTN_INCLUDE}
            -wp-fct ${NORMFS_CONN_WP_FCTS_ARG}
            src/conn.c src/net.c src/frame.c
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "WP: normfs TCP connection state"
        VERBATIM
    )

    # Every function with a body in src/request.c and the generated readers it
    # calls; gremlin.h itself is proved by gremlin.c's verify-runtime.
    set(NORMFS_REQUEST_WP_FCTS
        normfs_ClientRequest_reader_init
        normfs_SetupRequest_reader_init
        normfs_SetupRequest_reader_get_version
        normfs_PingRequest_reader_init
        normfs_PingRequest_reader_get_sequence
        normfs_PingRequest_reader_get_client_timestamp_ns
        normfs_WriteRequest_reader_init
        normfs_WriteRequest_reader_get_write_id
        normfs_WriteRequest_reader_packets_begin
        normfs_WriteRequest_reader_packets_next
        normfs_ReadRequest_reader_init
        normfs_ReadRequest_reader_get_read_id
        normfs_ReadRequest_reader_get_step
        normfs_ReadRequest_reader_get_limit
        normfs_ReadRequest_reader_accept_encodings_begin
        normfs_ReadRequest_reader_accept_encodings_next
        normfs_Offset_reader_init
        normfs_Offset_reader_get_type
        normfs_Id_reader_init
        normfs_request_span
        normfs_request_repeats
        normfs_request_decode_setup
        normfs_request_decode_ping
        normfs_request_decode_write
        normfs_request_decode_offset
        normfs_request_decode_read
        normfs_request_fill
        normfs_request_decode)
    string(REPLACE ";" "," NORMFS_REQUEST_WP_FCTS_ARG "${NORMFS_REQUEST_WP_FCTS}")

    add_custom_target(verify-request
        COMMAND ${FRAMAC}
            -machdep gcc_x86_64
            -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/verify-request.json
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
            -cpp-extra-args=-I${CMAKE_CURRENT_SOURCE_DIR}/include\ -I${CMAKE_CURRENT_SOURCE_DIR}/gremlin
            -wp-fct ${NORMFS_REQUEST_WP_FCTS_ARG}
            src/request.c
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "WP: normfs TCP client request decoder"
        VERBATIM
    )

    # src/response.c and the generated code it reaches: _size, _encode_at,
    # _cached_size and the ghost functions _size calls; gremlin.h is proved by
    # gremlin.c's verify-runtime.
    set(NORMFS_RESPONSE_WP_FCTS
        normfs_Id_tsize_nonneg
        normfs_Id_tsize_pos
        normfs_SetupResponse_tsize_nonneg
        normfs_PingRequest_tsize_nonneg
        normfs_PingResponse_tsize_nonneg
        normfs_WriteResponse_ids_mono
        normfs_WriteResponse_tsize_nonneg
        normfs_ReadResponse_tsize_nonneg
        normfs_ServerResponse_tsize_nonneg
        normfs_Id_cached_size
        normfs_Id_size
        normfs_Id_encode_at
        normfs_Id_encode_raw_at
        normfs_SetupResponse_cached_size
        normfs_SetupResponse_size
        normfs_SetupResponse_encode_at
        normfs_SetupResponse_encode_instance_id_at
        normfs_SetupResponse_encode_instance_id_bytes_at
        normfs_SetupResponse_encode_version_at
        normfs_PingRequest_cached_size
        normfs_PingRequest_size
        normfs_PingRequest_encode_at
        normfs_PingRequest_encode_client_timestamp_ns_at
        normfs_PingRequest_encode_sequence_at
        normfs_PingResponse_cached_size
        normfs_PingResponse_size
        normfs_PingResponse_encode_at
        normfs_PingResponse_encode_local_stamp_ns_at
        normfs_PingResponse_encode_monotonic_stamp_ns_at
        normfs_PingResponse_encode_request_at
        normfs_WriteResponse_cached_size
        normfs_WriteResponse_size
        normfs_WriteResponse_encode_at
        normfs_WriteResponse_encode_ids_at
        normfs_WriteResponse_encode_result_at
        normfs_WriteResponse_encode_write_id_at
        normfs_ReadResponse_cached_size
        normfs_ReadResponse_size
        normfs_ReadResponse_encode_at
        normfs_ReadResponse_encode_data_at
        normfs_ReadResponse_encode_data_source_at
        normfs_ReadResponse_encode_encoding_at
        normfs_ReadResponse_encode_id_at
        normfs_ReadResponse_encode_read_id_at
        normfs_ReadResponse_encode_result_at
        normfs_ServerResponse_size
        normfs_ServerResponse_encode_at
        normfs_ServerResponse_encode_ping_at
        normfs_ServerResponse_encode_read_at
        normfs_ServerResponse_encode_setup_at
        normfs_ServerResponse_encode_write_at
        normfs_response_size
        normfs_response_encode)
    string(REPLACE ";" "," NORMFS_RESPONSE_WP_FCTS_ARG "${NORMFS_RESPONSE_WP_FCTS}")

    add_custom_target(verify-response
        COMMAND ${FRAMAC}
            -machdep gcc_x86_64
            -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/verify-response.json
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
            -cpp-extra-args=-I${CMAKE_CURRENT_SOURCE_DIR}/include\ -I${CMAKE_CURRENT_SOURCE_DIR}/gremlin
            -wp-fct ${NORMFS_RESPONSE_WP_FCTS_ARG}
            src/response.c
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "WP: normfs TCP server response encoder"
        VERBATIM
    )

    # The generated frame lemmas the _size proofs use; a -wp-fct run leaves
    # them out. tip runs the induction strategies normfs.pb.h declares.
    add_custom_target(verify-gremlin-lemmas
        COMMAND ${FRAMAC}
            -machdep gcc_x86_64
            -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/verify-gremlin-lemmas.json
            -wp -wp-timeout 240
            -wp-prover alt-ergo,z3,tip
            -wp-par ${NPROC}
            -wp-cache none
            -wp-cache-env
            -wp-msg-key shell
            -wp-no-warn-memory-model
            -cpp-extra-args=-I${CMAKE_CURRENT_SOURCE_DIR}/include\ -I${CMAKE_CURRENT_SOURCE_DIR}/gremlin
            -wp-prop=@lemma
            src/response.c
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "WP: lemmas of the generated normfs.pb.h"
        VERBATIM
    )

    add_custom_target(verify-walk
        COMMAND ${FRAMAC}
            -machdep gcc_x86_64
            -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/verify-walk.json
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
            -wp-fct normfs_walk_step
            src/walk.c
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "WP: normfs read walk step"
        VERBATIM
    )
endif()
