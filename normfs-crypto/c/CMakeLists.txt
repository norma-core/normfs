cmake_minimum_required(VERSION 3.10)

project(normfs_crypto_c VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -pedantic")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(normfs_crypto_c STATIC
    src/seed.c
    src/seed_sys.c
    src/sha256.c
    src/hmac_sha256.c
    src/chacha20.c
    src/kdf.c
)
target_include_directories(normfs_crypto_c PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

enable_testing()

# src/seed.c is established by Frama-C WP. These tests cover what WP cannot
# reach: the syscall shims in normfs/seed_sys.h carry contracts no prover can
# discharge, so they are checked against a real filesystem instead.
add_executable(test_seed tests/test_seed.c)
target_link_libraries(test_seed PRIVATE normfs_crypto_c)
add_test(NAME seed COMMAND test_seed)

# The round transform and the sigma functions are echoed rather than proved:
# the ACSL is the same expression tree as the C, so the goals close by
# congruence and say nothing about the expression being SHA-256. These vectors
# are what says it, the way test_crc32c.c pins the CRC32C table.
add_executable(test_sha256 tests/test_sha256.c)
target_link_libraries(test_sha256 PRIVATE normfs_crypto_c)
# libm: the constants are re-derived with sqrt and cbrt.
find_library(NORMFS_LIBM m)
if(NORMFS_LIBM)
    target_link_libraries(test_sha256 PRIVATE ${NORMFS_LIBM})
endif()
add_test(NAME sha256 COMMAND test_sha256)

add_executable(test_hmac_sha256 tests/test_hmac_sha256.c)
target_link_libraries(test_hmac_sha256 PRIVATE normfs_crypto_c)
add_test(NAME hmac_sha256 COMMAND test_hmac_sha256)

add_executable(test_chacha20 tests/test_chacha20.c)
target_link_libraries(test_chacha20 PRIVATE normfs_crypto_c)
add_test(NAME chacha20 COMMAND test_chacha20)

add_executable(test_kdf tests/test_kdf.c)
target_link_libraries(test_kdf PRIVATE normfs_crypto_c)
add_test(NAME kdf COMMAND test_kdf)

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

    # Pinned, not configurable: the deployment targets are amd64 and arm64,
    # both LP64, so one model covers them. Naming it still beats the default,
    # which is not the same across Frama-C versions.
    function(normfs_add_wp_target target source fcts comment)
        string(REPLACE ";" "," fcts_arg "${fcts}")
        add_custom_target(${target}
            COMMAND ${FRAMAC}
                -machdep gcc_x86_64
                -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/${target}.json
                -wp -wp-rte -wp-timeout 240
                -wp-prover alt-ergo,z3
                -wp-par ${NPROC}
                -wp-cache none
                -wp-smoke-tests
                -wp-msg-key shell
                -kernel-warn-key annot:missing-spec=inactive
                -wp-warn-key pedantic-assigns=inactive
                -wp-no-warn-memory-model
                -cpp-extra-args=-I${CMAKE_CURRENT_SOURCE_DIR}/include
                -wp-fct ${fcts_arg}
                ${source}
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            COMMENT "${comment}"
            VERBATIM
        )
    endfunction()

    # Every function with a body in the unit, statics included. A function left
    # off a list is silently never scheduled, and check-proved.sh cannot tell
    # that apart from one that was proved.
    #
    # src/seed_sys.c is absent from every command on purpose. Feeding it here
    # would replace the assumed contracts in normfs/seed_sys.h with bodies WP
    # cannot interpret, and would drag in Frama-C's own libc, whose syscall
    # contracts assign no errno at all -- every os_error postcondition would go
    # from provable to impossible.
    set(NORMFS_SEED_WP_FCTS
        normfs_seed_path
        normfs_seed_zero
        normfs_seed_read_exact
        normfs_seed_write_all
        normfs_seed_generate
        normfs_seed_load
        normfs_seed_save
        normfs_seed_exists)

    set(NORMFS_SHA256_WP_FCTS
        normfs_sha256_rotr_fn
        normfs_sha256_ch_fn
        normfs_sha256_maj_fn
        normfs_sha256_bsig0_fn
        normfs_sha256_bsig1_fn
        normfs_sha256_ssig0_fn
        normfs_sha256_ssig1_fn
        normfs_sha256_t1_fn
        normfs_sha256_t2_fn
        normfs_sha256_be32_read
        normfs_sha256_be32_write
        normfs_sha256_be64_write_bits
        normfs_sha256_schedule
        normfs_sha256_compress
        normfs_sha256_absorb
        normfs_sha256_finish
        normfs_sha256)

    set(NORMFS_HMAC_SHA256_WP_FCTS
        normfs_hmac_sha256_k0
        normfs_hmac_sha256_xor_block
        normfs_hmac_sha256_2
        normfs_hmac_sha256)

    set(NORMFS_CHACHA20_WP_FCTS
        normfs_chacha20_rotl_fn
        normfs_chacha20_le32_read
        normfs_chacha20_le32_write
        normfs_chacha20_quarter
        normfs_chacha20_qr_at
        normfs_chacha20_block)

    set(NORMFS_KDF_WP_FCTS
        normfs_hkdf_sha256_extract
        normfs_hkdf_sha256_expand
        normfs_kdf_derive_file_key)

    normfs_add_wp_target(verify-seed src/seed.c
        "${NORMFS_SEED_WP_FCTS}" "WP: normfs crypto seed")
    normfs_add_wp_target(verify-sha256 src/sha256.c
        "${NORMFS_SHA256_WP_FCTS}" "WP: normfs SHA-256")
    normfs_add_wp_target(verify-hmac-sha256 src/hmac_sha256.c
        "${NORMFS_HMAC_SHA256_WP_FCTS}" "WP: normfs HMAC-SHA256")
    normfs_add_wp_target(verify-chacha20 src/chacha20.c
        "${NORMFS_CHACHA20_WP_FCTS}" "WP: normfs ChaCha20")
    normfs_add_wp_target(verify-kdf src/kdf.c
        "${NORMFS_KDF_WP_FCTS}" "WP: normfs HKDF and file key derivation")
endif()
