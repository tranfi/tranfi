cmake_minimum_required(VERSION 3.16)
project(tranfi C)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

function(tranfi_apply_c_common target)
    target_compile_definitions(${target} PRIVATE _POSIX_C_SOURCE=200809L _XOPEN_SOURCE=700)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(${target} PRIVATE
            -Wall
            -Wextra
            -Wpedantic
            -Werror=implicit-function-declaration
            -Werror=incompatible-pointer-types
            -Wformat
            -Werror=format-security
            -Werror=unused-result
            -fno-common
            $<$<CONFIG:Release>:-fstack-protector-strong>
        )
        if(NOT EMSCRIPTEN)
            target_compile_definitions(${target} PRIVATE $<$<CONFIG:Release>:_FORTIFY_SOURCE=3>)
        endif()
    endif()
endfunction()

function(tranfi_apply_executable_hardening target)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang" AND NOT EMSCRIPTEN)
        target_compile_options(${target} PRIVATE $<$<CONFIG:Release>:-fPIE>)
        target_link_options(${target} PRIVATE $<$<CONFIG:Release>:-pie>)
    endif()
endfunction()

set(TRANFI_SANITIZER "address,undefined" CACHE STRING "Debug sanitizer set: address,undefined, thread, or none")

function(tranfi_apply_debug_sanitizer target)
    if(CMAKE_BUILD_TYPE STREQUAL "Debug" AND NOT EMSCRIPTEN AND NOT TRANFI_SANITIZER STREQUAL "" AND NOT TRANFI_SANITIZER STREQUAL "none")
        target_compile_options(${target} PRIVATE -fsanitize=${TRANFI_SANITIZER} -fno-omit-frame-pointer -g -O0)
        target_link_options(${target} PRIVATE -fsanitize=${TRANFI_SANITIZER})
        if(TRANFI_SANITIZER STREQUAL "thread")
            target_compile_options(${target} PRIVATE -fPIE)
            target_link_options(${target} PRIVATE -pie)
        endif()
    endif()
endfunction()

# Core sources
set(CORE_SOURCES
    src/cJSON.c
    src/arena.c
    src/buffer.c
    src/size_utils.c
    src/path_policy.c
    src/batch.c
    src/pipeline.c
    src/expr.c
    src/selector.c
    src/memory_estimate.c
    src/spill.c
    src/json_path.c
    src/codec_csv.c
    src/codec_jsonl.c
    src/codec_text.c
    src/op_filter.c
    src/op_select.c
    src/op_rename.c
    src/op_head.c
    src/op_skip.c
    src/op_derive.c
    src/op_source_name.c
    src/op_across.c
    src/op_stats.c
    src/op_unique.c
    src/op_sort.c
    src/op_validate.c
    src/op_assert.c
    src/op_quarantine.c
    src/op_schema.c
    src/op_schema_infer.c
    src/op_tee.c
    src/op_trim.c
    src/op_fill_null.c
    src/op_cast.c
    src/op_clip.c
    src/op_replace.c
    src/op_hash.c
    src/op_bin.c
    src/op_fill_down.c
    src/op_step.c
    src/op_window.c
    src/op_explode.c
    src/op_split.c
    src/op_unpivot.c
    src/op_tail.c
    src/op_top.c
    src/op_sample.c
    src/op_group_agg.c
    src/op_frequency.c
    src/op_datetime.c
    src/op_grep.c
    src/op_json_extract.c
    src/op_json_filter.c
    src/op_json_schema.c
    src/op_json_flatten.c
    src/op_pivot.c
    src/op_join.c
    src/op_set.c
    src/op_stack.c
    src/op_lead.c
    src/op_lag.c
    src/op_rowid.c
    src/op_rleid.c
    src/op_date_trunc.c
    src/op_onehot.c
    src/op_label_encode.c
    src/op_ewma.c
    src/op_diff.c
    src/op_anomaly.c
    src/op_split_data.c
    src/op_interpolate.c
    src/op_normalize.c
    src/op_acf.c
    src/codec_table.c
    src/ir.c
    src/ir_serialize.c
    src/ir_sql.c
    src/ir_validate.c
    src/ir_schema.c
    src/op_registry.c
    src/compiler.c
    src/dsl.c
    src/recipes.c
    src/report.c
)

# Static library (used by all targets)
add_library(tranfi_core STATIC ${CORE_SOURCES})
target_include_directories(tranfi_core PUBLIC src)
set_target_properties(tranfi_core PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Shared library
add_library(tranfi SHARED ${CORE_SOURCES})
target_include_directories(tranfi PUBLIC src)

# Compiler warnings, POSIX features, and native hardening.
tranfi_apply_c_common(tranfi_core)
tranfi_apply_c_common(tranfi)

# Debug build: configurable sanitizer. Default remains ASan + UBSan.
tranfi_apply_debug_sanitizer(tranfi_core)
tranfi_apply_debug_sanitizer(tranfi)

# WASM (Emscripten)
if(EMSCRIPTEN)
    add_executable(tranfi_wasm src/wasm_api.c)
    target_link_libraries(tranfi_wasm tranfi_core)
    target_include_directories(tranfi_wasm PRIVATE src)
    tranfi_apply_c_common(tranfi_wasm)
    set_target_properties(tranfi_wasm PROPERTIES
        OUTPUT_NAME "tranfi_core"
        SUFFIX ".js"
    )
    target_link_options(tranfi_wasm PRIVATE
        -sMODULARIZE=1
        -sEXPORT_NAME=createTranfi
        "-sEXPORTED_RUNTIME_METHODS=['ccall','cwrap','UTF8ToString','lengthBytesUTF8','stringToUTF8','getValue','setValue','HEAPU8']"
        -sALLOW_MEMORY_GROWTH=1
        -sEXPORTED_FUNCTIONS=['_malloc','_free']
        -sSINGLE_FILE=1
    )
endif()

# Link math library
target_link_libraries(tranfi_core m)
target_link_libraries(tranfi m)

# CLI executable
add_executable(tranfi_cli src/main.c)
target_link_libraries(tranfi_cli tranfi_core)
target_include_directories(tranfi_cli PRIVATE src)
tranfi_apply_c_common(tranfi_cli)
tranfi_apply_executable_hardening(tranfi_cli)
set_target_properties(tranfi_cli PROPERTIES OUTPUT_NAME "tranfi")
tranfi_apply_debug_sanitizer(tranfi_cli)

# Tests
option(BUILD_TESTING "Build tests" OFF)
if(BUILD_TESTING)
    enable_testing()
    find_package(Threads REQUIRED)
    add_executable(test_core test/test_core.c)
    target_link_libraries(test_core tranfi_core Threads::Threads)
    target_include_directories(test_core PRIVATE src)
    # Never disable asserts in test binary
    tranfi_apply_c_common(test_core)
    target_compile_options(test_core PRIVATE $<$<CONFIG:Release>:-UNDEBUG>)
    tranfi_apply_executable_hardening(test_core)
    tranfi_apply_debug_sanitizer(test_core)
    add_test(NAME core_tests COMMAND test_core)

    add_executable(test_memory test/test_memory.c)
    target_link_libraries(test_memory tranfi_core)
    target_include_directories(test_memory PRIVATE src)
    tranfi_apply_c_common(test_memory)
    target_compile_options(test_memory PRIVATE $<$<CONFIG:Release>:-UNDEBUG>)
    tranfi_apply_executable_hardening(test_memory)
    tranfi_apply_debug_sanitizer(test_memory)
    add_test(NAME memory_tests COMMAND test_memory)

    add_executable(test_wasm_api test/test_wasm_api.c src/wasm_api.c)
    target_link_libraries(test_wasm_api tranfi_core)
    target_include_directories(test_wasm_api PRIVATE src)
    tranfi_apply_c_common(test_wasm_api)
    target_compile_options(test_wasm_api PRIVATE $<$<CONFIG:Release>:-UNDEBUG>)
    tranfi_apply_executable_hardening(test_wasm_api)
    tranfi_apply_debug_sanitizer(test_wasm_api)
    add_test(NAME wasm_api_tests COMMAND test_wasm_api)

    add_test(NAME cli_memory_policy COMMAND bash ${CMAKE_SOURCE_DIR}/test/test_cli_memory_policy.sh $<TARGET_FILE:tranfi_cli>)

    add_executable(test_oom test/test_oom.c)
    target_link_libraries(test_oom tranfi_core)
    target_include_directories(test_oom PRIVATE src)
    tranfi_apply_c_common(test_oom)
    target_compile_options(test_oom PRIVATE $<$<CONFIG:Release>:-UNDEBUG>)
    tranfi_apply_executable_hardening(test_oom)
    tranfi_apply_debug_sanitizer(test_oom)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang" AND NOT EMSCRIPTEN)
        target_link_options(test_oom PRIVATE
            -Wl,--wrap=malloc
            -Wl,--wrap=calloc
            -Wl,--wrap=realloc
            -Wl,--wrap=strdup
            -Wl,--wrap=strndup
        )
    endif()
    add_test(NAME oom_tests COMMAND test_oom)
endif()

# Benchmark
add_executable(bench bench/bench.c)
target_link_libraries(bench tranfi_core)
target_include_directories(bench PRIVATE src)
tranfi_apply_c_common(bench)
target_compile_options(bench PRIVATE -O2)
tranfi_apply_executable_hardening(bench)
tranfi_apply_debug_sanitizer(bench)
