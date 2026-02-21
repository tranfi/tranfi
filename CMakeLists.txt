cmake_minimum_required(VERSION 3.16)
project(tranfi C)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Core sources
set(CORE_SOURCES
    src/cJSON.c
    src/arena.c
    src/buffer.c
    src/batch.c
    src/pipeline.c
    src/plan.c
    src/expr.c
    src/codec_csv.c
    src/codec_jsonl.c
    src/codec_text.c
    src/op_filter.c
    src/op_select.c
    src/op_rename.c
    src/op_head.c
    src/op_skip.c
    src/op_derive.c
    src/op_stats.c
    src/op_unique.c
    src/op_sort.c
    src/op_validate.c
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
    src/op_pivot.c
    src/op_join.c
    src/op_stack.c
    src/op_lead.c
    src/op_date_trunc.c
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
)

# Static library (used by all targets)
add_library(tranfi_core STATIC ${CORE_SOURCES})
target_include_directories(tranfi_core PUBLIC src)
set_target_properties(tranfi_core PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Shared library
add_library(tranfi SHARED ${CORE_SOURCES})
target_include_directories(tranfi PUBLIC src)

# Compiler warnings and POSIX features (for strdup)
target_compile_definitions(tranfi_core PRIVATE _POSIX_C_SOURCE=200809L)
target_compile_definitions(tranfi PRIVATE _POSIX_C_SOURCE=200809L)
target_compile_options(tranfi_core PRIVATE -Wall -Wextra -Wpedantic)
target_compile_options(tranfi PRIVATE -Wall -Wextra -Wpedantic)

# WASM (Emscripten)
if(EMSCRIPTEN)
    add_executable(tranfi_wasm src/wasm_api.c)
    target_link_libraries(tranfi_wasm tranfi_core)
    target_include_directories(tranfi_wasm PRIVATE src)
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
target_compile_definitions(tranfi_cli PRIVATE _POSIX_C_SOURCE=200809L)
target_compile_options(tranfi_cli PRIVATE -Wall -Wextra -Wpedantic)
set_target_properties(tranfi_cli PROPERTIES OUTPUT_NAME "tranfi")

# Tests
option(BUILD_TESTING "Build tests" OFF)
if(BUILD_TESTING)
    enable_testing()
    add_executable(test_core test/test_core.c)
    target_link_libraries(test_core tranfi_core)
    target_include_directories(test_core PRIVATE src)
    add_test(NAME core_tests COMMAND test_core)
endif()

# Benchmark
add_executable(bench bench/bench.c)
target_link_libraries(bench tranfi_core)
target_include_directories(bench PRIVATE src)
target_compile_definitions(bench PRIVATE _POSIX_C_SOURCE=200809L)
target_compile_options(bench PRIVATE -Wall -Wextra -Wpedantic -O2)
