cmake_minimum_required(VERSION 3.24)
project(pipestream_sqlite_guard C)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR NOT CMAKE_SIZEOF_VOID_P EQUAL 8)
  message(FATAL_ERROR "The Java SQLite guard currently supports 64-bit Linux only")
endif()
include(FetchContent)
FetchContent_Declare(sqlite_headers
  URL https://www.sqlite.org/2026/sqlite-amalgamation-3530400.zip
  URL_HASH SHA256=1e71ddf93849c6a6ecf58b827c0692073d2dd7ee40196158068f7b29f422e87d
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
FetchContent_MakeAvailable(sqlite_headers)
find_package(Threads REQUIRED)
add_library(pipestream_sqlite SHARED bounded_vfs.c)
target_include_directories(pipestream_sqlite PRIVATE ${sqlite_headers_SOURCE_DIR})
target_link_libraries(pipestream_sqlite PRIVATE Threads::Threads)
target_compile_features(pipestream_sqlite PRIVATE c_std_11)
target_compile_options(pipestream_sqlite PRIVATE -Wall -Wextra -Werror -Wconversion -Wshadow -Wstrict-prototypes -fvisibility=hidden)
target_compile_definitions(pipestream_sqlite PRIVATE _POSIX_C_SOURCE=200809L _FILE_OFFSET_BITS=64)
if(DEFINED PIPESTREAM_NATIVE_OUTPUT)
  set_target_properties(pipestream_sqlite PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${PIPESTREAM_NATIVE_OUTPUT}")
endif()

# Only SQLite's public headers are used. The extension calls the API supplied by
# Xerial's SQLite instance; it must not link a second copy of libsqlite3.

include(CTest)
if(BUILD_TESTING)
  add_library(sqlite_test_engine STATIC ${sqlite_headers_SOURCE_DIR}/sqlite3.c)
  target_include_directories(sqlite_test_engine PUBLIC ${sqlite_headers_SOURCE_DIR})
  target_link_libraries(sqlite_test_engine PUBLIC Threads::Threads ${CMAKE_DL_LIBS})
  add_executable(sqlite_guard_test bounds_test.c)
  target_link_libraries(sqlite_guard_test PRIVATE sqlite_test_engine)
  target_compile_features(sqlite_guard_test PRIVATE c_std_11)
  target_compile_definitions(sqlite_guard_test PRIVATE _POSIX_C_SOURCE=200809L)
  target_compile_options(sqlite_guard_test PRIVATE -Wall -Wextra -Werror -Wconversion -Wshadow)
  add_test(NAME sqlite-file-growth COMMAND sqlite_guard_test $<TARGET_FILE:pipestream_sqlite>)
endif()
