cmake_minimum_required(VERSION 3.20)
project(pipestream_quic_msquic VERSION 0.1.0 LANGUAGES C CXX)

include(FetchContent)
find_package(OpenSSL 3.5 REQUIRED COMPONENTS Crypto)

set(BUILD_SHARED_LIBS ON CACHE BOOL "" FORCE)
set(QUIC_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(QUIC_BUILD_TEST OFF CACHE BOOL "" FORCE)
set(QUIC_BUILD_PERF OFF CACHE BOOL "" FORCE)
set(QUIC_ENABLE_LOGGING OFF CACHE BOOL "" FORCE)
set(QUIC_TLS_LIB openssl CACHE STRING "" FORCE)
set(QUIC_USE_EXTERNAL_OPENSSL ON CACHE BOOL "" FORCE)
set(QUIC_EMBED_GIT_HASH OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  msquic
  GIT_REPOSITORY https://github.com/microsoft/msquic.git
  GIT_TAG v2.6.1
  GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(msquic)

add_library(pipestream_wire src/wire.cpp)
target_include_directories(pipestream_wire PUBLIC include)
target_compile_features(pipestream_wire PUBLIC cxx_std_20)
target_compile_options(pipestream_wire PRIVATE -Wall -Wextra -Wpedantic -Werror)
target_link_libraries(pipestream_wire PUBLIC OpenSSL::Crypto)

add_library(pipestream_transport src/transport.cpp)
target_include_directories(pipestream_transport PUBLIC include)
target_include_directories(pipestream_transport SYSTEM PRIVATE ${msquic_SOURCE_DIR}/src/inc)
target_compile_features(pipestream_transport PUBLIC cxx_std_20)
target_compile_options(pipestream_transport PRIVATE -Wall -Wextra -Wpedantic -Werror)
target_link_libraries(pipestream_transport PUBLIC pipestream_wire PRIVATE msquic::msquic)

add_executable(pipestream-msquic src/main.cpp)
target_compile_features(pipestream-msquic PRIVATE cxx_std_20)
target_compile_options(pipestream-msquic PRIVATE -Wall -Wextra -Wpedantic -Werror)
target_link_libraries(pipestream-msquic PRIVATE pipestream_transport msquic::msquic)

enable_testing()
add_executable(pipestream-wire-tests tests/wire_tests.cpp)
target_compile_features(pipestream-wire-tests PRIVATE cxx_std_20)
target_compile_options(pipestream-wire-tests PRIVATE -Wall -Wextra -Wpedantic -Werror)
target_link_libraries(pipestream-wire-tests PRIVATE pipestream_wire)
target_compile_definitions(
  pipestream-wire-tests
  PRIVATE PIPESTREAM_VECTOR_ROOT="${CMAKE_CURRENT_SOURCE_DIR}/../../test-vectors"
)
add_test(NAME wire-vectors COMMAND pipestream-wire-tests)
