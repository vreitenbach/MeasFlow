cmake_minimum_required(VERSION 3.14)
project(measflow_c C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
include(GNUInstallDirs)

# ── Library ──────────────────────────────────────────────────────────────────

add_library(measflow STATIC measflow.c)
target_include_directories(measflow PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

if(MSVC)
    target_compile_options(measflow PRIVATE /W4 /WX)
else()
    target_compile_options(measflow PRIVATE -Wall -Wextra -Wpedantic -Werror)
endif()

# ── Optional compression support (via vcpkg) ────────────────────────────────

find_package(lz4 CONFIG QUIET)
if(lz4_FOUND AND TARGET lz4::lz4)
    target_compile_definitions(measflow PUBLIC MEAS_HAVE_LZ4)
    target_link_libraries(measflow PUBLIC lz4::lz4)
    message(STATUS "LZ4 compression: enabled")
else()
    message(STATUS "LZ4 compression: disabled (lz4 not found)")
endif()

find_package(zstd CONFIG QUIET)
if(zstd_FOUND AND TARGET zstd::libzstd)
    target_compile_definitions(measflow PUBLIC MEAS_HAVE_ZSTD)
    target_link_libraries(measflow PUBLIC zstd::libzstd)
    message(STATUS "Zstd compression: enabled")
else()
    message(STATUS "Zstd compression: disabled (zstd not found)")
endif()

# ── Install ─────────────────────────────────────────────────────────────────

install(TARGETS measflow
    EXPORT measflow-targets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(FILES measflow.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT measflow-targets
    FILE measflow-config.cmake
    NAMESPACE measflow::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/measflow
)

# ── Quickstart ───────────────────────────────────────────────────────────────

option(MEAS_BUILD_QUICKSTART "Build quickstart example" OFF)

if(MEAS_BUILD_QUICKSTART)
    add_executable(quickstart quickstart/quickstart.c)
    target_link_libraries(quickstart PRIVATE measflow m)
    if(MSVC)
        target_compile_options(quickstart PRIVATE /W4)
    else()
        target_compile_options(quickstart PRIVATE -Wall -Wextra)
    endif()
endif()

# ── Tests ────────────────────────────────────────────────────────────────────

option(MEAS_BUILD_TESTS "Build C unit tests" ON)

if(MEAS_BUILD_TESTS)
    enable_testing()
    add_executable(test_measflow tests/test_measflow.c)
    target_link_libraries(test_measflow PRIVATE measflow)
    if(MSVC)
        target_compile_options(test_measflow PRIVATE /W4)
    else()
        target_compile_options(test_measflow PRIVATE -Wall -Wextra)
    endif()
    add_test(NAME measflow_tests COMMAND test_measflow)
endif()
