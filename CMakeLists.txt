cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME raster)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Enable network functionality (OpenSSL and GDAL's CURL based fs/drivers)
option(SPATIAL_USE_NETWORK "Enable network functionality" ON)
# Enable GEOS support
option(SPATIAL_USE_GEOS "Enable GEOS support" ON)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it.

# Find required dependencies for GDAL static linking
set(ZLIB_USE_STATIC_LIBS ON)
set(OPENSSL_USE_STATIC_LIBS ON)
find_package(ZLIB REQUIRED)
find_package(PROJ CONFIG REQUIRED)
find_package(GDAL CONFIG REQUIRED)

# Important: The link order matters, its the reverse order of dependency
set(EXTENSION_DEPENDENCIES GDAL::GDAL PROJ::proj ZLIB::ZLIB)

if(SPATIAL_USE_NETWORK)
  message(STATUS "Building with network functionality")
  message(STATUS "OpenSSL root dir hint: '$ENV{OPENSSL_ROOT_DIR}'")
  find_package(OpenSSL REQUIRED)
  find_package(CURL REQUIRED)
  list(APPEND EXTENSION_DEPENDENCIES CURL::libcurl OpenSSL::SSL OpenSSL::Crypto)
endif()

if(SPATIAL_USE_GEOS)
  message(STATUS "Building with GEOS functionality")
  find_package(GEOS REQUIRED)
  set(EXTENSION_DEPENDENCIES ${EXTENSION_DEPENDENCIES} GEOS::geos_c)
  add_definitions(-DSPATIAL_USE_GEOS=1)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(src/raster)

set(EXTENSION_SOURCES src/raster_extension.cpp)
add_subdirectory(src/raster)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} ${EXTENSION_DEPENDENCIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${EXTENSION_DEPENDENCIES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
