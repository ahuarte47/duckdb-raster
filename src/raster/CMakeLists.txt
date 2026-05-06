add_subdirectory(modules)
add_subdirectory(scalar)
add_subdirectory(table)

set(EXTENSION_SOURCES
    ${EXTENSION_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/data_cube.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/data_format_utils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/data_types.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/filter_eval.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raster_casts.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raster_types.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raster_utils.cpp
    PARENT_SCOPE)
