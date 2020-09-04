# Functions and Pre-build  -----------------------------------

# Stop messy in source builds
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_DISABLE_SOURCE_CHANGES  ON)

if ( ${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR} )
    message( FATAL_ERROR "In-source builds not allowed. Please make a new directory (called a build directory) and run CMake from there. You may need to remove CMakeCache.txt." )
endif()

# Start Build Config -----------------------------------
cmake_minimum_required(VERSION 3.6)
set(CMAKE_SKIP_RPATH true)
set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_COLOR_MAKEFILE ON)

# Library output name for the final binaries
set(LIBHDBPP_MYSQL_NAME "libhdb++mysql")

# Versioning
set(VERSION_MAJOR "6")
set(VERSION_MINOR "2")
set(VERSION_PATCH "3")
set(VERSION_METADATA "")
set(VERSION_STRING ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH})

# Add any include paths from the command line
list(APPEND INCLUDE_PATHS ${CMAKE_INCLUDE_PATH})
list(APPEND INCLUDE_PATHS ${CMAKE_SOURCE_DIR})
list(APPEND LIBRARY_PATHS ${CMAKE_LIBRARY_PATH})

# Start the project
project(libhdbpp_mysql VERSION ${VERSION_STRING} LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# To build both the static and shared version, we require two projects, we do
# this by just defining the name appended with the target
#project(shared_library)
#project(static_library)

# arch install definitions
include(GNUInstallDirs)

message(STATUS "Searching for libraries...")

include(FindPkgConfig)
pkg_check_modules(LIBMYSQLCLIENT REQUIRED mysqlclient)

foreach(FLAG ${LIBMYSQLCLIENT_CFLAGS_OTHER})
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${FLAG}")
endforeach()

# allow pkg-config to search the CMAKE_PREFIX_PATH
set(PKG_CONFIG_USE_CMAKE_PREFIX_PATH ON)
list(APPEND CMAKE_PREFIX_PATH "/usr")

# Find Dependencies  ---------------------
include(cmake/FindLibraries.cmake)

# First find tango if it has not already been found. Returns an interface library
# called TangoInterfaceLibrary
set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${CMAKE_CURRENT_SOURCE_DIR}/cmake")
find_package(Tango)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Add any include paths from the command line
set(INCLUDE_PATHS ${INCLUDE_PATHS} ${CMAKE_INCLUDE_PATH})

foreach(INC_PATH ${CMAKE_INCLUDE_PATH})
   message(STATUS "Extra include path: " ${INC_PATH})
endforeach()

message(STATUS "Building library paths...")

foreach(LIB_PATH ${CMAKE_LIBRARY_PATH})
   message(STATUS "Extra library search path: " ${LIB_PATH})
endforeach()

# Variable to contain a list of all the libs we depend on
set(HDBPP_MSQL_LIBRARIES
#   libhdb++.so
   )

# Now load the libraries and check each is found, if not
# this is an error condition, so halt the cmake config
foreach(LIB ${HDBPP_MSQL_LIBRARIES})
   # try the user provided paths first
   find_library(FOUND_LIB_${LIB} ${LIB} PATHS ${LIBRARY_PATHS} NO_DEFAULT_PATH)
   # if we could not find it, drop to the system paths
   if (NOT FOUND_LIB_${LIB})
      find_library(FOUND_LIB_${LIB} ${LIB})
   endif()

   if (FOUND_LIB_${LIB})
      message(STATUS "Found " ${LIB}" at: " ${FOUND_LIB_${LIB}})
   else()
      message(FATAL_ERROR "Could not find " ${LIB})
   endif()
   list(APPEND LIBS_DIR ${FOUND_LIB_${LIB}})
endforeach(LIB)

# Source -----------------------------------

add_subdirectory(src)


# Build Targets  -----------------------------------


# Shared library ------------------------------------------------------

#add_library(shared_library SHARED $<TARGET_OBJECTS:object_library>)
add_library(libhdbpp_mysql_shared_library SHARED ${SRC_FILES})

target_link_libraries(libhdbpp_mysql_shared_library PRIVATE ${LIBMYSQLCLIENT_LIBRARIES})
target_link_libraries(libhdbpp_mysql_shared_library PUBLIC ${LIBS_DIR} PRIVATE TangoInterfaceLibrary)
#target_link_libraries(libhdbpp_mysql_shared_library PRIVATE hdb++)

set_target_properties(libhdbpp_mysql_shared_library PROPERTIES 
        OUTPUT_NAME hdb++mysql
        LINK_FLAGS "-Wl,--no-undefined")

# Setting the target includes will allow us to link to this shared
# library via the cmake project and not require us to include all
# its dependencies
target_include_directories(libhdbpp_mysql_shared_library PRIVATE ${LIBMYSQLCLIENT_INCLUDE_DIRS})
target_include_directories(libhdbpp_mysql_shared_library PRIVATE ${INCLUDE_PATHS})
target_include_directories(libhdbpp_mysql_shared_library PUBLIC ${CMAKE_CURRENT_LIST_DIR}/src)
#target_include_directories(libhdbpp_mysql_shared_library PUBLIC "${PROJECT_BINARY_DIR}")
target_compile_options(libhdbpp_mysql_shared_library PUBLIC -DRELEASE="${VERSION_STRING}" -Wall)

# This will version the shared library
set_target_properties(libhdbpp_mysql_shared_library
    PROPERTIES VERSION ${VERSION_STRING}
    SOVERSION ${VERSION_MAJOR})


# Static library ------------------------------------------------------

#add_library(libhdbpp_mysql_static_library STATIC $<TARGET_OBJECTS:object_library>)
add_library(libhdbpp_mysql_static_library STATIC ${SRC_FILES})
target_link_libraries(libhdbpp_mysql_static_library PRIVATE ${LIBMYSQLCLIENT_LIBRARIES})
target_link_libraries(libhdbpp_mysql_static_library PUBLIC ${LIBS_DIR} PRIVATE TangoInterfaceLibrary)
#target_link_libraries(libhdbpp_mysql_static_library PRIVATE hdb++)
set_target_properties(libhdbpp_mysql_static_library PROPERTIES OUTPUT_NAME hdb++mysql)

# Like the shared project, define dependencies, the static library is
# used in the tests and requires no additional includes due to this
target_include_directories(libhdbpp_mysql_static_library PRIVATE ${LIBMYSQLCLIENT_INCLUDE_DIRS})
target_include_directories(libhdbpp_mysql_static_library PRIVATE ${INCLUDE_PATHS})
target_include_directories(libhdbpp_mysql_static_library PUBLIC ${CMAKE_CURRENT_LIST_DIR}/src)
#target_include_directories(libhdbpp_mysql_static_library PUBLIC "${PROJECT_BINARY_DIR}")
target_compile_options(libhdbpp_mysql_static_library PUBLIC -DRELEASE="${VERSION_STRING}" -Wall)


# Install -----------------------------------------------------------

install(TARGETS libhdbpp_mysql_shared_library
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        NAMELINK_SKIP)


install(TARGETS libhdbpp_mysql_static_library ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(FILES etc/create_hdb++_mysql.sql DESTINATION ${CMAKE_INSTALL_DATADIR}/libhdb++mysql)
install(FILES etc/create_hdb++_mysql_innodb.sql DESTINATION ${CMAKE_INSTALL_DATADIR}/libhdb++mysql)
install(FILES etc/create_hdb++_mysql_innodb_json.sql DESTINATION ${CMAKE_INSTALL_DATADIR}/libhdb++mysql)

