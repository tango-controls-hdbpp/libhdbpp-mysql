# Functions and Pre-build  -----------------------------------

# Stop messy in source builds
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_DISABLE_SOURCE_CHANGES  ON)

if ( ${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR} )
    message( FATAL_ERROR "In-source builds not allowed. Please make a new directory (called a build directory) and run CMake from there. You may need to remove CMakeCache.txt." )
endif()

# Start Build Config -----------------------------------
cmake_minimum_required(VERSION 3.11)
set(CMAKE_SKIP_RPATH true)
set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_COLOR_MAKEFILE ON)

# Library output name for the final binaries
set(LIBHDBPP_MYSQL_NAME "libhdb++mysql")

# Versioning
set(VERSION_MAJOR "7")
set(VERSION_MINOR "0")
set(VERSION_PATCH "0")
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
pkg_search_module(LIBMYSQLCLIENT REQUIRED mysqlclient libmariadb>=3)
if(LIBMYSQLCLIENT_FOUND)
  list(GET LIBMYSQLCLIENT_LIBRARIES 0 LIBNAME)
  list(GET LIBMYSQLCLIENT_LINK_LIBRARIES 0 LIBPATH)
  message(STATUS "Found ${LIBNAME} version ${LIBMYSQLCLIENT_VERSION} at: " ${LIBPATH})
endif()

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

# Find the libhdbpp headers
find_package(libhdbpp)

# If not installed then get them
if(NOT libhdbpp_FOUND)
    include(FetchContent)
    FetchContent_Declare(
      libhdbpp
      GIT_REPOSITORY https://github.com/tango-controls-hdbpp/libhdbpp.git
#set to master as soon as libhdbpp project-build gets merged
#      GIT_TAG        master
      GIT_TAG        project-build
    )

    FetchContent_GetProperties(libhdbpp)
    if(NOT libhdbpp_POPULATED)
        FetchContent_Populate(libhdbpp)
        add_subdirectory(${libhdbpp_SOURCE_DIR} ${libhdbpp_BINARY_DIR})
    endif()

    add_library(libhdbpp::libhdbpp_headers ALIAS libhdbpp_headers)

endif(NOT libhdbpp_FOUND)

# Add any include paths from the command line
set(INCLUDE_PATHS ${INCLUDE_PATHS} ${CMAKE_INCLUDE_PATH})

foreach(INC_PATH ${CMAKE_INCLUDE_PATH})
   message(STATUS "Extra include path: " ${INC_PATH})
endforeach()

message(STATUS "Building library paths...")

foreach(LIB_PATH ${CMAKE_LIBRARY_PATH})
   message(STATUS "Extra library search path: " ${LIB_PATH})
endforeach()

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

# Export target name for subdirectory build
get_directory_property(hasParent PARENT_DIRECTORY)
if(hasParent)
    set(BACKEND_TARGET_SHARED "libhdbpp_mysql_shared_library" PARENT_SCOPE)
    set(BACKEND_TARGET_STATIC "libhdbpp_mysql_static_library" PARENT_SCOPE)
endif(hasParent)

set(BACKEND_TARGET_SHARED "libhdbpp_mysql_shared_library")
set(BACKEND_TARGET_STATIC "libhdbpp_mysql_static_library")

# Shared library ------------------------------------------------------

add_library(${BACKEND_TARGET_SHARED} SHARED ${SRC_FILES})

target_link_libraries(${BACKEND_TARGET_SHARED} PRIVATE ${LIBMYSQLCLIENT_LIBRARIES})
target_link_libraries(${BACKEND_TARGET_SHARED} PUBLIC libhdbpp::libhdbpp_headers PRIVATE TangoInterfaceLibrary)

set_target_properties(${BACKEND_TARGET_SHARED} PROPERTIES 
        OUTPUT_NAME hdb++mysql
        LINK_FLAGS "-Wl,--no-undefined")

target_include_directories(${BACKEND_TARGET_SHARED} PRIVATE ${LIBMYSQLCLIENT_INCLUDE_DIRS})

target_include_directories(${BACKEND_TARGET_SHARED} 
    PUBLIC
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    PRIVATE 
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>
        ${INCLUDE_PATHS}
        "${PROJECT_BINARY_DIR}")

target_compile_options(${BACKEND_TARGET_SHARED} PUBLIC -DRELEASE="${VERSION_STRING}" -Wall)

# This will version the shared library
set_target_properties(${BACKEND_TARGET_SHARED}
    PROPERTIES VERSION ${VERSION_STRING}
    SOVERSION ${VERSION_MAJOR})


# Static library ------------------------------------------------------

add_library(${BACKEND_TARGET_STATIC} STATIC ${SRC_FILES})
target_link_libraries(${BACKEND_TARGET_STATIC} PRIVATE ${LIBMYSQLCLIENT_LIBRARIES})
target_link_libraries(${BACKEND_TARGET_STATIC} PUBLIC libhdbpp::libhdbpp_headers PRIVATE TangoInterfaceLibrary)
set_target_properties(${BACKEND_TARGET_STATIC} PROPERTIES OUTPUT_NAME hdb++mysql)

target_include_directories(${BACKEND_TARGET_STATIC} PRIVATE ${LIBMYSQLCLIENT_INCLUDE_DIRS})

target_include_directories(${BACKEND_TARGET_STATIC} 
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    PRIVATE 
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>
        ${INCLUDE_PATHS}
        "${PROJECT_BINARY_DIR}")

target_compile_options(${BACKEND_TARGET_STATIC} PUBLIC -DRELEASE="${VERSION_STRING}" -Wall)


# Install -----------------------------------------------------------
include(CMakePackageConfigHelpers)

install(TARGETS ${BACKEND_TARGET_SHARED}
        EXPORT libhdbpp-mysqlTargets
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        )

set(ConfigPackageLocation lib/cmake/libhdbpp-mysql)
set(Namespace libhdbpp::)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/libhdbpp-mysql/libhdbpp-mysqlConfigVersion.cmake"
  VERSION ${VERSION_STRING}
  COMPATIBILITY AnyNewerVersion
)

export(EXPORT libhdbpp-mysqlTargets
  FILE
    "{CMAKE_CURRENT_BINARY_DIR}/libhdbpp-mysql/libhdbpp-mysqlTargets.cmake"
  NAMESPACE ${Namespace}
)

# generate the config file that is includes the exports
configure_package_config_file(cmake/libhdbpp-mysqlConfig.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/libhdbpp-mysql/libhdbpp-mysqlConfig.cmake"
  INSTALL_DESTINATION
    ${ConfigPackageLocation}
  NO_SET_AND_CHECK_MACRO
  NO_CHECK_REQUIRED_COMPONENTS_MACRO
)

install(EXPORT libhdbpp-mysqlTargets
  FILE
    libhdbpp-mysqlTargets.cmake
  NAMESPACE
    ${Namespace}
  DESTINATION
    ${ConfigPackageLocation}
)

install(
  FILES
    "${CMAKE_CURRENT_BINARY_DIR}/libhdbpp-mysql/libhdbpp-mysqlConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/libhdbpp-mysql/libhdbpp-mysqlConfigVersion.cmake"
  DESTINATION
    ${ConfigPackageLocation}
)

install(TARGETS ${BACKEND_TARGET_STATIC} ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(FILES etc/create_hdb++_mysql.sql DESTINATION ${CMAKE_INSTALL_DATADIR}/libhdb++mysql)
install(FILES etc/create_hdb++_mysql_innodb.sql DESTINATION ${CMAKE_INSTALL_DATADIR}/libhdb++mysql)
install(FILES etc/create_hdb++_mysql_innodb_json.sql DESTINATION ${CMAKE_INSTALL_DATADIR}/libhdb++mysql)

