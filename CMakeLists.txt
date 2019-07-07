#
# CMakeLists.txt  top-level cmake file for deltafs-preload
# 09-Dec-2016  chuck@ece.cmu.edu
#

#
#  general cmake flags:
#    -DCMAKE_INSTALL_PREFIX=/usr/local     -- the prefix for installing
#    -DCMAKE_BUILD_TYPE=type               -- type can be DEBUG, RELEASE, ...
#    -DCMAKE_PREFIX_PATH=/dir              -- external packages
#    -DBUILD_TOOLS=NO                      -- build tools
#
#     note that CMAKE_PREFIX_PATH can be a list of directories:
#      -DCMAKE_PREFIX_PATH='/dir1;/dir2;/dir3'
#

cmake_minimum_required (VERSION 3.1)
project (deltafs-preload C CXX)
set (CMAKE_CXX_STANDARD 11)

# add our cmake module directory to the path
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
        "${CMAKE_CURRENT_SOURCE_DIR}/src/papi-try/cmake")

# link shared lib with full rpath
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# preload library is always shared, force on and hide it as an advanced var
set (BUILD_SHARED_LIBS On CACHE STRING "Build shared libs (must be on)." FORCE)
mark_as_advanced (BUILD_SHARED_LIBS)

# setup cache variables for ccmake
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release
         CACHE STRING "Choose the type of build." FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
                  "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set (DEBUG_SANITIZER Off CACHE STRING "Sanitizer for debug builds")
set_property (CACHE DEBUG_SANITIZER PROPERTY STRINGS
              "Off" "Address" "Thread")
set (CMAKE_PREFIX_PATH "" CACHE STRING "External dependencies path")

#
# sanitizer config (XXX: does not probe compiler to see if sanitizer flags
# are supported... )
#
set (as_flags "-fsanitize=address -O1 -fno-omit-frame-pointer")
set (ts_flags "-fsanitize=thread  -O1 -fno-omit-frame-pointer")
if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    if (${DEBUG_SANITIZER} STREQUAL "Address")
        set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${as_flags}")
        set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${as_flags}")
    elseif (${DEBUG_SANITIZER} STREQUAL "Thread")
        set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${ts_flags}")
        set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${ts_flags}")
    endif ()
endif ()

# Find required packages
find_package (MPI MODULE REQUIRED)
string (REPLACE " " ";" MPI_CXX_COMPILE_FLAGS_LIST "${MPI_CXX_COMPILE_FLAGS}")

find_package (mercury CONFIG REQUIRED)
find_package (deltafs CONFIG REQUIRED)
find_package (deltafs-nexus CONFIG REQUIRED)
find_package (mssg CONFIG REQUIRED)

# libch-placement
set (PRELOAD_CH_PLACEMENT "ON" CACHE BOOL "Build with libch-placement")

if (PRELOAD_CH_PLACEMENT)
    include (xpkg-import) # comes with deltafs
    xpkg_import_module (ch-placement REQUIRED ch-placement)
    add_definitions (-DPRELOAD_HAS_CH_PLACEMENT)
endif ()

# libnuma and libpapi
set (PRELOAD_NUMA "ON" CACHE BOOL "Build with libnuma")
set (PRELOAD_PAPI "ON" CACHE BOOL "Build with libpapi")

# user hook to build tools
set (BUILD_TOOLS "ON" CACHE BOOL "Build tools")

add_subdirectory (src)
if (BUILD_TOOLS)
    add_subdirectory (tools)
endif ()
