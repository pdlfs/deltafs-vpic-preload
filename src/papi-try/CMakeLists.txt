#
# CMakeLists.txt  cmake for papi-try
#

#
# Use CMAKE_INSTALL_PATH or CMAKE_PREFIX_PATH to specify custom lib locations
#
cmake_minimum_required(VERSION 3.1)

project (papi-try C CXX)

# add our cmake module directory to the path
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set (CMAKE_CXX_STANDARD 11)

find_package (MPI MODULE REQUIRED)
string (REPLACE " " ";" MPI_CXX_COMPILE_FLAGS_LIST "${MPI_CXX_COMPILE_FLAGS}")

set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
set (THREADS_PREFER_PTHREAD_FLAG TRUE)

find_package (Threads REQUIRED)

find_package (NUMA REQUIRED)
find_package (PAPI REQUIRED)

include (CheckCXXCompilerFlag)
check_cxx_compiler_flag (-Wall flagWall)
if (flagWall)
    add_compile_options (-Wall)
endif ()

add_executable (papi-try papi-try.cc)
target_link_libraries (papi-try papi numa Threads::Threads)

# XXX: MPICH on Ubuntu gives a leading space that we need to trim off
foreach (lcv ${MPI_CXX_COMPILE_FLAGS_LIST})
    if (NOT ${lcv} STREQUAL "")
        target_compile_options (papi-try PUBLIC $<BUILD_INTERFACE:${lcv}>)
    endif ()
endforeach ()

foreach (lcv ${MPI_CXX_INCLUDE_PATH})
    target_include_directories (papi-try PUBLIC $<BUILD_INTERFACE:${lcv}>)
endforeach ()

foreach (lcv ${MPI_CXX_LIBRARIES})
    target_link_libraries(papi-try $<BUILD_INTERFACE:${lcv}>)
endforeach ()

set_property (TARGET papi-try APPEND PROPERTY LINK_FLAGS
        ${MPI_CXX_LINK_FLAGS})

#
# "make install" rule
#
install (TARGETS papi-try RUNTIME DESTINATION bin)
