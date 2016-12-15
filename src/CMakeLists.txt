#
# CMakeLists.txt  cmake file for deltafs-preload directory
# 09-Dec-2016  chuck@ece.cmu.edu
#

#
# this file is either included from ../CMakeLists.txt or some other
# file if we are being embedded within another project.
#
#
# since this is for a preload library, we don't expect anyone to
# link to us like a normal lib, so this CMakeLists.txt is cut down
# from a normal library config...
#

# configure/load in standard modules we plan to use
set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
set (THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package (Threads REQUIRED)

#
# create the library target 
#
add_library (deltafs-preload deltafs-preload.cc)
target_link_libraries (deltafs-preload deltafs)

if (CMAKE_THREAD_LIBS_INIT)
  target_link_libraries (deltafs-preload "${CMAKE_THREAD_LIBS_INIT}")
endif ()

#
# testing/debug options
#
if (PLFS_DIRMODE_BYPASS)
  target_compile_definitions (deltafs-preload PRIVATE -DPLFS_DIRMODE_BYPASS)
endif ()
if (SHUFFLE_BYPASS)
  target_compile_definitions (deltafs-preload PRIVATE -DSHUFFLE_BYPASS)
endif ()

#
# test program
#
add_executable (deltafs-preload-test deltafs-preload-test.c)
add_test (preload-test ${CMAKE_CURRENT_SOURCE_DIR}/preload-test.sh)

#
# make sure we link with MPI
#
foreach (lcvs ${MPI_CXX_COMPILE_FLAGS})
    # mpich on ub14 gives a leading space that we need to trim off
    string (REPLACE " " ";" lcvl ${lcvs})
    foreach (lcv ${lcvl})
        if (NOT ${lcv} STREQUAL "")
            target_compile_options (deltafs-preload
                                    PUBLIC $<BUILD_INTERFACE:${lcv}>)
            target_compile_options (deltafs-preload-test
                                    PUBLIC $<BUILD_INTERFACE:${lcv}>)
        endif ()
    endforeach ()
endforeach ()

# XXX: have to do this one dir at a time otherwise, otherwise I get
# error: target 'deltafs' INTERFACE_INCLUDE_DIRECTORIES contains path
#         prefixed in the source directory
foreach (lcv ${MPI_CXX_INCLUDE_PATH})
    target_include_directories (deltafs-preload
                                PUBLIC $<BUILD_INTERFACE:${lcv}>)
    target_include_directories (deltafs-preload-test
                                PUBLIC $<BUILD_INTERFACE:${lcv}>)
endforeach ()

foreach (lcv ${MPI_CXX_LIBRARIES})
    target_link_libraries(deltafs-preload $<BUILD_INTERFACE:${lcv}>)
    target_link_libraries(deltafs-preload-test $<BUILD_INTERFACE:${lcv}>)
endforeach ()

# XXX: this doesn't propagate to lib users, is that a problem?
# XXX: prob ok.
set_property (TARGET deltafs-preload APPEND PROPERTY LINK_FLAGS
              ${MPI_CXX_LINK_FLAGS})
set_property (TARGET deltafs-preload-test APPEND PROPERTY LINK_FLAGS
              ${MPI_CXX_LINK_FLAGS})

#
# installation stuff 
#

# "make install" rules...
install (TARGETS deltafs-preload
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)

