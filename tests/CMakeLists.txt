#
# Copyright (c) 2017 Carnegie Mellon University.
# George Amvrosiadis <gamvrosi@cs.cmu.edu>
#
# All rights reserved.
#
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#

#
# CMakeLists.txt  cmake file for deltafs-shuffle tests/ directory
#
# this file is either included from ../CMakeLists.txt or some other
# file if we are being embedded within another project.
#

#
# test programs
#
add_executable (preload-test preload-test.c)
add_test (test-preload-single ${CMAKE_CURRENT_SOURCE_DIR}/preload-test.sh
                              ${CMAKE_CURRENT_BINARY_DIR}/.. 1)
add_test (test-preload-multi  ${CMAKE_CURRENT_SOURCE_DIR}/preload-test.sh
                              ${CMAKE_CURRENT_BINARY_DIR}/.. 2)

add_executable (shuffle-test shuffle-test.c)
add_test (test-shuffle-single ${CMAKE_CURRENT_SOURCE_DIR}/shuffle-test.sh
                              ${CMAKE_CURRENT_BINARY_DIR}/.. 1)
add_test (test-shuffle-multi  ${CMAKE_CURRENT_SOURCE_DIR}/shuffle-test.sh
                              ${CMAKE_CURRENT_BINARY_DIR}/.. 2)

add_executable (placement-test placement-test.c)
add_test (test-placement-single ${CMAKE_CURRENT_SOURCE_DIR}/placement-test.sh
                                ${CMAKE_CURRENT_BINARY_DIR}/.. 1)
add_test (test-placement-multi  ${CMAKE_CURRENT_SOURCE_DIR}/placement-test.sh
                                ${CMAKE_CURRENT_BINARY_DIR}/.. 2)

#
# make sure we link with MPI
#
foreach (lcvs ${MPI_CXX_COMPILE_FLAGS})
    # mpich on ub14 gives a leading space that we need to trim off
    string (REPLACE " " ";" lcvl ${lcvs})
    foreach (lcv ${lcvl})
        if (NOT ${lcv} STREQUAL "")
            target_compile_options (preload-test PUBLIC $<BUILD_INTERFACE:${lcv}>)
            target_compile_options (shuffle-test PUBLIC $<BUILD_INTERFACE:${lcv}>)
            target_compile_options (placement-test PUBLIC $<BUILD_INTERFACE:${lcv}>)
        endif ()
    endforeach ()
endforeach ()

# XXX: have to do this one dir at a time otherwise, otherwise I get
# error: target 'deltafs' INTERFACE_INCLUDE_DIRECTORIES contains path
#         prefixed in the source directory
foreach (lcv ${MPI_CXX_INCLUDE_PATH})
    target_include_directories (preload-test PUBLIC $<BUILD_INTERFACE:${lcv}>)
    target_include_directories (shuffle-test PUBLIC $<BUILD_INTERFACE:${lcv}>)
    target_include_directories (placement-test PUBLIC $<BUILD_INTERFACE:${lcv}>)
endforeach ()

foreach (lcv ${MPI_CXX_LIBRARIES})
    target_link_libraries(preload-test $<BUILD_INTERFACE:${lcv}>)
    target_link_libraries(shuffle-test $<BUILD_INTERFACE:${lcv}>)
    target_link_libraries(placement-test $<BUILD_INTERFACE:${lcv}>)
endforeach ()

# XXX: this doesn't propagate to lib users, is that a problem?
# XXX: prob ok.
set_property (TARGET preload-test APPEND PROPERTY LINK_FLAGS ${MPI_CXX_LINK_FLAGS})
set_property (TARGET shuffle-test APPEND PROPERTY LINK_FLAGS ${MPI_CXX_LINK_FLAGS})
set_property (TARGET placement-test APPEND PROPERTY LINK_FLAGS ${MPI_CXX_LINK_FLAGS})
