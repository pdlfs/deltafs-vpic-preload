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

#
# test programs
#
set (tests preload shuffle placement)  # XXX: add later: noplfs, deltafs

foreach (tst ${tests})

    add_executable (${tst}-test ${tst}-test.c)
    add_test (${tst}-single ${CMAKE_CURRENT_SOURCE_DIR}/${tst}-test.sh
                            ${CMAKE_BINARY_DIR} 1)
    add_test (${tst}-multi  ${CMAKE_CURRENT_SOURCE_DIR}/${tst}-test.sh
                            ${CMAKE_BINARY_DIR} 2)

    # and because they all use MPI...

    foreach (lcvs ${MPI_CXX_COMPILE_FLAGS})
        # mpich on ub14 gives a leading space that we need to trim off
        string (REPLACE " " ";" lcvl ${lcvs})
        foreach (lcv ${lcvl})
            if (NOT ${lcv} STREQUAL "")
                target_compile_options (${tst}-test ${lcv})
            endif ()
        endforeach ()
    endforeach ()

    # XXX: have to do this one dir at a time otherwise, otherwise I get
    # error: target 'deltafs' INTERFACE_INCLUDE_DIRECTORIES contains path
    #               prefixed in the source directory
    foreach (lcv ${MPI_CXX_INCLUDE_PATH})
        target_include_directories (${tst}-test PUBLIC ${lcv})
    endforeach ()

    foreach (lcv ${MPI_CXX_LIBRARIES})
        target_link_libraries (${tst}-test ${lcv})
    endforeach ()

    set_property (TARGET ${tst}-test APPEND PROPERTY LINK_FLAGS
                  ${MPI_CXX_LINK_FLAGS})
    
endforeach ()
