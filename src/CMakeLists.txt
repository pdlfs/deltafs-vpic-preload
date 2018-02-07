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
add_library (deltafs-preload preload.cc preload_internal.cc preload_mon.cc
        preload_shuffle.cc nn_shuffler.cc nn_shuffler_internal.cc
        xn_shuffler.cc shuffler/shuffler.cc shuffler/shuf_mlog.cc
        shuffler/mlog.c shuffler/acnt_wrap.c hstg.cc common.cc
        pthreadtap.cc)

target_link_libraries (deltafs-preload deltafs mercury ssg ch-placement
        deltafs-nexus Threads::Threads ${CMAKE_DL_LIBS})

#
# make sure we link with MPI.  use "MPI_CXX_COMPILE_FLAGS_LIST"
# prepared by the calling module.
#
foreach (tgt deltafs-preload)

    # mpich on ub14 gives a leading space that we need to trim off
    foreach (lcv ${MPI_CXX_COMPILE_FLAGS_LIST})
        if (NOT ${lcv} STREQUAL "")
            target_compile_options (${tgt}
                    PUBLIC $<BUILD_INTERFACE:${lcv}>)
        endif ()
    endforeach ()

    # XXX: have to do this one dir at a time
    foreach (lcv ${MPI_CXX_INCLUDE_PATH})
        target_include_directories (${tgt}
                PUBLIC $<BUILD_INTERFACE:${lcv}>)
    endforeach ()

    foreach (lcv ${MPI_CXX_LIBRARIES})
        target_link_libraries(${tgt} $<BUILD_INTERFACE:${lcv}>)
    endforeach ()

    set_property (TARGET ${tgt} APPEND PROPERTY LINK_FLAGS
            ${MPI_CXX_LINK_FLAGS})

endforeach ()

#
# installation stuff
#

# "make install" rules...
install (TARGETS deltafs-preload
        ARCHIVE DESTINATION lib LIBRARY DESTINATION lib)

#
# tests
#

foreach (tst preload)
    add_executable (${tst}-test ${tst}-test.cc)
    add_test (${tst}-single ${CMAKE_CURRENT_SOURCE_DIR}/${tst}-test.sh
            ${CMAKE_BINARY_DIR} 1)
    add_test (${tst}-multi  ${CMAKE_CURRENT_SOURCE_DIR}/${tst}-test.sh
            ${CMAKE_BINARY_DIR} 2)

    # and because they all use MPI...
    foreach (lcv ${MPI_CXX_COMPILE_FLAGS_LIST})
        if (NOT ${lcv} STREQUAL "")
            target_compile_options (${tst}-test PUBLIC ${lcv})
        endif ()
    endforeach ()

    # XXX: have to do this one dir at a time
    foreach (lcv ${MPI_CXX_INCLUDE_PATH})
        target_include_directories (${tst}-test PUBLIC ${lcv})
    endforeach ()

    foreach (lcv ${MPI_CXX_LIBRARIES})
        target_link_libraries (${tst}-test ${lcv})
    endforeach ()

    set_property (TARGET ${tst}-test APPEND PROPERTY LINK_FLAGS
            ${MPI_CXX_LINK_FLAGS})

endforeach ()
