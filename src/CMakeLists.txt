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

set(CMAKE_CXX_STANDARD 11)

# configure/load in standard modules we plan to use
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

if (PRELOAD_PAPI)
    find_package(PAPI MODULE)
    if (PAPI_FOUND)
        add_definitions(-DPRELOAD_HAS_PAPI)
    endif ()
endif ()

if (PRELOAD_NUMA)
    find_package(NUMA MODULE)
    if (NUMA_FOUND)
        add_definitions(-DPRELOAD_HAS_NUMA)
    endif ()
endif ()

#
# create the library target
#
set(CARP_SOURCES
        carp/carp.cc
        carp/carp_perflog.cc
        carp/carp_preload.cc
        carp/msgfmt.cc
        carp/oob_buffer.cc
        carp/ordered_bins.cc
        carp/pivot_buffer.cc
        carp/pivot_union.cc
        carp/pivots.cc
        carp/policy.cc
        carp/range_common.cc
        carp/rtp.cc
        carp/rtp_state_mgr.cc
        carp/rtp_util.cc
        carp/shuffle_write_range.cc
        )

set(PRELOAD_SOURCES
        preload.cc
        preload_internal.cc
        preload_mon.cc
        preload_shuffle.cc
        nn_shuffler.cc
        nn_shuffler_internal.cc
        xn_shuffle.cc
        hstg.cc
        common.cc
        pthreadtap.cc
        )

add_library(deltafs-preload
        ${CARP_SOURCES}
        ${PRELOAD_SOURCES})

target_include_directories(deltafs-preload PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        )

target_link_libraries(deltafs-preload deltafs mercury mssg
        deltafs-nexus deltafs-shuffle Threads::Threads ${CMAKE_DL_LIBS})

if (PRELOAD_CH_PLACEMENT)
    target_link_libraries(deltafs-preload ch-placement)
endif ()

if (PRELOAD_PAPI)
    if (PAPI_FOUND)
        target_link_libraries(deltafs-preload papi)
    endif ()
endif ()

if (PRELOAD_NUMA)
    if (NUMA_FOUND)
        target_link_libraries(deltafs-preload numa)
    endif ()
endif ()

#
# make sure we link with MPI.  use "MPI_CXX_COMPILE_FLAGS_LIST"
# prepared by the calling module.
#
foreach (tgt deltafs-preload)

    # mpich on ub14 gives a leading space that we need to trim off
    foreach (lcv ${MPI_CXX_COMPILE_FLAGS_LIST})
        if (NOT ${lcv} STREQUAL "")
            target_compile_options(${tgt}
                    PUBLIC $<BUILD_INTERFACE:${lcv}>)
        endif ()
    endforeach ()

    # XXX: have to do this one dir at a time
    foreach (lcv ${MPI_CXX_INCLUDE_PATH})
        target_include_directories(${tgt}
                PUBLIC $<BUILD_INTERFACE:${lcv}>)
    endforeach ()

    foreach (lcv ${MPI_CXX_LIBRARIES})
        target_link_libraries(${tgt} $<BUILD_INTERFACE:${lcv}>)
    endforeach ()

    set_property(TARGET ${tgt} APPEND PROPERTY LINK_FLAGS
            ${MPI_CXX_LINK_FLAGS})

endforeach ()

#
# installation stuff
#

# "make install" rules...
install(TARGETS deltafs-preload
        ARCHIVE DESTINATION lib LIBRARY DESTINATION lib)
