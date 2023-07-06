# Copy MPI hacks from parent CMakeLists. Is there a cleaner way?
function(link_with_mpi_manual tgt)
    message("Target ${tgt}")
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
endfunction()

set(CARP_SOURCES
        carp/carp.cc
        carp/carp_perflog.cc
        carp/carp_preload.cc
        carp/carp_utils.cc
        carp/msgfmt.cc
        carp/oob_buffer.cc
        carp/ordered_bins.cc
        carp/pivot_buffer.cc
        carp/pivots.cc
        carp/policy.cc
        carp/range_common.cc
        carp/range_utils.cc
        carp/rtp.cc
        carp/rtp_state_mgr.cc
        carp/rtp_util.cc
        carp/shuffle_write_range.cc
        )

set(PRELOAD_SOURCES
        common.cc
        hstg.cc
        nn_shuffler.cc
        nn_shuffler_internal.cc
        preload_internal.cc
        preload_mon.cc
        preload_shuffle.cc
        pthreadtap.cc
        xn_shuffle.cc
        )

set(ALL_SOURCES "")

foreach (tgt ${CARP_SOURCES} ${PRELOAD_SOURCES})
    list(APPEND ALL_SOURCES ${CMAKE_SOURCE_DIR}/src/${tgt})
endforeach ()

# To prevent preloading we exclude preload.cc from carp-base
# This almost works but there is no definition of preload_write
# which is invoked outside. We provide that symbol in preload_funcs.cc
add_library(carp-base STATIC ${ALL_SOURCES} preload_funcs.cc)
target_link_libraries(carp-base
        mercury
        mssg
        deltafs-common
        deltafs-nexus
        deltafs-shuffle
        deltafs
        Threads::Threads)
target_include_directories(carp-base PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src/>)

add_subdirectory(pivot_bench)
add_subdirectory(rtp_bench)
