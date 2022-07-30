set(CARP_SOURCES
        carp/carp.cc
        carp/carp_utils.cc
        carp/data_buffer.cc
        carp/oob_buffer.cc
        carp/policy.cc
        carp/regular_trigger.cc
        carp/rtp.cc
        carp/rtp_state_mgr.cc
        carp/rtp_util.cc
        carp/stat_trigger.cc
        )

set(PRELOAD_SOURCES
        perfstats/perfstats.cc
        perfstats/stat.cc
        preload_internal.cc
        preload_mon.cc
        preload_shuffle.cc
        shuffle_write_providers.cc
        msgfmt.cc
        nn_shuffler.cc
        nn_shuffler_internal.cc
        xn_shuffle.cc
        hstg.cc
        common.cc
        pthreadtap.cc
        range_common.cc
        range_utils.cc
        )

set(OTHER_SOURCES
        range_backend/mock_backend.cc
        )

set(ALL_SOURCES "")

foreach (tgt ${CARP_SOURCES} ${PRELOAD_SOURCES} ${OTHER_SOURCES})
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

add_executable(rtp-bench-runner rtp_bench_runner.cc rtp_bench.cc)
target_link_libraries(rtp-bench-runner carp-base)

# Copy MPI hacks from parent CMakeLists. Is there a cleaner way?
foreach (lcv ${MPI_CXX_COMPILE_FLAGS_LIST})
    if (NOT ${lcv} STREQUAL "")
        target_compile_options(rtp-bench-runner
                PUBLIC $<BUILD_INTERFACE:${lcv}>)
    endif ()
endforeach ()

# XXX: have to do this one dir at a time
foreach (lcv ${MPI_CXX_INCLUDE_PATH})
    target_include_directories(rtp-bench-runner
            PUBLIC $<BUILD_INTERFACE:${lcv}>)
endforeach ()

foreach (lcv ${MPI_CXX_LIBRARIES})
    target_link_libraries(rtp-bench-runner $<BUILD_INTERFACE:${lcv}>)
endforeach ()

set_property(TARGET rtp-bench-runner APPEND PROPERTY LINK_FLAGS
        ${MPI_CXX_LINK_FLAGS})

install(TARGETS rtp-bench-runner
        RUNTIME DESTINATION bin)
