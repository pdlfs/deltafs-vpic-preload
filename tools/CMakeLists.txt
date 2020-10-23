#
# CMakeLists.txt  cmake file for the tools subdirectory
# 09-Dec-2016
#

# configure/load in standard modules we plan to use
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

# link shared lib with full rpath
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

add_executable(preload-plfsdir-reader preload_plfsdir_reader.cc)
target_link_libraries(preload-plfsdir-reader deltafs)

add_executable(preload-reader preload_reader.cc)
target_link_libraries(preload-reader deltafs)

add_executable(preload-runner preload_runner.cc)
target_link_libraries(preload-runner deltafs-preload Threads::Threads)

add_executable(range-runner range_runner.cc workload_generator.cc)
target_link_libraries(range-runner deltafs-preload Threads::Threads)

add_executable(preload-runner-no-deltafs preload_runner.cc)
target_link_libraries(preload-runner-no-deltafs Threads::Threads)

add_executable(range-reader query_main.cc query_client/query_client.cc
        query_client/sortable_buffer.cc ../src/range_backend/range_backend.cc)

target_include_directories(range-reader PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../src>
        )

#
# make sure we link with MPI.  use "MPI_CXX_COMPILE_FLAGS_LIST"
# prepared by the calling module.
#
foreach (tgt preload-runner range-runner preload-runner-no-deltafs range-reader)

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
# "make install" rules
#
install(TARGETS preload-runner preload-runner-no-deltafs range-runner
        RUNTIME DESTINATION bin)

install(TARGETS preload-plfsdir-reader
        RUNTIME DESTINATION bin)

install(TARGETS preload-reader
        RUNTIME DESTINATION bin)

install(TARGETS range-reader
        RUNTIME DESTINATION bin)
