find_package (MPI MODULE REQUIRED)
string (REPLACE " " ";" MPI_CXX_COMPILE_FLAGS_LIST "${MPI_CXX_COMPILE_FLAGS}")

# link shared lib with full rpath
set (CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

add_executable (vpic-deltafs-reader parallel_reader.cc)
target_link_libraries (vpic-deltafs-reader deltafs ch-placement m)

foreach (lcv ${MPI_CXX_COMPILE_FLAGS_LIST})
    if (NOT ${lcv} STREQUAL "")
        target_compile_options (vpic-deltafs-reader PUBLIC ${lcv})
    endif ()
endforeach ()

# XXX: have to do this one dir at a time otherwise, otherwise I get
# error: target 'deltafs' INTERFACE_INCLUDE_DIRECTORIES contains path
#               prefixed in the source directory
foreach (lcv ${MPI_CXX_INCLUDE_PATH})
    target_include_directories (vpic-deltafs-reader PUBLIC ${lcv})
endforeach ()

foreach (lcv ${MPI_CXX_LIBRARIES})
    target_link_libraries (vpic-deltafs-reader ${lcv})
endforeach ()

set_property (TARGET vpic-deltafs-reader APPEND PROPERTY LINK_FLAGS
              ${MPI_CXX_LINK_FLAGS})

#
# "make install" rules
#
install (TARGETS vpic-deltafs-reader
         RUNTIME DESTINATION bin)
