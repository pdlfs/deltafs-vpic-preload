# link shared lib with full rpath
set (CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

add_executable (vpic-deltafs-reader vpic-deltafs-reader.cc)
target_link_libraries (vpic-deltafs-reader deltafs ch-placement)

#
# "make install" rules
#
install (TARGETS vpic-deltafs-reader
         RUNTIME DESTINATION bin)
