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
# installation stuff 
#

# "make install" rules...
install (TARGETS deltafs-preload 
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)

