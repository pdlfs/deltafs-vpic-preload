include(FindPackageHandleStandardArgs)

find_path(BLKID_INCLUDE_DIR blkid/blkid.h)
find_library(BLKID_LIBRARY blkid)

find_package_handle_standard_args(blkid DEFAULT_MSG BLKID_LIBRARY BLKID_INCLUDE_DIR)

mark_as_advanced(BLKID_LIBRARY BLKID_INCLUDE_DIR)

if (BLKID_FOUND)
  message(STATUS "libblkid found")
  add_library(blkid UNKNOWN IMPORTED)
  set_property(TARGET blkid APPEND PROPERTY IMPORTED_LOCATION "${BLKID_LIBRARY}")
elseif (BLKID_FIND_REQUIRED)
  message(FATAL_ERROR "libblkid not found")
endif()
