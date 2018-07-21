#
# find the NUMA library and set up an imported target for it
#

#
# inputs:
#   - NUMA_INCLUDE_DIR: hint for finding numa.h
#   - NUMA_LIBRARY_DIR: hint for finding the numa lib
#
# output:
#   - "numa" library target
#   - NUMA_FOUND  (set if found)
#

include (FindPackageHandleStandardArgs)

find_path (NUMA_INCLUDE numa.h HINTS ${NUMA_INCLUDE_DIR})
find_library (NUMA_LIBRARY numa HINTS ${NUMA_LIBRARY})

find_package_handle_standard_args (NUMA DEFAULT_MSG
        NUMA_INCLUDE NUMA_LIBRARY)

mark_as_advanced (NUMA_INCLUDE NUMA_LIBRARY)

if (NUMA_FOUND AND NOT TARGET numa)
    add_library (numa UNKNOWN IMPORTED)
    set_target_properties (numa PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${NUMA_INCLUDE}")
    set_property (TARGET numa APPEND PROPERTY
            IMPORTED_LOCATION "${NUMA_LIBRARY}")
endif ()
