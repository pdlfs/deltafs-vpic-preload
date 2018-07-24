#
# find the PAPI library and set up an imported target for it
#

#
# inputs:
#   - PAPI_INCLUDE_DIR: hint for finding papi.h
#   - PAPI_LIBRARY_DIR: hint for finding the papi lib
#
# output:
#   - "papi" library target
#   - PAPI_FOUND  (set if found)
#

include (FindPackageHandleStandardArgs)

find_path (PAPI_INCLUDE papi.h HINTS ${PAPI_INCLUDE_DIR})
find_library (PAPI_LIBRARY papi HINTS ${PAPI_LIBRARY})

find_package_handle_standard_args (PAPI DEFAULT_MSG
        PAPI_INCLUDE PAPI_LIBRARY)

mark_as_advanced (PAPI_INCLUDE PAPI_LIBRARY)

if (PAPI_FOUND AND NOT TARGET papi)
    add_library (papi UNKNOWN IMPORTED)
    if (NOT "${PAPI_INCLUDE}" STREQUAL "/usr/include")
        set_target_properties (papi PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES "${PAPI_INCLUDE}")
    endif ()
    set_property (TARGET papi APPEND PROPERTY
            IMPORTED_LOCATION "${PAPI_LIBRARY}")
endif ()
