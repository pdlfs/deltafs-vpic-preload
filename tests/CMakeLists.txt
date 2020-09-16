#
# test programs
#
set (tests range_utils)

foreach (tst ${tests})

    add_executable (${tst}-test ${tst}-test.cc)
    target_link_libraries (${tst}-test deltafs-common deltafs-preload)
    add_test(${tst}-test ${tst}-test)

endforeach ()
