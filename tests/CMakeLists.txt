#
# test programs
#
set (tests range_utils rtp)

foreach (tst ${tests})

    add_executable (${tst}-test ${tst}-test.cc pivot_calc_9_data.cc)
    target_link_libraries (${tst}-test deltafs-common deltafs-preload)
    add_test(${tst}-test ${tst}-test)

endforeach ()
