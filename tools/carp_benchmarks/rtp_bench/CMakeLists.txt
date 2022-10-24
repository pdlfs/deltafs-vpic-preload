add_executable(rtp-bench-runner rtp_bench_runner.cc rtp_bench.cc)
target_link_libraries(rtp-bench-runner carp-base)

link_with_mpi_manual(rtp-bench-runner)

install(TARGETS rtp-bench-runner
        RUNTIME DESTINATION bin)
