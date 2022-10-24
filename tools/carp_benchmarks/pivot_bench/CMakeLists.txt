add_executable(pivot-bench-runner pivot_bench_runner.cc)
target_link_libraries(pivot-bench-runner carp-base)
link_with_mpi_manual(pivot-bench-runner)
install(TARGETS pivot-bench-runner
        RUNTIME DESTINATION bin)
