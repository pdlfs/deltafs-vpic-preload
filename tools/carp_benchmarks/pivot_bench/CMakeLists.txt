set(PVTBENCH_SOURCES
        pivot_aggr.cc
        pivot_bench_runner.cc
        pivot_bench.cc)
add_executable(pivot-bench-runner ${PVTBENCH_SOURCES})
target_link_libraries(pivot-bench-runner carp-base)
link_with_mpi_manual(pivot-bench-runner)
install(TARGETS pivot-bench-runner
        RUNTIME DESTINATION bin)
