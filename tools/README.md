# preload-runner

A simple `VPIC` particle I/O emulator using an `one-file-per-particle` model for performance testing on VPIC I/O.

## Sample configuration for a direct VPIC I/O run without DeltaFS

```bash
mpirun -np 2 \
    -env VPIC_current_working_dir `pwd` \
    -env PRELOAD_Ignore_dirs "fields:hydro:rundata:names" \
    -env PRELOAD_Log_home `pwd` \
    -env PRELOAD_Enable_verbose_error 1 \
  `pwd`/../preload-runner -T 1 -d 2 -s 5 -b 40 -c 16
```

The MPI options above are written in the `MPICH` format. For `OPENMPI`, change every `-env K V` to `-x K=V`.

This will launch 2 MPI processes (`-np 2`), with 16 particles per process per timestep dump (`-c 16`). There are 2 dumps (`-d 2`) in total. Each dump goes through 5 timesteps (`-s 5`). Each particle is 48 bytes, consisting of an 8-byte particle ID and 40-byte particle data (`-b 40`). Paticle IDs are used as particle filenames during particle I/O.

## Sample configuration for a VPIC I/O run with DeltaFS but bypassing all data processing including data writing

```bash
mpirun -np 2 \
    -env VPIC_current_working_dir `pwd` \
    -env PRELOAD_Ignore_dirs "fields:hydro:rundata:names" \
    -env PRELOAD_Log_home `pwd` \
    -env PRELOAD_Enable_verbose_error 1 \
    -env PRELOAD_Deltafs_mntp particle \
    -env PRELOAD_Bypass_shuffle 1 \
    -env PRELOAD_Bypass_write 1 \
  `pwd`/../preload-runner -T 1 -d 2 -s 5 -b 40 -c 16
```
The MPI options above are written in the `MPICH` format. For `OPENMPI`, change every `-env K V` to `-x K=V`.

This will launch 2 MPI processes (`-np 2`), with 16 particles per process per timestep dump (`-c 16`). There are 2 dumps (`-d 2`) in total. Each dump goes through 5 timesteps (`-s 5`). Each particle is 48 bytes, consisting of an 8-byte particle ID and 40-byte particle data (`-b 40`).

DeltaFS will be mounted at the "particle" output directory of VPIC (`PRELOAD_Deltafs_mntp`), but will simply translate every particle write into an no-op (due to `PRELOAD_Bypass_shuffle` and `PRELOAD_Bypass_write`).

## Sample configuration for an all-to-all shuffle ONLY test with DeltaFS

```bash
export NUM_PARTICLE_PER_RANK=$((1024*1024))
export PARTICLE_SIZE=40

mpirun -np 2 \
    -env VPIC_current_working_dir `pwd` \
    -env PRELOAD_Ignore_dirs "fields:hydro:rundata:names" \
    -env PRELOAD_Deltafs_mntp particle \
    -env PRELOAD_Local_root `pwd` \
    -env PRELOAD_Log_home `pwd` \
    -env PRELOAD_Bypass_deltafs_namespace 1 \
    -env PRELOAD_Skip_papi 1 \
    -env PRELOAD_Skip_sampling 1 \
    -env PRELOAD_Enable_verbose_error 1 \
    -env PRELOAD_No_paranoid_checks 1 \
    -env PRELOAD_No_paranoid_barrier 1 \
    -env PRELOAD_No_paranoid_pre_barrier 1 \
    -env PRELOAD_Bypass_placement 1 \
    -env PRELOAD_Bypass_shuffle 0 \
    -env PRELOAD_Enable_bg_pause 1 \
    -env PRELOAD_Number_particles_per_rank "$NUM_PARTICLE_PER_RANK" \
    -env PRELOAD_Particle_id_size 8 \
    -env PRELOAD_Particle_size "$PARTICLE_SIZE" \
    -env SHUFFLE_Mercury_proto "bmi+tcp" \
    -env SHUFFLE_Buffer_per_queue 32768 \
    -env SHUFFLE_Num_outstanding_rpc 4 \
    -env SHUFFLE_Use_worker_thread 1 \
    -env SHUFFLE_Random_flush 1 \
    -env PRELOAD_Bypass_write 1 \
  `pwd`/../preload-runner -T 1 -d 2 -s 5 -c $NUM_PARTICLE_PER_RANK -b $PARTICLE_SIZE
```

The MPI options above are written in the `MPICH` format. For `OPENMPI`, change every `-env K V` to `-x K=V`.

This will launch 2 MPI processes (`-np 2`), with 1 million particles per process per timestep dump (`NUM_PARTICLE_PER_RANK`). There are 2 dumps (`-d 2`) in total. Each dump goes through 5 timesteps (`-s 5`). Each particle is 48 bytes, consisting of an 8-byte particle ID (`PRELOAD_Particle_id_size`) and 40-byte particle data (`PARTICLE_SIZE`).

The shuffle will use BMI and TCP (`SHUFFLE_Mercury_proto`). Each RPC message is buffered till 32K (`SHUFFLE_Buffer_per_queue`). The shuffle will send at most 4 concurrent messages without hearing from their replies (`SHUFFLE_Num_outstanding_rpc`).

Data indexing and writing is bypassed entirely (`PRELOAD_Bypass_write`).
