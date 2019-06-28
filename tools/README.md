# Typical configuration for an all-to-all shuffle test

```bash
export NUM_PARTICLE_PER_RANK=$((1024*1024))

mpirun -np 2 \
-env VPIC_current_working_dir `pwd` \
-env PRELOAD_Ignore_dirs "fields:hydro:rundata:names" \
-env PRELOAD_Deltafs_mntp particle \
-env PRELOAD_Local_root `pwd` \
-env PRELOAD_Log_home `pwd` \
-env PRELOAD_Bypass_deltafs_namespace 1 \
-env PRELOAD_Skip_papi=1 \
-env PRELOAD_Skip_sampling 1 \
-env PRELOAD_Enable_verbose_error 1 \
-env PRELOAD_No_paranoid_checks 1 \
-env PRELOAD_No_paranoid_barrier 1 \
-env PRELOAD_No_paranoid_pre_barrier 1 \
-env PRELOAD_Bypass_placement=1 \
-env PRELOAD_Bypass_shuffle=0 \
-env PRELOAD_Enable_bg_pause 1 \
-env PRELOAD_Number_particles_per_rank="$NUM_PARTICLE_PER_RANK" \
-env PRELOAD_Particle_id_size=8 \
-env PRELOAD_Particle_size=40 \
-env SHUFFLE_Mercury_proto="bmi+tcp" \
-env SHUFFLE_Buffer_per_queue=32768 \
-env SHUFFLE_Num_outstanding_rpc=4 \
-env SHUFFLE_Use_worker_thread=1 \
-env SHUFFLE_Random_flush=1 \
-env PRELOAD_Bypass_write=1 \
`pwd`/../preload-runner -T 1 -d 2 -c $NUM_PARTICLE_PER_RANK
```
