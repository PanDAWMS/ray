#!/bin/bash

export OPENBLAS_NUM_THREADS=1
export RAYTHENA_RAY_REDIS_PASSWORD=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
export RAYTHENA_RAY_HEAD_IP=`hostname -i`
export RAYTHENA_RAY_REDIS_PORT=6379
export RAYTHENA_RAY_NWORKERS=8

# start ray head node
srun -N1 -n1 -w $SLURMD_NODENAME \
    ray start --head --port=$RAYTHENA_RAY_REDIS_PORT --redis-password=$RAYTHENA_RAY_REDIS_PASSWORD --num-cpus=$RAYTHENA_RAY_NWORKERS --block &

# wait for head node to start ray
ray_sync

# start ray on all other nodes
srun -x $SLURMD_NODENAME -N`expr $SLURM_JOB_NUM_NODES - 1` -n`expr $SLURM_JOB_NUM_NODES - 1` \
    ray start --address $RAYTHENA_RAY_HEAD_IP:$RAYTHENA_RAY_REDIS_PORT --redis-password $RAYTHENA_RAY_REDIS_PASSWORD --num-cpus=$RAYTHENA_RAY_NWORKERS --block &

# wait for worker nodes to connect to the main node
ray_sync --wait-workers --nworkers `expr $SLURM_JOB_NUM_NODES - 1`
