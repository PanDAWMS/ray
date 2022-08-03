#!/bin/bash 
#JE SBATCH -q debug
#JE SBATCH --time 30:00
#JE #SBATCH -q premium
#JE #SBATCH --time 2:00:00
#SBATCH -q regular
#SBATCH --time 2:00:00
#JE #SBATCH -q flex
#JE #SBATCH --time-min=02:00:00
#JE #SBATCH --time 4:00:00
#SBATCH --image=docker:atlas/athena:21.0.15_31.8.1
#SBATCH --module=cvmfs
#SBATCH -A m2616
#SBATCH -L SCRATCH
#JE SBATCH -C haswell
#JE SBATCH --cpus-per-task 32
#SBATCH -C knl,quad,cache
#SBATCH --cpus-per-task 136
#SBATCH -N {nNode}

# Brackets are substituted by harvester at runtime
export HARVESTER_WORKER_ID={workerID}
export HARVESTER_ACCESS_POINT={accessPoint}
export HARVESTER_NNODE={nNode}

# for testing without harvester, needs evnt files present in the workdir
#export HARVESTER_ACCESS_POINT=/global/cscratch1/sd/tsulaia/tmp-raythena
#export HARVESTER_WORKDIR=$HARVESTER_ACCESS_POINT
#export HARVESTER_NNODE=$SLURM_NNODES

export PANDA_QUEUE=NERSC_Cori_p2_ES_Test

# variables read by pilot3
export HARVESTER_CONTAINER_RELEASE_SETUP_FILE=/release_setup.sh
export HARVESTER_PILOT_CONFIG=/global/common/software/atlas/raythena-pilot/default.cfg
export PILOT_LOGFILE=log.pilotwrapper

export CONFDIR=/global/common/software/atlas/raythena-pilot

export RAYTHENA_HARVESTER_ENDPOINT=$HARVESTER_ACCESS_POINT
export RAYTHENA_RAY_WORKDIR=$HARVESTER_ACCESS_POINT
export RAYTHENA_RAY_REDIS_PASSWORD=$(uuidgen -r)
export RAYTHENA_RAY_REDIS_PORT=6379
export RAYTHENA_RAY_EVENTSPERFILE=500
export RAYTHENA_RAY_HITSPERFILE=100
export RAYTHENA_CONFIG=$CONFDIR/cori.yaml
export RAYTHENA_DEBUG=1
export RAYTHENA_RAY_HEAD_IP=$(hostname -i)
export RAYTHENA_PANDA_QUEUE=$PANDA_QUEUE
export NWORKERS=$((HARVESTER_NNODE - 1))
export RAYTHENA_CORE_PER_NODE=128

export ATHENA_PROC_NUMBER_JOB=$RAYTHENA_CORE_PER_NODE
export ATHENA_PROC_NUMBER=$RAYTHENA_CORE_PER_NODE
export ATHENA_CORE_NUMBER=$RAYTHENA_CORE_PER_NODE
echo "Running $RAYTHENA_CORE_PER_NODE workers per Athena"

#export RAY_BACKEND_LOG_LEVEL=debug

# Create a file with current timestamp and job time limit
export TIME_MONITOR_FILE=jobtimeout.txt
date "+%H:%M:%S" > $RAYTHENA_RAY_WORKDIR/$TIME_MONITOR_FILE
squeue -h -j $SLURM_JOBID -o "%l" >> $RAYTHENA_RAY_WORKDIR/$TIME_MONITOR_FILE

export RAY_TMP_DIR=/tmp/ray/$SLURM_JOB_ID

if [[ ! -d $RAY_TMP_DIR ]]; then
  mkdir -p "$RAY_TMP_DIR"
fi

srun -N1 -n1 -w "$SLURMD_NODENAME" ray_start_head > $RAYTHENA_RAY_WORKDIR/headnode.log 2> $RAYTHENA_RAY_WORKDIR/headnode.err &
pid=$!
retsync=1
try=1
while [[ $retsync -ne 0 ]]; do
  ray_sync
  retsync=$?
  kill -0 "$pid"
  status=$?
  if [[ $retsync -ne 0 ]] && [[ $status -ne 0 ]]; then
    try=$((try+1))
    if [[ $try -gt 5 ]]; then
      exit 1
    fi
    echo restarting head node init
    srun -N1 -n1 -w "$SLURMD_NODENAME" ray_start_head > $RAYTHENA_RAY_WORKDIR/headnode.log 2> $RAYTHENA_RAY_WORKDIR/headnode.err &
    pid=$!
  fi
done

srun -x "$SLURMD_NODENAME" -N$NWORKERS -n$NWORKERS ray_start_worker &
pid=$!
retsync=1
try=1
while [[ $retsync -ne 0 ]]; do
  ray_sync --wait-workers --nworkers $NWORKERS
  retsync=$?
  kill -0 "$pid"
  status=$?
  if [[ $retsync -ne 0 ]] && [[ $status -ne 0 ]]; then
    try=$((try+1))
    if [[ $try -gt 5 ]]; then
      exit 1
    fi
    echo restarting workers setup
    srun -x "$SLURMD_NODENAME" -N$NWORKERS -n$NWORKERS ray_start_worker &
    pid=$!
  fi
done

raythena > $RAYTHENA_RAY_WORKDIR/raythena.log 2> $RAYTHENA_RAY_WORKDIR/raythena.err

ray stop

wait
