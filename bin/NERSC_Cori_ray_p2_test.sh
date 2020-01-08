#!/bin/bash 
#SBATCH -q debug
#SBATCH --time 0:30:00
#DPB #SBATCH -q premium
#DPB #SBATCH --time 2:00:00
#DPB #SBATCH -q regular
#DPB #SBATCH --time 6:00:00
#DPB #SBATCH -q flex
#DPB #SBATCH --time-min=02:00:00
#DPB #SBATCH --time 4:00:00
#SBATCH --image=custom:atlas_athena_21.0.15_DBRelease-31.8.1:latest
#SBATCH --module=cvmfs
#SBATCH -A m2616
#SBATCH -L SCRATCH,project
#SBATCH -C haswell
#SBATCH --cpus-per-task 32
#DPB #SBATCH -C knl,quad,cache
#DPB #SBATCH --cpus-per-task 136
#SBATCH -N {nNode}

export HARVESTER_WORKER_ID={workerID}
export HARVESTER_ACCESS_POINT={accessPoint}
export HARVESTER_NNODE={nNode}

#for testing without harvester, needs evnt files present in the workdir
#export HARVESTER_ACCESS_POINT=/global/cscratch1/sd/esseivaj/raythena/workdir
#export HARVESTER_WORKDIR=$HARVESTER_ACCESS_POINT
#export HARVESTER_NNODE=$SLURM_NNODES

export HARVESTER_VENV=/global/project/projectdirs/m2616/harvester/bin

export PANDA_QUEUE=NERSC_Cori_p2_ES_Test
export container_setup=/usr/atlas/release_setup.sh
export HARVESTER_CONTAINER_RELEASE_SETUP_FILE=$container_setup
export pilot_wrapper_bin=/project/projectdirs/atlas/esseivaj/raythena/software/runpilot2-wrapper.sh
export pilot_tar_file=/project/projectdirs/atlas/esseivaj/raythena/software/pilot2.tar.gz

export SOURCEDIR=/project/projectdirs/atlas/esseivaj/raythena/software/raythena
export BINDIR=$SOURCEDIR/bin
export CONFDIR=$SOURCEDIR/conf

export RAYTHENA_HARVESTER_ENDPOINT=$HARVESTER_ACCESS_POINT
export RAYTHENA_RAY_WORKDIR=$HARVESTER_ACCESS_POINT
export RAYTHENA_PAYLOAD_BINDIR=$HARVESTER_ACCESS_POINT
export RAYTHENA_RAY_REDIS_PASSWORD=$(uuidgen -r)
export RAYTHENA_RAY_REDIS_PORT=6379
export RAYTHENA_CONFIG=$CONFDIR/cori.yaml
export RAYTHENA_DEBUG=1
export RAYTHENA_RAY_HEAD_IP=$(hostname -i)
export RAYTHENA_PANDA_QUEUE=$PANDA_QUEUE
export NWORKERS=$(($HARVESTER_NNODE - 1))
export RAYTHENA_CORE_PER_NODE=$SLURM_CPUS_PER_TASK

cp $pilot_wrapper_bin $RAYTHENA_RAY_WORKDIR
tar xzf $pilot_tar_file -C$RAYTHENA_RAY_WORKDIR


# setup ray
source $HARVESTER_VENV/activate
srun -N1 -n1 -w $SLURMD_NODENAME $BINDIR/ray_start_head &

$BINDIR/ray_sync

srun -x $SLURMD_NODENAME -N$NWORKERS -n$NWORKERS $BINDIR/ray_start_worker &

$BINDIR/ray_sync --wait-workers --nworkers $NWORKERS

python $SOURCEDIR/app.py

ray stop

wait
