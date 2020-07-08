#!/bin/bash

# settings for SLURM at your site
# at least 2 nodes are needed
# this script assumes that same virtual env used by harvester has been setup

#SBATCH -A ATLAS-HEP-group
#SBATCH --nodes=10
#SBATCH -p knlall
#SBATCH --cpus-per-task 128
##SBATCH -p bdwall
##SBATCH --cpus-per-task 36

##SBATCH -A condo
##SBATCH -p  hepd
##SBATCH --cpus-per-task 36
##SBATCH --nodes=2

#SBATCH --time 0:05:00
#SBATCH --tasks-per-node 1

nodes=$(scontrol show hostnames $SLURM_JOB_NODELIST) # Getting the node names
nodes_array=( $nodes )

node1=${nodes_array[0]}

ip_prefix=$(srun --nodes=1 --ntasks=1 -w $node1 hostname --ip-address) # Making address
suffix=':6379'
ip_head=$ip_prefix$suffix

export ip_prefix
export ip_head

echo 
echo "ip_prefix = "$ip_prefix
echo "ip_head = "$ip_head
echo

export OPENBLAS_NUM_THREADS=1
export RAYTHENA_RAY_REDIS_PASSWORD=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
export RAYTHENA_RAY_HEAD_IP=$ip_prefix
export RAYTHENA_RAY_REDIS_PORT=6379
export RAYTHENA_RAY_NWORKERS=8

# define the location of the Raythena code - fetched from git
export RAYTHENA_DIR=${HARVESTER_DIR}/ray

echo 
env | sort| grep RAYTHENA
echo
env | sort | grep SLURM
echo


export RAY_TMP_DIR=/tmp/raytmp/$SLURM_JOB_ID

if [[ ! -d $RAY_TMP_DIR ]]; then
  mkdir -p "$RAY_TMP_DIR"
fi

# start ray head node
srun -N1 -n1 -w "$node1" ${RAYTHENA_DIR}/bin/ray_start_head &

# wait for head node to start ray
${RAYTHENA_DIR}/bin/ray_sync

# start ray on all other nodes
srun -x $SLURMD_NODENAME -N`expr $SLURM_JOB_NUM_NODES - 1` -n`expr $SLURM_JOB_NUM_NODES - 1` ${RAYTHENA_DIR}/bin/ray_start_worker &

# wait for worker nodes to connect to the main node
${RAYTHENA_DIR}/bin/ray_sync --wait-workers --nworkers `expr $SLURM_JOB_NUM_NODES - 1`

# execute command to show ray works
${RAYTHENA_DIR}/example/standalone_ray_test_hello_world.py 

# exit code
RAY_EXIT_CODE=$?

# stop ray
ray stop

RAY_STOP_CODE=$?

echo "ray hello world exit code: ${RAY_EXIT_CODE}"
echo "ray exit code: ${RAY_STOP_CODE}"

# exit
if [ $RAY_EXIT_CODE -eq 0 ]
then
  echo "The script ran ok"
  exit 0
else
  echo "The script failed" >&2
  exit $RAY_EXIT_CODE
fi
