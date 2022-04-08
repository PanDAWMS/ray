#!/bin/bash

# settings for SLURM at your site
# at least 2 nodes are needed
# this script assumes that same virtual env used by harvester has been setup

#SBATCH -A ATLAS-HEP-group

##SBATCH --nodes=200
##SBATCH -p knlall
##SBATCH --cpus-per-task 128

#SBATCH --nodes=7
#SBATCH -p bdws
#SBATCH --cpus-per-task 36

##SBATCH -A condo
##SBATCH -p  hepd
##SBATCH --cpus-per-task 36
##SBATCH --nodes=2

#SBATCH --time 0:03:00
#SBATCH --tasks-per-node 1

nodes=$(scontrol show hostnames $SLURM_JOB_NODELIST) # Getting the node names
nodes_array=( $nodes )

echo $nodes_arrray

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


#export OPENBLAS_NUM_THREADS=1
export REDIS_PASSWORD=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
export REDIS_IP=$ip_prefix
export REDIS_PORT=6379


# environmental variables needed for ray_sync python script
export RAYTHENA_RAY_REDIS_PASSWORD=$REDIS_PASSWORD
export RAYTHENA_RAY_HEAD_IP=$REDIS_IP
export RAYTHENA_RAY_REDIS_PORT=$REDIS_PORT

# define the location of the Raythena code - fetched from git
# export RAYTHENA_DIR=${HARVESTER_DIR}/ray

echo 
env | sort
echo 
env | sort| grep REDIS
echo
env | sort| grep RAYTHENA
echo
env | sort | grep SLURM
echo


# start ray head node
srun -N1 -n1 -w "$node1" ray start --block --head --port="$REDIS_PORT" --redis-password="$REDIS_PASSWORD" --num-cpus 1 &

# wait for head node to start ray
sleep 15
#${RAYTHENA_DIR}/bin/ray_sync

# start ray on all other nodes
srun -x $SLURMD_NODENAME -N`expr $SLURM_JOB_NUM_NODES - 1` -n`expr $SLURM_JOB_NUM_NODES - 1` ray start --address="$REDIS_IP":"$REDIS_PORT" --redis-password="$REDIS_PASSWORD" --block --num-cpus 1 &


# wait for worker nodes to connect to the main node
sleep 30
#${RAYTHENA_DIR}/bin/ray_sync --wait-workers --nworkers `expr $SLURM_JOB_NUM_NODES - 1`

# execute command to show ray works
standalone_ray_test_hello_world.py --redis-ip $REDIS_IP --redis-port $REDIS_PORT --redis-password $REDIS_PASSWORD 
#${RAYTHENA_DIR}/example/standalone_ray_v1_ping_pong_simple.py

# exit code
RAY_EXIT_CODE=$?

# stop ray on the cluster nodes
srun -x $SLURMD_NODENAME -N`expr $SLURM_JOB_NUM_NODES - 1` -n`expr $SLURM_JOB_NUM_NODES - 1` ray stop --verbose
RAY_STOP_Cluster_Nodes=$?

# stop ray on this node
ray stop --verbose

RAY_STOP_CODE=$?

# Stop ray Redis node
srun -N1 -n1 -w "$node1" ray stop --verbose
RAY_STOP_Redis_Node=$?


echo "ray hello world exit code: ${RAY_EXIT_CODE}"
echo "ray exit code: ${RAY_STOP_CODE}"
echo "ray stop Redis node exit code: ${RAY_STOP_Redis_Node}"
echo "ray stop cluster nodes exit code: ${RAY_STOP_Cluster_Nodes}"

# exit
if [ $RAY_EXIT_CODE -eq 0 ]
then
  echo "The script ran ok"
  exit 0
else
  echo "The script failed" >&2
  exit $RAY_EXIT_CODE
fi
