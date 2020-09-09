![](https://github.com/PanDAWMS/ray/workflows/tests/badge.svg)
# Ray
Raythena orchestrates Event Service Jumbo Jobs on HPCs based on the Ray framework and utilizes Harvester and Pilot2.

# Installation With Conda

Create a new Conda Python3.7 environemnt
```
conda create -n raythena python=3.7
source activate raythena
```
Clone and intall this package
```
git clone git@github.com:PanDAWMS/ray.git
pip install -e ray
```

## Stand-Alone Ray Example

Test ray compatibility with slurm via the stand-alone example for slurm:

```
salloc -N 3 -C haswell -q interactive -t 01:00:00
source setup_ray_cluster_slurm.sh
```

This will setup the Ray cluster with all requested nodes. Script needs to be sourced because it sets up some environment variables. Next step is to run a Ray script:

```
standalone_ray_test_hello_world.py
```

Script will look for all nodes in the Ray cluster and start a ping-pong test. The output is expected to look like this:
```
Found 3 nodes in the Ray Cluster:
	['payload_slot@10.128.0.28', 'payload_slot@10.128.0.30', 'payload_slot@10.128.0.29']
Setting up 3 Actors...
(pid=3722, ip=10.128.0.30) Initial message from Actor_10.128.0.30
(pid=25088, ip=10.128.0.29) Initial message from Actor_10.128.0.29
(pid=62534) Initial message from Actor_10.128.0.28
(pid=62534) ping
(pid=3722, ip=10.128.0.30) ping
(pid=25088, ip=10.128.0.29) ping
Received back message pong
Received back message pong
Received back message pong
```

This test verifies that all nodes were connected to to the Ray Cluster and that communication between the nodes is successful.

## Ray Batch Example

Instead of running an example on interactive nodes, it is also possible to run the example in batch mode with the [raythena-hello-world-test.sbatch](https://github.com/PanDAWMS/ray/blob/master/example/raythena-hello-world-test.sbatch) script. The script assumes that the virtual environment used by harvester has been setup and that the `$HARVESTER_DIR` variable exists.

## Raythena Configuration

Raythena requires a site-specific configuration. If using in combination with Harvester, this specifies the location of Harvester configuration and configures the panda queue. Further, it specifies a virtual environment in which Pilot is launched. Example for NERSC's Cori [cori.yaml](https://github.com/PanDAWMS/ray/blob/master/conf/cori.yaml).

# Usage with Harvester

Raythena can be used in combination with Harvester which serves as a connection to the Panda server. Install Harvester in the same Conda environemnt and update the configuration files accordingly. Example configuration files for NERSC's Cori are available here [NERSC_Cori_Raythena](https://github.com/PanDAWMS/harvester_configurations/tree/master/NERSC_Cori_Raythena).

# Pilot Installation

Raythena uses Pilot to run AthenaMP and maintain the yampl communication. Pilot is given in a tarball and is usually extracted in the Raythena workdir. For an example see [NERSC_Cori_p2_ES_test_ray_haswell.sh](https://github.com/PanDAWMS/harvester_configurations/blob/master/NERSC_Cori_Raythena/etc/panda/NERSC_Cori_p2_ES_test_ray_haswell.sh). Pilot configuration file is given via the `HARVESTER_PILOT_CONFIG` variable. Example Pilot config file can be found here [default.cfg](https://github.com/PanDAWMS/harvester_configurations/blob/master/NERSC_Cori_Raythena/pilot/default.cfg).
