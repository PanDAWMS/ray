![](https://github.com/PanDAWMS/ray/workflows/tests/badge.svg)
# Ray
Raythena orchestrates Event Service Jumbo Jobs on HPCs based on the Ray framework. It communicates with Harvester to retrieve jobs from the central panda server and manages an AthenaMP payload on each node through pilot3.

# Installation With Conda

For an easier setup, Raythena should be installed in the same conda environment as Harvester. As Harvester typically runs on edge nodes and Raythena will run on compute node, this setup requires the python environment to be installed on a shared filesystem.
```bash
pip install git@github.com:PanDAWMS/ray.git
```


If necessary, Raythena can also be installed in a separate conda environment. This will require additional setup in the shell script submitted to the job scheduler to make sure that the correct environment is activated.
```bash
# create and activate an environment for Raythena
conda create -n raythena python=3.7
source activate raythena
```
 
## Stand-Alone Ray Example

Test ray compatibility with slurm via the stand-alone example for slurm:

```bash
salloc -N 3 -C haswell -q interactive -t 01:00:00
# starts ray processes on all nodes to setup a cluster
source example/setup_ray_cluster_slurm.sh
```

This will setup the Ray cluster with all requested nodes. Script needs to be sourced because it sets up some environment variables. Next step is to run a Ray script:

```bash
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

Instead of running an example on interactive nodes, it is also possible to run the example in batch mode with the [raythena-hello-world-test.sbatch](https://github.com/PanDAWMS/ray/blob/master/example/raythena-hello-world-test.sbatch) script. The script assumes that the virtual environment used by harvester and Raythena is already activated at the time of submitting the job and that the environment is propagated to the worker nodes.

# Raythena Configuration

Raythena requires a site-specific configuration. It has been tested and developed using Cori@Nersc. Raythena is configured using a yaml configuration file. An example for running Raythena on cori is provided in [conf/cori.yaml](https://github.com/PanDAWMS/ray/blob/master/conf/cori.yaml). Some configuration options can also be specified as command line arguments and environment variables (order of priority is clif > env vars > conf file). Run ```app.py --help``` for a list of options available from command line. Any of those options can be specified as an env var prefixed with ```RAYTHENA_``` (```--ray-workdir``` can be specified as the variable ```RAYTHENA_RAY_WORKDIR```)

## Setup with Harvester

Raythena is used in combination with Harvester which serves as a proxy to the Panda server. Install Harvester in the same Conda environemnt and update the configuration files accordingly. Example configuration files for NERSC's Cori are available here [NERSC_Cori_Raythena](https://github.com/PanDAWMS/harvester_configurations/tree/master/NERSC_Cori_Raythena).

Raythena will need to read panda_harvester.cfg configuration file from Harvester to communicate with it using the shared_file_messenger module. This is specified by the harvester.harvesterconf option.

Raythena also needs to know the Harvester access point. This is the directory in which the file-based communication with Harvester is processed. As this directory is only known after Harvester created a worker and submitted the job to the HPC scheduler, this should be specified to Raythena by setting ```RAYTHENA_HARVESTER_ENDPOINT```

As most HPC sites will not allow internet access from compute nodes, the harvester cacher module needs to be configured to write ```cric_ddmendpoints.json, cric_pandaqueues.json, <PANDA_QUEUE>_queuedata.json``` which will be staged by Raythena to pilot. Pilot will then read these files instead of trying to fetch them from the panda server.

## Pilot Installation

Raythena uses Pilot3 (version>=3.2.2.22) and PilotWrapper to run AthenaMP and maintain the yampl communication. Pilot source code (from git or cvmfs) and the pilot wrapper script should be availaible in the ```RAYTHENA_PAYLOAD_BINDIR``` directory. The provided example extracts the cvmfs pilot3 tarball to that directory which is the same as the raythena main workdir. For an example, see [NERSC_Cori_p2_ES_test_ray_haswell.sh](https://github.com/PanDAWMS/harvester_configurations/blob/master/NERSC_Cori_Raythena/etc/panda/NERSC_Cori_p2_ES_test_ray_haswell.sh). Pilot configuration file is given via the `HARVESTER_PILOT_CONFIG` variable. Example Pilot config file can be found here [default.cfg](https://github.com/PanDAWMS/harvester_configurations/blob/master/NERSC_Cori_Raythena/pilot/default.cfg). It is important to update the panda server url as each node will run its own local server to communicate with pilot on 127.0.0.1:8080.

To communicate with AthenaMP, pilot uses yampl. This library needs to be found by pilot. There are two options; either update ```LD_LIBRARY_PATH``` and ```PYTHONPATH``` in the job shell script to point to the library or use the payload.extrasetup config option in raythena configuration file. This option is an extra shell command that is executed before starting the pilot and can be used to setup the environment.
