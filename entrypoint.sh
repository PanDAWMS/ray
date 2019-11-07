#!/bin/bash

function help() {
    echo "Usage: $0 PROGNAME [args]..."
    echo
    echo "  Runs PROGNAME with specified arguments after activating ray virtualenv and atlas setup"   
}

function activate_ray_env() {
    source ${RAYTHENA_CONDA_BIN}/activate $RAYTHENA_RAY_VENV
}

function activate_pilot_env() {
    source ${RAYTHENA_CONDA_BIN}/activate $RAYTHENA_PILOT_VENV
}

function activate_atlas_utils() {
    source /opt/lcg/binutils/*/x86_64-*/setup.sh
    source /opt/lcg/gcc/*/x86_64-*/setup.sh
}

export -f activate_ray_env
export -f activate_pilot_env
export -f activate_atlas_utils

if [ $# -eq 0 ]; then
    help
    exit 0
fi

activate_ray_env
activate_atlas_utils

echo "Running: $@"
"$@"
