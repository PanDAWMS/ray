#!/usr/bin/env python
from __future__ import print_function

import argparse
import json
import os.path as path

import PyUtils.AthFile as af

def validate_job(job_dir, job_state_file):
    with open(job_state_file, 'r') as f:
        job_state = json.load(f)
    merged_input_files = job_state["merged"]
    merged_output_files = set([list(x.keys())[0] for x in merged_input_files.values()])
    event_numbers = set()
    for output_file in merged_output_files:
        output_file_abs = path.join(job_dir, output_file)
        if not path.isfile(output_file_abs):
            print("Expected file " + output_file_abs + " to be present in the job directory")
            exit(1)
        info=af.server._peeker(str(output_file_abs), -1)

        current_event_numbers = set(info["evt_number"])
        if len(info["evt_number"]) != len(current_event_numbers):
            print("Duplicate events in file " + output_file + "(" + str(len(current_event_numbers)) + ", " + str(len(info["evt_number"])) + "): " + str(info["evt_number"]))
            exit(1)
        print(str(len(current_event_numbers)) + " events in file " + output_file)
        if not current_event_numbers.isdisjoint(event_numbers):
            print("Found duplicate events in file " + output_file + ": "+ str(current_event_numbers & event_numbers))
            exit(1)
        event_numbers |= current_event_numbers
    print("No duplicate found. # events merged: " + str(len(event_numbers)) + ", # of files: " + str(len(merged_output_files)))
    


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("jobdir")
    args = parser.parse_args()
    job_dir = args.jobdir
    job_state_file = path.join(job_dir, "state.json")
    if not path.isfile(job_state_file):
        print("state file not found in the job directory")
        exit(1)
    validate_job(job_dir, job_state_file)

if __name__ == "__main__":
    main()