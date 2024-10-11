#!/usr/bin/env python3
import argparse
from datetime import datetime
import matplotlib.pyplot as plt
import re
from typing import List, Tuple, Dict

RE_PILOT_START = re.compile(r'Actor_([0-9]+):_start_payload | Pilot payload started with PID')
RE_PILOT_END = re.compile(r'Actor_([0-9]+) stopped')
RE_ATHINIT_END = re.compile(r'Sending [0-9]+ events to Actor_([0-9]+)')
RE_MERGE_START = re.compile(r'Starting merge transform for (HITS.*\.1)')
RE_MERGE_END = re.compile(r'Merge transform for file (HITS.*\.1) finished.')
RE_JOB_START = re.compile(r'Raythena v[\w\+\-\.]+ initializing')
RE_JOB_END = re.compile(r'All driver threads stopped. Quitting...')
RE_DATE_TIME = re.compile(r'([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})')
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

class LogData:
    def __init__(self):
        self.pilots: Dict[str, Tuple[datetime,datetime]] = {}
        self.athinit_end: Dict[str, datetime] = {}
        self.merges: Dict[str, Tuple[datetime,datetime]] = {}
        self.jobs: List[Tuple[datetime,datetime]] = []

    def add_pilot_start(self, pilot_id, start_time):
        self.pilots[pilot_id] = start_time
    
    def add_pilot_end(self, pilot_id, end_time):
        self.pilots[pilot_id] = (self.pilots[pilot_id], end_time)
    
    def add_athinit_end(self, pilot_id, end_time):
        if pilot_id not in self.athinit_end:
            self.athinit_end[pilot_id] = end_time

    def add_merge_start(self, filename, start_time):
        self.merges[filename] = start_time
    
    def add_merge_end(self, filename, end_time):
        self.merges[filename] = (self.merges[filename], end_time)

    def add_job_start(self, start_time):
        self.jobs.append(start_time)
    
    def add_job_end(self, end_time):
        self.jobs[-1] = (self.jobs[-1], end_time)

    def plot(self, title, outfile):
        
        fig, [ax, ax2] = plt.subplots(nrows=2, figsize=(8, 6), sharex=True, layout='constrained', gridspec_kw={'height_ratios': [3, 1]})

        job_start_time = self.jobs[0][0]
        job_end_time = self.jobs[0][1]
        job_total_runtime = (job_end_time - job_start_time).total_seconds() / 60
        y_delta = 10

        ax.set_xlim(0, (self.jobs[-1][1] - job_start_time).total_seconds() / 60)
        y_ticks = [i * y_delta + y_delta/2 for i in range(1, len(self.pilots)+1)]
        ax.set_yticks(y_ticks, labels=[f'Pilot {i}' for i in range(1, len(self.pilots)+1)])
        ax.set_ylim(0, y_ticks[-1] + y_delta)
        ax.text(0.98, 0.02, title, va='bottom', ha='right',
                fontstyle='italic', color=(0.5,)*3, size='x-small',
                transform=ax.transAxes,
                zorder=-100
            )
        node_utilization_ratio = []
        for y_tick, (pilot_id, pilot) in zip( y_ticks, self.pilots.items()):
            pilot_runtime = (pilot[1] - pilot[0]).total_seconds() / 60
            delta_start = (pilot[0] - job_start_time).total_seconds() / 60
            ath_init_time = (self.athinit_end[pilot_id] - pilot[0]).total_seconds() / 60
            idle_time = (job_end_time - pilot[1]).total_seconds() / 60
            node_utilization_ratio.append(((pilot[1] - pilot[0]).total_seconds() / 60) / job_total_runtime)
            ax.broken_barh([(0, delta_start), (delta_start, ath_init_time), (ath_init_time+delta_start, pilot_runtime), (delta_start+pilot_runtime, idle_time)], (y_tick - y_delta / 2, y_delta - 1), facecolors=('tab:brown', 'tab:red', 'tab:blue', 'tab:orange'))

        y_ticks = [i * y_delta + y_delta / 2 for i in range(1, len(self.merges)+1)]
        ax2.set_xlabel('Time since start [min]')
        ax2.set_yticks(y_ticks, labels=[f'Merge {i}' for i in range(1, len(self.merges)+1)], fontsize='small')
        ax2.set_ylim(0, y_ticks[-1]+y_delta)

        for y_tick, (_, merge) in zip(y_ticks, self.merges.items()):
            merge_runtime = (merge[1] - merge[0]).total_seconds() / 60
            delta_start = (merge[0] - job_start_time).total_seconds() / 60
            ax2.broken_barh([(delta_start, merge_runtime)], (y_tick - y_delta / 2, y_delta - 1), facecolors=('tab:green'))

        plt.title(f'Job Efficiency (avg: {sum(node_utilization_ratio)/len(node_utilization_ratio):.2f})', )
        plt.savefig(outfile)

    def __str__(self):
        return "Pilots: {}\nMerges: {}\nJobs: {}".format(self.pilots, self.merges, self.jobs)

    def __repr__(self):
        return self.__str__()

LOG_DATA = LogData()

def parse_datetime(line):
    match_date = RE_DATE_TIME.search(line)
    return datetime.strptime(match_date.group(1), DATE_FORMAT)

def process_pilot_start(pilot_id, line):
    datetime_object = parse_datetime(line)
    LOG_DATA.add_pilot_start(pilot_id, datetime_object)

def process_pilot_end(pilot_id, line):
    datetime_object = parse_datetime(line)
    LOG_DATA.add_pilot_end(pilot_id, datetime_object)

def process_athinit_end(pilot_id, line):
    datetime_object = parse_datetime(line)
    LOG_DATA.add_athinit_end(pilot_id, datetime_object)

def process_merge_start(filename, line):
    datetime_object = parse_datetime(line)
    LOG_DATA.add_merge_start(filename, datetime_object)

def process_merge_end(filename, line):
    datetime_object = parse_datetime(line)
    LOG_DATA.add_merge_end(filename, datetime_object)

def process_job_start(line):
    datetime_object = parse_datetime(line)
    LOG_DATA.add_job_start(datetime_object)

def process_job_end(line):
    datetime_object = parse_datetime(line)
    LOG_DATA.add_job_end(datetime_object)

def parse_logfile(logfile):
    with open(logfile, 'r') as f:
        lines = f.readlines()

    for line in lines:
        match = RE_PILOT_START.search(line)
        if match:
            process_pilot_start(match.group(1), line)
            continue
        match = RE_PILOT_END.search(line)
        if match:
            process_pilot_end(match.group(1), line)
            continue
        match = RE_ATHINIT_END.search(line)
        if match:
            process_athinit_end(match.group(1), line)
            continue
        match = RE_MERGE_START.search(line)
        if match:
            process_merge_start(match.group(1), line)
            continue
        match = RE_MERGE_END.search(line)
        if match:
            process_merge_end(match.group(1), line)
            continue
        match = RE_JOB_START.search(line)
        if match:
            process_job_start(line)
            continue
        match = RE_JOB_END.search(line)
        if match:
            process_job_end(line)
            continue
    return lines

def main():
    parser = argparse.ArgumentParser(description='Logfile to parse')
    parser.add_argument('logfile', type=str, help='The name of the logfile to process')
    parser.add_argument('outfile', type=str, help='The name of the plot file to save')
    
    args = parser.parse_args()

    parse_logfile(args.logfile)
    LOG_DATA.plot('100k ttbar events on Perlmutter, 1 AthenaMT process @ 128 cores/node', args.outfile)


if __name__ == '__main__':
    main()
