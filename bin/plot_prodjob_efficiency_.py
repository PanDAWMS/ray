#!/usr/bin/env python3
import argparse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import matplotlib.pyplot as plt
import re
from typing import List, Tuple, Dict

RE_PILOT_START = re.compile(r'^ *([0-9]+):.*PanDA Pilot version.*')
RE_PILOT_END = re.compile(r'^ *([0-9]+):.*make_job_report.*PanDA job id: [0-9]+')
RE_JOB_START = re.compile(r'Start_SLURM_Job')
RE_JOB_END = re.compile(r'JOB [0-9]+ ON nid[0-9]+ CANCELLED AT .* DUE TO TIME LIMIT')
RE_DATE_TIME = re.compile(r'([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})')
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

class LogData:
    def __init__(self):
        self.pilots: Dict[str, List[Tuple[datetime,datetime]]] = {}
        self.jobs: List[Tuple[datetime,datetime]] = []

    def add_pilot_start(self, pilot_id, start_time):
        pilots = self.pilots.setdefault(pilot_id, [])
        pilots.append((start_time, None))
    
    def add_pilot_end(self, pilot_id, end_time):
        self.pilots[pilot_id][-1] = (self.pilots[pilot_id][-1][0], end_time)

    def add_job_start(self, start_time):
        self.jobs.append((start_time, None))
    
    def add_job_end(self, end_time):
        self.jobs[-1] = (self.jobs[-1][0], end_time)

    def plot(self, title, outfile):
        
        fig, ax = plt.subplots(figsize=(8, 6), layout='constrained')

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
        for (pilot_id, pilots) in self.pilots.items():
            last_pilot = pilots[-1]
            if last_pilot[1] is None:
                last_pilot = (last_pilot[0], job_end_time)
                idle_time = (job_end_time - last_pilot[0]).total_seconds() / 60
            else:
                idle_time = (job_end_time - last_pilot[1]).total_seconds() / 60

            delta_start = (pilots[0][0] - job_start_time).total_seconds() / 60

            pilots_timeline = [(0, delta_start)]
            offset = delta_start
            pilot_total_runtime = 0
            for pilot in pilots:
                if pilot[1] is None:
                    continue
                pilot_runtime = (pilot[1] - pilot[0]).total_seconds() / 60
                pilot_total_runtime += pilot_runtime
                pilots_timeline.append((offset, pilot_runtime))
                offset += pilot_runtime
            
            node_utilization_ratio.append(pilot_total_runtime / job_total_runtime)
            ax.broken_barh(pilots_timeline, (y_ticks[int(pilot_id)-1] - y_delta / 2, y_delta - 1), facecolors=('tab:brown', 'tab:red', 'tab:blue', 'tab:orange'))

        ax.set_xlabel('Time since start [min]')

        plt.title(f'Job Efficiency (avg: {sum(node_utilization_ratio)/len(node_utilization_ratio):.2f})', )
        plt.savefig(outfile)

    def __str__(self):
        return "Pilots: {}\nMerges: {}\nJobs: {}".format(self.pilots, self.merges, self.jobs)

    def __repr__(self):
        return self.__str__()

LOG_DATA = LogData()

def parse_datetime(line, regex=RE_DATE_TIME, format=DATE_FORMAT):
    match_date = regex.search(line)
    return datetime.strptime(match_date.group(1), format)

def process_pilot_start(pilot_id, line):
    datetime_object = parse_datetime(line)
    datetime_object = datetime_object.replace(tzinfo=ZoneInfo('UTC'))
    LOG_DATA.add_pilot_start(pilot_id, datetime_object)

def process_pilot_end(pilot_id, line):
    datetime_object = parse_datetime(line)
    datetime_object = datetime_object.replace(tzinfo=ZoneInfo('UTC'))
    LOG_DATA.add_pilot_end(pilot_id, datetime_object)

def process_job_start(line):
    datetime_object = parse_datetime(line, re.compile(r'\[([0-9]{2}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})\]'), '%d-%m-%y %H:%M:%S')
    datetime_object = datetime_object.replace(tzinfo=ZoneInfo('America/Los_Angeles'))
    LOG_DATA.add_job_start(datetime_object)

def process_job_end(line):
    datetime_object = parse_datetime(line, re.compile(r'([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})'), '%Y-%m-%dT%H:%M:%S')
    datetime_object = datetime_object.replace(tzinfo=ZoneInfo('UTC'))
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
    LOG_DATA.plot('Perlmutter production job, 20 pilots', args.outfile)


if __name__ == '__main__':
    main()
