import psutil
import time
import json

from threading import Event

from raythena.utils.exception import ExThread


class CPUMonitor:

    def __init__(self, log_file, pid=None):
        self.process = psutil.Process(pid)
        self.log_file = log_file
        self.stop_event = Event()
        self.monitor_thread = ExThread(target=self.monitor_cpu, name="cpu_monitor")
        self.write_interval = 1 * 60
        self.time_step = 1

    def start(self):
        if not self.monitor_thread.is_alive():
            self.monitor_thread.start()

    def stop(self):
        if not self.stop_event.is_set():
            self.stop_event.set()
            self.monitor_thread.join()
            self.monitor_thread = ExThread(target=self.monitor_cpu, name="cpu_monitor")
            self.stop_event = Event()

    def _log_to_file(self, data):
        with open(self.log_file, 'w') as f:
            json.dump(data, f)

    def monitor_cpu(self):
        last_write = 0
        _ = psutil.cpu_percent()
        _ = self.process.cpu_percent()
        system_usage = list()
        process_usage = list()

        process_cpu_times = self.process.cpu_times()
        process_times = dict()
        for field in process_cpu_times._fields:
            if not field.startswith("children_"):
                process_times[field] = list()

        cpu_infos = {
            "system_usage": system_usage,
            "process_usage": process_usage,
            "process_times": process_times,
            "time_step": self.time_step
        }

        while not self.stop_event.is_set():
            time.sleep(self.time_step)
            system_usage.append(psutil.cpu_percent())
            process_usage.append(self.process.cpu_percent())
            process_cpu_times = self.process.cpu_times()

            for k in process_times.keys():
                process_times[k].append(getattr(process_cpu_times, k))

            if time.time() >= last_write + self.write_interval:
                self._log_to_file(cpu_infos)
                last_write = time.time()
        self._log_to_file(cpu_infos)


class Timing:

    def __init__(self):
        self._timings = dict()

    def add_timing(self, event):
        pass
