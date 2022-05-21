import psutil
import time
import json

from threading import Event
from typing import Any, Dict, List, Union

from raythena.utils.exception import ExThread


class CPUMonitor:
    """
    Monitoring tools recording system cpu utilization as well as process cpu utilization to a file
    """
    def __init__(self, log_file: str, pid: Any = None) -> None:
        self.process = psutil.Process(pid)
        self.log_file = log_file
        self.stop_event = Event()
        self.monitor_thread = ExThread(target=self.monitor_cpu, name="cpu_monitor")
        self.write_interval = 10 * 60
        self.time_step = 1

    def start(self) -> None:
        """
        Start the cpu monitoring thread

        Returns:
            None
        """
        if not self.monitor_thread.is_alive():
            self.monitor_thread.start()

    def stop(self) -> None:
        """
        Stop the monitoring thread and reset state to allow to restart monitoring

        Returns:
            None
        """
        if not self.stop_event.is_set():
            self.stop_event.set()
            self.monitor_thread.join()
            self.monitor_thread = ExThread(target=self.monitor_cpu, name="cpu_monitor")
            self.stop_event = Event()

    def _log_to_file(self, data: Dict[str, Union[Dict[str, List], List, int]]) -> None:
        """
        Write data to log file

        Args:
            data: dict holding cpu time and utilization

        Returns:
            None
        """
        with open(self.log_file, 'w') as f:
            json.dump(data, f)

    def monitor_cpu(self) -> None:
        """
        Records cpu utilization, cpu times for the monitored process and system-wide cpu utilization every second.
        Cpu utilization data are saved to a dict using the following structure:

        {
            "system_usage": [...],
            "process_usage": [...],
            "process_times": {
                "user": [..],
                "system": [..],
                "iowait": [..] # Linux only
                },
            "time_step": 1
        }

        The dict is dumped to the logfile every minute and when the thread is stopped

        Returns:
            None
        """
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
