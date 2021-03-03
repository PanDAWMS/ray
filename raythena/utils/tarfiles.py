import os
import time
from typing import List, Dict, Union, Any
import concurrent.futures 
import tarfile
import shutil
import zlib
    
from raythena.actors.loggingActor import LoggingActor
from raythena.utils.config import Config

class TarFiles(object):
    """
    Handles creating the tar files of event service event range files
    """

    def __init__(self, logging_actor: LoggingActor, config: Config) -> None:
        self.logging_actor = logging_actor
        self.config = config
        self.start_time = time.time()
        self.logging_actor.debug.remote("TarFiles", f" start_time {self.start_time}", time.asctime())

        self.tarmaxprocesses = self.config.ray['tarmaxprocesses']
        self.tar_timestamp = time.time()
        self.tar_merge_es_output_dir = os.path.join(self.workdir,"merge_es_output")
        self.tar_merge_es_files_dir = os.path.join(self.workdir,"merge_es_files")
        self.ranges_to_tar: List[List[Dict]] = list()
        
        self.tar_executor = concurrent.futures.ProcessPoolExecutor(max_workers=self.tarmaxprocesses)
        self.running_tar_procs = list()

        # create the output directories if needed
        try:
            if not os.path.isdir(self.tar_merge_es_output_dir):
                self.logging_actor.debug.remote(self.id,
                                                f"Creating dir for es files merged into zip files {self.tar_merge_es_output_dir}",
                                                time.asctime())
                os.mkdir(self.tar_merge_es_output_dir)
        except Exception:
            self.logging_actor.warn.remote(
                self.id,
                "Exception when creating the tar_merge_es_output_dir",
                time.asctime()
            )
            raise
        try:
            if not os.path.isdir(self.tar_merge_es_files_dir):
                self.logging_actor.debug.remote(self.id,
                                                f"Creating dir for zipped es files {self.tar_merge_es_files_dir}",
                                                time.asctime())
                os.mkdir(self.tar_merge_es_files_dir)
        except Exception:
            self.logging_actor.warn.remote(
                self.id,
                "Exception when creating the tar_merge_es_files_dir",
                time.asctime()
            )
            raise
        
   def check_for_running_tar_proc(self) -> int:
        """
        Checks the self.running_tar_procs list for the Future objects. if process still running let it run otherwise
        get the results of running and pass information to the BookKeeper and onto Harvester
            
        Args:
            None
            
        Returns:
            number of running Tar subprocesses. 

        self.tar_executor = ProcessPoolExecutor(max_workers=self.tarmaxprocesses)
        self.running_tar_procs = list()
        """
        if len(self.running_tar_procs) > 0:
            completed_failed_futures = []
            try:
                for future in concurrent.futures.as_completed(self.running_tar_procs, 60):
                    completed_failed_futures.append(future)
                    try:
                        result = future.result()
                        self.logging_actor.debug.remote(self.id, f"Tar subprocess result {repr(result)}", time.asctime())
                    except Exception as ex:
                        # do something
                        self.logging_actor.info.remote(self.id, f"Tar subprocess Caught exception {ex}", time.asctime())
                        pass
                # clear out finished or failed tar processes
                while completed_failed_futures:
                    future = completed_failed_futures.pop()
                    self.running_tar_procs.remove(future)
            except concurrent.futures.TimeoutError:
                # did not get information within timeout try later
                self.logging_actor.debug.remote(self.id, "Warning - did not get tar process completed tasks within 60 seconds", time.asctime())
                pass
        return len(self.running_tar_procs)
    
    def create_tar_file(self, range_list: list) -> Dict[str, List[Dict]]:
        """
        Use input range_list to create tar file and return list of tarred up event ranges and information needed by Harvester

        Args:
            range_list   list of event range dictionaries 
        Returns:
            Dictionary - key PanDAid, item list of event ranges with information needed by Harvester
        """
        return_val = dict()
        # read first element in list to build temporary filename
        file_base_name = "panda." + os.path.basename(range_list[0]['path']) + ".zip"
        temp_file_base_name = file_base_name + ".tmpfile"
        temp_file_path = os.path.join(self.tar_merge_es_output_dir, temp_file_base_name)
        file_path = os.path.join(self.tar_merge_es_output_dir, file_base_name)
        
        #self.tar_merge_es_output_dir zip files
        #self.tar_merge_es_files_dir  es files

        PanDA_id = range_list[0]['PanDAID']

        file_fsize = 0
        try: 
            # create tar file looping over event ranges
            with tarfile.open(temp_file_path, "w") as tar:
                for event_range in range_list:
                    tar.add(event_range['path'])
            file_fsize = os.path.getsize(temp_file_path)
            # calculate alder32 checksum
            file_chksum = self.calc_adler32(temp_file_path)
            return_val = self.create_harvester_data(PanDA_id, file_path, file_chksum, file_fsize, range_list)
            for event_range in range_list:
                lfn = os.path.basename(event_range["path"])
                pfn = os.path.join(self.tar_merge_es_files_dir, lfn)
                shutil.move(event_range["path"],pfn)
            # rename zip file (move)
            shutil.move(tmp_file_path,file_path)
            return return_val
            # move input files to new location and create output dictionary
        except Exception as exc:
            raise
            return return_val

    def calc_adler32(self, file_name: str) -> str:
        """
        Calculate adler32 checksum for file
        
        Args:
           file_name - name of file to calculate checksum
        Return:
           string with Adler 32 checksum
        """
        val = 1
        blockSize = 32 * 1024 * 1024
        with open(file_name, 'rb') as fp:
            while True:
                data = fp.read(blockSize)
                if not data:
                    break
                val = zlib.adler32(data, val)
        if val < 0:
            val += 2 ** 32
        return hex(val)[2:10].zfill(8).lower()


    def create_harvester_data(self, PanDA_id: str, file_path: str, file_chksum: str, file_fsize: int, range_list: list) -> Dict[str, List[Dict]]:
        """
        create data structure for telling Harvester what files are merged and ready to process

        Args:
             PanDA_id - Panda ID (as string)
             file_path - path on disk to the merged es output "zip" file
             file_chksum - adler32 checksum of file
             file_fsize - file size in bytes
             range_list - list of event ranges in the file
        Return:
             Dictionary with PanDA_id is the key and list of Event range elements
        """
        return_list = list()
        for event_range in range_list:
            return_list.append(
                {
                    "eventRangeID": event_range['eventRangeID'],
                    "eventStatus": "finished",
                    "path": file_path,
                    "type": "zip_output",
                    "chksum": file_chksum,
                    "fsize": file_fsize
                })
        return_val = { PanDA_id: return_list }
        return return_val
    
# [{'eventRangeID': '24027003-4985019832-23509382954-429-1', 'eventStatus': 'finished', 'path': '/lcrc/group/ATLAS/harvester/var/lib/workdir/panda/testing/raythena/testdir/4985019832/Actor_0/esOutput/HITS.24027003._001647.pool.root.1.24027003-4985019832-23509382954-429-1', 'chksum': '7fb76e57', 'fsize': 191698, 'type': 'es_output', 'PanDAID': '4985019832'}, {'eventRangeID': '24027003-4985019832-23509382954-419-1', 'eventStatus': 'finished', 'path': '/lcrc/group/ATLAS/harvester/var/lib/workdir/panda/testing/raythena/testdir/4985019832/Actor_0/esOutput/HITS.24027003._001647.pool.root.1.24027003-4985019832-23509382954-419-1', 'chksum': '91324077', 'fsize': 317505, 'type': 'es_output', 'PanDAID': '4985019832'}]
 
    def tar_es_output(self, ranges_to_tar:List[List[Dict]]) -> None:
        """
        Get from bookKeeper the event ranges arraigned by input file than need to put into output tar files

        Returns:
            None
        """
        # get number of running tar processes
        num_running_tar_procs = self.check_for_running_tar_proc()
        self.logging_actor.debug.remote(self.id, f"Enter tar_es_output - number of running tar procs {num_running_tar_procs}", time.asctime())
        # add new ranges to tar to the list
        self.ranges_to_tar.extend(ranges_to_tar)

        maxtarprocs = self.tarmaxprocesses - num_running_tar_procs
        log_message = f"Launch tar subprocesses : num possible processes - {maxtarprocs} number of tar files to make - {len(self.ranges_to_tar)}" 
        self.logging_actor.debug.remote(self.id, log_message, time.asctime())
        while ranges_to_tar and maxtarprocs > 0:
            try:
                range_list = self.ranges_to_tar.pop()
                self.running_tar_procs.append(self.tar_executor.submit(create_tar_file,range_list))
                maxtarprocs = maxtarprocs - 1
            except Exception as exc :
                self.logging_actor.warn.remote(self.id, f"Exception {exc} when submitting tar subprocess",time.asctime())
                self.ranges_to_tar.append(range_list)
                pass
            
        #self.tarmaxfilesize = self.config.ray['tarmaxfilesize']
        #self.tarmaxprocesses = self.config.ray['tarmaxprocesses']
