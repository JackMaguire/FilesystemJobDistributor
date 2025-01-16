from filesystemjd import signals
from filesystemjd.signals import create_fileprefix, create_wildcard_for_direction, create_wildcard_for_task, parse_filename
from filesystemjd.util import create_file, attempt_to_create_a_directory

from pathlib import Path
import os
import time
import glob

class Head:
    def __init__( self, signal_dir: str, greeting_message: str = None ):
        self.path = Path( signal_dir )
        self.greeting_message = greeting_message
        
        if not self.path.exists():
            attempt_to_create_a_directory( self.path )

        if not self.path.is_dir():
            raise Exception( "path is not recognaized as a dir: {}".format( str(self.path) ) )

        self.working_nodes = set()
        self.available_nodes = set()


    def total_n_workers( self ):
        return len(self.working_nodes) + len(self.available_nodes)

    def n_available_workers( self ):
        return len(self.available_nodes)

    def n_busy_workers( self ):
        return len(self.working_nodes)

    def look_for_new_workers( self ):
        x = glob.glob( str(self.path) + "/worker_*/" + create_wildcard_for_task( task=signals.new_worker(), direction=signals.worker_to_head() ) )
        for filename in x:
            _, _, owner = parse_filename( filename )
            worker_path = os.path.dirname( filename )
            if self.greeting_message is not None:
                self._send_job_to_worker( self.greeting_message, worker_path )
                self.working_nodes.add( worker_path )
            else:
                self.available_nodes.add( worker_path )
            print( "Adding new worker: ", worker_path )
            if owner == signals.head_will_delete():
                os.remove( filename )

    def look_for_retiring_workers( self ):
        x = glob.glob( str(self.path) + "/worker_*/" + create_wildcard_for_task( task=signals.spin_down_task(), direction=signals.worker_to_head() ) )
        for filename in x:
            _, _, owner = parse_filename( filename )
            worker_path = os.path.dirname( filename )

            if worker_path in self.available_nodes:
                self.available_nodes.remove( worker_path )
            elif worker_path in self.working_nodes:
                raise Exception( "Worker {} retired while working on a job".format(worker_path) )
            else:
                raise Exception( "Unregistered worker {} retired".format(worker_path) )

            # should we delete the whole directory? Or just this file?
            #os.rmdir( worker_path )

            if owner == signals.head_will_delete():
                os.remove( filename )


    def update_workers( self ):
        self.look_for_new_workers()
        self.look_for_retiring_workers()


    def broadcast_job_to_all_available_workers( self, job_str: str ):
        while self.n_available_workers() > 0:
            self.submit_job( job_str )
        
    def submit_job( self, job_str: str ):
        if self.n_available_workers() == 0:
            raise Exception( "Asked to submit a job despite there being no available nodes. Please query n_available_workers() before submitting." )

        worker_path = self.available_nodes.pop()
        self._send_job_to_worker( job_str, worker_path )
        self.working_nodes.add( worker_path )
        pass
            
    def _send_job_to_worker( self, job_str: str, worker_path: str ):
        filename = create_fileprefix(
            direction = signals.head_to_worker(),
            task = signals.job_task(),
            owner = signals.worker_will_delete()
        ) + ".txt"
        filepath = worker_path + "/" + filename

        with open( filepath, 'w' ) as f:
            f.write( job_str )
            f.close()
            pass
        pass

    def look_for_finished_jobs( self, safety_sleep_window: float = 0.01 ):
        x = glob.glob( str(self.path) + "/worker_*/" + create_wildcard_for_task( task=signals.job_task(), direction=signals.worker_to_head() ) )
        job_results = []

        for filename in x:
            _, _, owner = parse_filename( filename )
            worker_path = os.path.dirname( filename )
            if not worker_path in self.working_nodes:
                raise Exception( "Nonworker reported a job:", worker_path )
            self.working_nodes.remove( worker_path )
            self.available_nodes.add( worker_path )
 
            # let the other node finish writing, if needed
            time.sleep( safety_sleep_window )

            with open( filename, 'r' ) as f:
                contents = f.read()
                job_results.append( contents )
                f.close()

            if owner == signals.head_will_delete():
                os.remove( filename )

        return job_results

    def wait_for_finished_jobs( self, sleep_s = 0, update_workers_while_waiting = True, raise_exception_if_no_workers = True, safety_sleep_window: float = 0.01, max_seconds = None ):
        start = time.time()
        while True:
            job_results = self.look_for_finished_jobs(safety_sleep_window=safety_sleep_window)
            if len(job_results) > 0:
                return job_results
            if update_workers_while_waiting:
                self.update_workers()
                if self.total_n_workers() == 0 and raise_exception_if_no_workers:
                    raise Exception( "All workers died" )
            if max_seconds is not None and (time.time() - start) > max_seconds:
                raise Exception( "Reached Timeout. Maybe all workers are dead?" )
            time.sleep( sleep_s )

    def spin_all_down( self ):
        filename = create_fileprefix(
            direction = signals.head_to_worker(),
            task = signals.spin_down_task(),
            owner = signals.worker_will_delete()
        ) + ".txt"

        for w in self.available_nodes:
            filepath = w + "/" + filename
            with open( filepath, 'w' ) as f:
                f.write( "" )
                f.close()
        self.available_nodes.clear()

        for w in self.working_nodes:
            filepath = w + "/" + filename
            with open( filepath, 'w' ) as f:
                f.write( "" )
                f.close()
        self.working_nodes.clear()

        create_file( str(self.path / "dead") )
