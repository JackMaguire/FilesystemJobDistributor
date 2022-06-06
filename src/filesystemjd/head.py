from filesystemjd import signals
from filesystemjd.signals import create_fileprefix, create_wildcard_for_direction, create_wildcard_for_task, parse_filename
from filesystemjd.util import create_file, attempt_to_create_a_directory

from pathlib import Path
import os
import time

import glob

class Head:
    def __init__( self, signal_dir: str ):
        self.path = Path( signal_dir )

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


    def submit_job( self, job_str: str ):
        if self.n_available_workers() == 0:
            raise Exception( "Asked to submit a job despite there being no available nodes. Please query n_available_workers() before submitting." )

        worker_path = self.available_nodes.pop()
        
        filename = create_fileprefix(
            direction = signals.head_to_worker(),
            task = signals.job_task(),
            owner = signals.worker_will_delete()
        ) + ".txt"
        filepath = worker_path + "/" + filename

        with open( filepath, 'w' ) as f:
            f.write( job_str )
            f.close()

        self.working_nodes.add( worker_path )

    def look_for_finished_jobs( self ):
        x = glob.glob( str(self.path) + "/worker_*/" + create_wildcard_for_task( task=signals.job_task(), direction=signals.worker_to_head() ) )
        job_results = []

        for filename in x:
            _, _, owner = parse_filename( filename )
            worker_path = os.path.dirname( filename )
            if not worker_path in self.working_nodes:
                raise Exception( "Nonworker reported a job:", worker_path )
            self.working_nodes.remove( worker_path )
            self.available_nodes.add( worker_path )
 
            with open( filename, 'r' ) as f:
                contents = f.read()
                job_results.append( contents )
                f.close()

            if owner == signals.head_will_delete():
                os.remove( filename )

        return job_results

    def wait_for_finished_jobs( self, sleep_s = 0 ):
        while True:
            job_results = self.look_for_finished_jobs()
            if len(job_results) > 0:
                return job_results
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
