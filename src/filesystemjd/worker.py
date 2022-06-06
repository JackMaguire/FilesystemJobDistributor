from filesystemjd import signals
from filesystemjd.signals import create_fileprefix, create_wildcard_for_direction, parse_filename
from filesystemjd.util import create_file, attempt_to_create_a_directory

from pathlib import Path
import os
import time
import glob

class Worker:
    def __init__( self,
                  signal_dir: str,
                  unique_key: str,
                 ):
        """
        - signal_dir (str): directory name for all of the signals in this project.
        - unique_key (str): some unique (by your filesystem's definition of unique) identifier for this worker. We will create the $signal_dir/worker_$unique_key directory
        """

        # create directory
        self.path = Path( "{}/worker_{}/".format(signal_dir, unique_key) )
        if self.path.exists():
            raise ValueError( "unique_key {} is not unique. Or at least, this path already exists: {}".format( unique_key, str(self.path) ) )
        attempt_to_create_a_directory( self.path )

        # create "hello" file
        hello_filename = create_fileprefix(
            direction = signals.worker_to_head(),
            task = signals.new_worker(),
            owner = signals.head_will_delete()
        ) + ".txt"
        self.hello_filepath = self.path / hello_filename
        create_file( self.hello_filepath )
        if not self.hello_filepath.exists():
            raise Exception( "failed to create: " + str(self.hello_filepath) )
        



    def head_has_registered_worker( self ):
        """
        Check to see if the hello file still exists. If not, then the head has accepted our handshake
        """
        return not self.hello_filepath.exists()


    def spin_self_down( self ):
        """
        Tell the head node that this node is no longer available to run jobs.
        """
        filename = create_fileprefix(
            direction = signals.worker_to_head(),
            task = signals.spin_down_task(),
            owner = signals.head_will_delete()
        ) + ".txt"
        filepath = self.path / filename
        create_file( filepath )
        if not filepath.exists():
            raise Exception( "failed to create: " + str(filepath) )
        

    def query_for_job( self ):
        x = glob.glob( str(self.path) + "/" + create_wildcard_for_direction( signals.head_to_worker() ) )
        if len(x) == 0:
            #time.sleep( sleep_s )
            return None, None, None

        more_messages = (len(x) > 1)

        # just read the first one
        filename = x[0]
        direction, task, owner = parse_filename( filename )

        # let the head node finish writing, if needed
        time.sleep( 0.1 )

        with open( filename, 'r' ) as f:
            contents = f.read()
            f.close()

        if owner == signals.worker_will_delete():
            os.remove( filename )

        spin_down = task == signals.spin_down_task()

        return contents, spin_down, more_messages

    def wait_for_job( self, sleep_s = 0 ):
        """
        Parameters:
        - sleep_s: the number of seconds to sleep between checks for new messages. Defaults to 0. Type can be anything accepted by time.sleep()
        Returns:
        - string: the message for the new job
        - boolean: If the bool is true, then the simulation is over and it is time to shutdown
        - boolean: If true, there are more messages waiting
        """
        while True:
            a, b, c = self.query_for_job()
            if a == None and b == None and c == None:
                time.sleep( sleep_s )
                continue
            else:
                return a, b, c

    def send_results_of_job( self, results: str ):
        """
        Parameters:
        - results (string): this string will be parsed by the head node in a manner determined by you, the developer
        """
        filename = create_fileprefix(
            direction = signals.worker_to_head(),
            task = signals.job_task(),
            owner = signals.head_will_delete()
        ) + ".txt"
        filepath = self.path / filename
        with open( filepath, 'w' ) as f:
            f.write( results )
            f.close()
        if not filepath.exists():
            raise Exception( "failed to create: " + str(filepath) )
