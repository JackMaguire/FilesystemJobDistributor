from filesystemjd import signals
from filesystemjd.signals import create_fileprefix, create_wildcard_for_direction
from filesystemjd.util import create_file
from pathlib import Path
import os

def attempt_to_create_a_directory( path: Path ) -> None:
    os.mkdir( str(path) )

    if not path.exists():
        raise Exception( "unable to create dir: {}".format( str(path) ) )
    
    if not path.is_dir():
        raise Exception( "new path is not recognaized as a dir: {}".format( str(path) ) )


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
            task = signals.new_worker_signal(),
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

    def wait_for_job( self ):
        """
        Returns:
        - string: the message for the new job
        - boolean: If the bool is true, then the simulation is over and it is time to shutdown
        """
        while True:
            x = glob.glob(
                create_wildcard_for_direction( signals.head_to_worker() ),
                root_dir = self.path
            )
            for filename in x:
                direction, task, owner = parse_filename( filename )
                contents = 
