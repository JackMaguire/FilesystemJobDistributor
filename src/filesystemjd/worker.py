from filesystemjd.signals import *
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

        self.path = Path( "{}/worker_{}/".format(signal_dir, unique_key) )
        if self.path.exists():
            raise ValueError( "unique_key {} is not unique. Or at least, this path already exists: {}".format( unique_key, str(self.path) ) )
        attempt_to_create_a_directory( self.path )

    def
