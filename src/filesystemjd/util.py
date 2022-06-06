from pathlib import Path
import os

def create_file( filename, contents = "new file" ):
    with open( filename, 'w' ) as f:
        f.write( contents )

def attempt_to_create_a_directory( path: Path ) -> None:
    os.mkdir( str(path) )

    if not path.exists():
        raise Exception( "unable to create dir: {}".format( str(path) ) )
    
    if not path.is_dir():
        raise Exception( "new path is not recognaized as a dir: {}".format( str(path) ) )

