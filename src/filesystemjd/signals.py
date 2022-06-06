import os

# Slot 1: Direction

def head_to_worker() -> str:
    return "h2w"

def worker_to_head() -> str:
    return "w2h"

# Slot 2: Task

def new_worker_task() -> str:
    return "new_worker"

def spin_down_task() -> str:
    return "spin_down"

def new_job_task() -> str:
    return "job"

# Slot 3: Ownership

def head_will_delete() -> str:
    return "hdel"

def worker_will_delete() -> str:
    return "wdel"

# Parsing

def create_fileprefix( direction: str, task: str, owner: str  ):
    if "." in direction:
        raise ValueError( "There is a . in direction: " + direction ) 
    if "." in task:
        raise ValueError( "There is a . in task: " + task ) 
    if "." in owner:
        raise ValueError( "There is a . in owner: " + owner ) 
    return "{}.{}.{}".format( direction, task, owner )

def create_wildcard_for_direction( direction: str ):
    return [filename for filename in os.listdir(folder) if filename.startswith('spam')]

def parse_filename( filename ):
    bn = os.path.basename( filename )
    tokens = bn.split(".")
    
    direction = None
    task = None
    owner = None

    if len(tokens) < 3:
        raise Exception( "Unable to parse: " + filename )

    return tokens[0], tokens[1], tokens[2]
