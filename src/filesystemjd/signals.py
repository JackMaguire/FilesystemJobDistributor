import os

# Slot 1: Direction

def head_to_worker() -> str:
    return "h2w"

def worker_to_head() -> str:
    return "w2h"

# Slot 2: Task

def new_worker() -> str:
    return "new_worker"

def spin_down_task() -> str:
    return "spin_down"

def job_task() -> str:
    return "job"

def new_message_task() -> str:
    return "message"

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
    return create_fileprefix( direction=direction, task="*", owner="*" )

def create_wildcard_for_task( task:str, direction: str = worker_to_head() ):
    return create_fileprefix( direction=direction, task=task, owner="*" )

def parse_filename( filename ):
    bn = os.path.basename( filename )
    tokens = bn.split(".")
    
    direction = None
    task = None
    owner = None

    if len(tokens) < 3:
        raise Exception( "Unable to parse: " + filename )

    return tokens[0], tokens[1], tokens[2]
