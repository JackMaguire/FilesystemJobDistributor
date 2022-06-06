from filesystemjd.worker import Worker
from filesystemjd.head import Head

import time
import random

signal_dir = "/tmp/async_test/"
key = ''.join(random.choice('0123456789ABCDEF') for i in range(6) )
w = Worker( signal_dir=signal_dir, unique_key=key )

t0 = time.time()

while True:
    if time.time() - t0 > 120:
        raise Exception( "Worker was left to die" )

    contents, spin_down, more_messages = w.query_for_job()
    if contents == None:
        continue

    if spin_down:
        break

    print( key, contents )

    time.sleep( random.uniform( 0.1, 1 ) )

    w.send_results_of_job( contents )
