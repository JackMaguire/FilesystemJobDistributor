from filesystemjd.worker import Worker
from filesystemjd.head import Head

import time
import random

#signal_dir = "/tmp/fsjd_" + ''.join(random.choice('0123456789ABCDEF') for i in range(6))
signal_dir = "/tmp/async_test/"

h = Head( signal_dir=signal_dir )

t0 = time.time()
time_limit_seconds = 60

while h.total_n_workers() == 0 and (time.time()-t0) < time_limit_seconds:
    h.update_workers()

assert h.total_n_workers() > 0

for i in range( 0, int(time_limit_seconds) ):
    h.update_workers()

    if h.n_available_workers() == 0:
        assert h.total_n_workers() > 0
        job_results = h.wait_for_finished_jobs()
        print( "HEAD", job_results )

    h.submit_job( str(i) )

assert h.n_available_workers() == 0
job_results = h.wait_for_finished_jobs()
print( "HEAD", job_results )

print( "HEAD workers:", h.n_busy_workers(), "of", h.total_n_workers() )

h.spin_all_down()
