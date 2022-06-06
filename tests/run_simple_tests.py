from filesystemjd.worker import Worker
from filesystemjd.head import Head
#from filesystemjd.signals import create_fileprefix, create_wildcard_for_direction
#from filesystemjd.util import create_file

import random

def run_test1():

    signal_dir = "/tmp/fsjd_" + ''.join(random.choice('0123456789ABCDEF') for i in range(6))

    h = Head( signal_dir=signal_dir )
    w1 = Worker( signal_dir=signal_dir, unique_key="w1" )
    w2 = Worker( signal_dir=signal_dir, unique_key="w2" )
    h.update_workers()

    assert h.total_n_workers() == 2

    w3 = Worker( signal_dir=signal_dir, unique_key="w3" )
    h.update_workers()

    assert h.total_n_workers() == 3




if __name__ == '__main__':
    run_test1()
