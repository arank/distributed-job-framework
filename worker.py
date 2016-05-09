#Created by Aran Khanna All Rights Reserved
import urllib2
import time
from bs4 import BeautifulSoup
from sets import Set
import random
import numpy as np
import math
import sys
import os
import re
import tarfile
import threading
import json
from optparse import OptionParser
from datetime import datetime, timedelta
import util

current_priority = None
complete_queue = Queue('comp_t')

MAX_POLL_TIME = 3600
BACK_OFF_LENGTH = 500


# queues are a priority ordered list of queues to check
def poll_for_work_packet(queues, max_queue_poll_time=MAX_POLL_TIME):
    poll_time = 1
    while True:
        for i, queue in enumerate(queues):
            if i < current_priority:
                new_job_str = queue.pop()
                if new_job_str is not None:
                    return new_job_str, queue.id_name, i
        print "waiting " +str(poll_time)+ " secs till next poll"
        time.sleep(poll_time)
        if poll_time < max_queue_poll_time:
            poll_time = min(poll_time*2, max_queue_poll_time)

# Call this function at appropriate points in your worker function to cede to higher priority jobs
def check_lock(lock, priority):
    if priority > current_priority:
        lock.release()
        while priority > current_priority:
            sleep(3)
        lock.acquire()


def do_work(job_type, job_str, priority, prev_priority, worker, lock):
    lock.acquire()         
    packet = json.loads(job_str)
    startTime = time.time()

    check_lock()
    serialized_results = json.dumps({'results': worker(packet, job_type, lock, priority), 'job_type': job_type, 'time_to_complete':time.time()-startTime, 'time_completed':time.time()})
    check_lock()

    current_priority = prev_priority
    lock.release()

    poll_time = 1
    while complete_queue.length() > BACK_OFF_LENGTH:
        print "waiting " +str(poll_time)+ " secs for complete queue to clear"
        time.sleep(poll_time)
        if poll_time < MAX_POLL_TIME:
            poll_time = min(poll_time*2, MAX_POLL_TIME)

    complete_queue.push(serialized_results)


"""
Takes a list of queues in priority order and a callable function worker which takes an arbitrary python dict 
of arguments (known as a work packet), a job type (based on the name of the queue the work packet was pulled from),
a lock pointer and a job priority to be used in the check_lock function to cede control to any higher priority jobs
that are queued up on this worker, and returns a python object result.
"""
def get_work(queues, worker):
    current_priority = queues.length
    lock = threading.Lock()
    while True:
        job_str, job_type, priority = poll_for_work_packet(queues)
        prev_priority = current_priority
        current_priority = priority

        # Make a new thread for the job
        thread = threading.Thread(target=do_work, args=(job_type, job_str, priority, prev_priority, worker, lock))
        thread.start()

