#Created by Aran Khanna All Rights Reserved
import time
import threading
import json
import util

# The priority of the job currently being completed by this worker
current_priority = None

# The queue of complete jobs
complete_queue = Queue('comp_t')

# The maximum time to wait to poll for a new job
MAX_POLL_TIME = 3600

# The maximum length of the complete queue before we wait for it to clear a bit
BACK_OFF_LENGTH = 500

"""
Given a list of queues in priority order iterates over them checking in order for a new job, and returning 
the job argument json string, the queue it came from, and that queue's priority. If all queues are empty it
waits progressively until a new job is found. If the current_priority is set, it only looks for jobs more important
"""
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

"""
Takes a lock, owned by the calling thread, and the current priority of the job the thread is working on.
Call this function at appropriate points in your worker function to cede to higher priority jobs if there are any
and progressively wait till your thread has the highest priority job again. 
"""
def check_lock(lock, priority):
    if priority > current_priority:
        lock.release()
        while priority > current_priority:
            sleep(3)
        lock.acquire()

"""
Takes a job_str a json string with job arguments, a job type (i.e. the queue the job came from)
the priority of the job the worker had before this, the priority of this job, a lock to control thread execution,
and a worker function that takes the argument dictionary, the job type, the lock and priority and returns a results dict.
Waits to acquire the execution lock then runs the job, resets the priority and releases the execution lock and pushed the
result dict to the complete queue.
"""
def do_work(job_type, job_str, priority, prev_priority, worker, lock):
    # wait for execution lock
    lock.acquire()         
    packet = json.loads(job_str)
    startTime = time.time()

    # write back results
    serialized_results = json.dumps({'results': worker(packet, job_type, lock, priority), 'job_type': job_type, 'time_to_complete':time.time()-startTime, 'time_completed':time.time()})

    # wait for room on complete queue and push results
    poll_time = 1
    while complete_queue.length() > BACK_OFF_LENGTH:
        print "waiting " +str(poll_time)+ " secs for complete queue to clear"
        time.sleep(poll_time)
        if poll_time < MAX_POLL_TIME:
            poll_time = min(poll_time*2, MAX_POLL_TIME)
    complete_queue.push(serialized_results)

    # Gives up execution privalages before exiting, and resets priority
    current_priority = prev_priority
    lock.release()


"""
Takes a list of queues in priority order and a callable function worker which takes an arbitrary python dict 
of arguments (known as a work packet), a job type (based on the name of the queue the work packet was pulled from),
a lock pointer and a job priority to be used in the check_lock function to cede control to any higher priority jobs
that are queued up on this worker, and returns a python object result.
"""
def get_work(queues, worker):
    current_priority = queues.length
    lock = threading.Lock()

    # keep looking for new, higher priority work packets
    while True:
        job_str, job_type, priority = poll_for_work_packet(queues)
        prev_priority = current_priority
        current_priority = priority

        # Make a new thread for the job
        thread = threading.Thread(target=do_work, args=(job_type, job_str, priority, prev_priority, worker, lock))
        thread.start()

