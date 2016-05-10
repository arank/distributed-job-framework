import redis
import urllib2
import time
import random
import numpy as np
import math
import sys
import os
import MySQLdb
import json
import util

# The maximum number of tries to connect to SQL before giving up
MAX_SQL_TRIES = 5

# The smallest size a queue can be before we refresh it
QUEUE_REFRESH = 50

# The maximum size a queue can be
MAX_IN_QUEUE = 100

# SQL Database connection info
DB_HOST= "173.194.230.42"
DB_USER = "controller"
DB_PASS = "toor"
DB_NAME = "venmo"


''' SQL Functions to connect to SQL backend that holds job state returning none if no connection is available'''
def connect_to_sql(max_tries = MAX_SQL_TRIES):
    tries = 0
    while tries < max_tries:
        time.sleep(2**tries)
        try:
            db = MySQLdb.connect(host=DB_HOST,    # your host, usually localhost
                         user=DB_USER,         # your username
                         passwd=DB_PASS,  # your password
                         db=DB_NAME)        # name of the data base
            return db
        except Exception as e:
            print "failed to connect to sql with error "+str(e)+" retrying in a few"
            tries += 1
    return None

""" Given a database connection object generates and returns a new job_id checkout at the current time in the database"""
def get_job_id(db):
    cur = db.cursor()
    cur.execute("INSERT INTO jobs (checkout_time) VALUES (\'"+str(time.time())+"\');")
    return cur.lastrowid

""" 
Given a database connection object and job id, removes job id from the database doing nothing if job is not found
(useful ror removing expired jobs) 
"""
def kill_job_id(db, job_id):
    cur = db.cursor()
    cur.execute("DELETE FROM jobs WHERE job_id = \'"+str(job_id)+"\';")
    db.commit()

"Given a database connection object and job id, pulls the existing job and all its data from the database, returning None if not found"
def pull_job_id(db, job_id):
    cur.execute("SELECT * FROM jobs WHERE job_id = \'"+str(job_id)+"\';")
    row = cur.fetchone()
    if row is not None:
        return row
    return None

"""
This can be called with the database connection object and a timeout in second to select a single timed out job to 
return a list with the specific job id and checkout time or none if there are no timed out jobs
"""
def get_expired_job(db, time_limit):
    cur = db.cursor()
    cur.execute("SELECT * FROM jobs WHERE checkin_time = NULL AND checkout_time < \'"+str(time.time()-time_limit)+"\';")
    row = cur.fetchone()
    if row is not None:
        return row
    return None

"""
Given a database connection object, a checkin time (in sec since epoch), a processed time, and a job id
checks the job in to the database updating the time it was put back in, and the time it finished processing on the worker
throws an exception if the job doesn't exist
"""
def push_job_stats(db, checkin_time, process_time, job_id):
    cur = db.cursor()
    cur.execute("UPDATE jobs SET checkin_time =  \'"+str(checkin_time)+"\', process_time =  \'"+str(process_time)+"\' WHERE job_id = \'"+str(job_id)+"\';")
    db.commit()

# The queue for errors from jobs
error_queue =  Queue('err_t')

# The queue of completed jobs
complete_queue = Queue('comp_t')

# The database object to hold for SQL connection
db = connect_to_sql()

"""
Given a list of queue objects in priority order and a function to checkout a job taking a job id and a queue name 
and returning a dict with the job arguments, iterates over the queues in priority order, refilling them if 
they have gotten too empty.
"""
def add_new_jobs(checkout_job_packet, queue_list):
    # Update queues with new jobs if the workers have cleared a queue to below its threshold size
    for index, queue in enumerate(queue_list):
        num_in_queue = queue.length()
        if num_in_queue < QUEUE_REFRESH:
            # Add new edges up to max in queue
            to_add = MAX_IN_QUEUE - num_in_queue
            while to_add > 0:
                try: 
                    job_id = get_job_id(db)
                    job = checkout_job_packet(job_id, queue.id_name)
                    if job is not None:
                        queue.push(json.dumps(job))
                        to_add -= 1
                    else:
                        return None
                except Exception as e:
                    print "failed to add jobs to queue "+str(queue.id_name)
                    kill_job_id(db, job_id)
                    break

"""
Given a limit of jobs to process and a process job function taking a dict of results from the job and processing
them however the user sees fit, this will pull up to the limit of completed jobs, process them back into the database
if no jobs are complete it will return immediately. If the processing fails the raw job string will be put onto an 
error queue for debugging purposes so regenerating the results doesn't need to be done.
"""
def process_complete_jobs(limit, process_job_results): 
    # The string pulled from the complete queue
    print "checking for completed jobs..."
    complete_job_str = complete_queue.pop()
    jobs_processed = 0
    while complete_job_str is not None:
        try:
            # Processes the complete job from the queue
            complete_job = json.loads(complete_job_str)
            process_job_results(complete_job.results)

            # complete scrape and stats
            print "updating scrape stats..."
            start = time.time()
            push_job_stats(db, time.time(), complete_job['time_completed'], complete_job['job_id'])
            print "finished in "+str(time.time()-start)     

            # Check if we have processed enough else get another
            jobs_processed += 1
            if jobs_processed > limit:
                break
            else:
                complete_job_str = complete_queue.pop()

        except Exception as e:
            print "error unhandled fail in checkin code"
            # Throw the complete but malformed job back to error queue
            error_queue.push(complete_job_str)