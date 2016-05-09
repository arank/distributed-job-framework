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
import traceback

MAX_SQL_TRIES = 5

QUEUE_REFRESH = 50
MAX_IN_QUEUE = 500

MAX_POLL_TIME = 3600

DB_HOST= "173.194.230.42"
DB_USER = "controller"
DB_PASS = "toor"
DB_NAME = "venmo"


''' SQL Functions to connect to SQL backend that hold job state '''
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

# Generates a new job id in the database
def get_job_id(db):
    cur = db.cursor()
    cur.execute("INSERT INTO jobs (checkout_time) VALUES (\'"+str(time.time())+"\');")
    return cur.lastrowid

# Removes an existing job in the database
def kill_job_id(db, job_id):
    cur = db.cursor()
    cur.execute("DELETE FROM jobs WHERE job_id = \'"+str(job_id)+"\';")
    db.commit()

# Pulls an existing job and all its data from the database
def pull_job_id(db, job_id):
    cur.execute("SELECT * FROM jobs WHERE job_id = \'"+str(job_id)+"\';")
    row = cur.fetchone()
    if row is not None:
        return row
    return None

# Updates a completed job into the database
def push_job_stats(db, checkin_time, process_time, job_id):
    cur = db.cursor()
    cur.execute("UPDATE jobs SET checkin_time =  \'"+str(checkin_time)+"\', process_time =  \'"+str(process_time)+"\' WHERE job_id = \'"+str(job_id)+"\';")
    db.commit()

# The queue for errors from jobs
error_queue =  Queue('err_t')

# The set of completed jobs
complete_queue = Queue('comp_t')

# Priority Queue List
queue_list = []

complete_job_str = None
db = connect_to_sql()

def add_new_jobs(checkout_job_packet):
    # Update queues with new jobs if the workers have cleared jobs below the threshold
    for index, queue in enumerate(queue_list):
        num_in_queue = queue.length()
        if num_in_queue < QUEUE_REFRESH:
            # Add new edges up to max in queue
            to_add = MAX_IN_QUEUE - num_in_queue
            while to_add > 0:
                try: 
                    job_id = get_job_id(db)
                    jobs = checkout_job_packet(job_id, queue.id_name)
                    queue.push(json.dumps(object_to_push))
                    to_add -= 1
                except Exception as e:
                    print "failed to add jobs to queue "+str(queue.id_name)
                    kill_job_id(db, job_id)
                    break

def process_complete_jobs(limit):   
    # Check for completed jobs and process them
    print "checking for completed jobs..."
    complete_job_str = complete_queue.pop()
    jobs_processed = 0
    # {'new_profiles', 'new_edges', 'new_transactions', 'crawled_profiles', 'scrape_id', 'time_to_complete', 'time_completed'}       
    while jobs_processed < limit and complete_job_str is not None:
        try:
            complete_job = json.loads(complete_job_str)

            process_job_results(complete_job)

            # complete scrape and stats
            print "updating scrape stats..."
            start = time.time()
            push_job_stats(db, time.time(), complete_job['time_completed'], complete_job['job_id'])
            print "finished in "+str(time.time()-start)     

            jobs_processed += 1
            complete_job_str = complete_queue.pop()

        except Exception as e:
            print "error unhandled fail in checkin code"
            # Throw the complete job back to error queue
            error_queue.push(complete_job_str)