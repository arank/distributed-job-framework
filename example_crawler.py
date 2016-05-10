import worker
import util
import urllib2

"""
Example worker that pulls from queues and either scrapes a url or at a higer priority factors a large number, 
depending on the queue it pulls from
"""
def job(arguments, job_type, lock, priority):
	# If we have a url queue argument pull the html from it
	if job_type == 'url':
	    req = urllib2.Request(arguments.url, None)
	    #check lock for higher priority job
	    worker.check_lock(lock, priority)
	    opened = urllib2.urlopen(req)
	    # return a dict result object
	    return {"arg":arguments.url, "res": opened.read()}
	# if we have a math argument, factor the number in it 
	elif job_type == 'math': 
		n = int(arguments.num) 
    	res = set(reduce(list.__add__, 
                ([i, n//i] for i in range(1, int(n**0.5) + 1) if n % i == 0)))
    	# return a dict result object
    	return {"arg":arguments.num, "res": res}

queues = [util.Queue("math"), util.Queue("url")]

# Start polling the queues for work packets and doing jobs
worker.get_work(queues, job)
