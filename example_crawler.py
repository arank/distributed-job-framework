import worker
import util
import urllib2

def job(arguments, job_type, lock, priority):
	if job_type == 'url':
	    req = urllib2.Request(arguments.url, None)
	    worker.check_lock(lock, priority)
	    opened = urllib2.urlopen(req)
	    return {html: opened.read()}
	elif job_type == 'math': 
		n = int(arguments.num) 
    	return set(reduce(list.__add__, 
                ([i, n//i] for i in range(1, int(n**0.5) + 1) if n % i == 0)))

queues = [util.Queue("url"), util.Queue("math")]
get_work(queues, job)
