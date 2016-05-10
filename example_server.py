import controller
import time
import util

# Urls and math problems to solve
urls = ["https://en.wikipedia.org/wiki/Harvard_University", "https://en.wikipedia.org/wiki/Michelle_Obama", "https://en.wikipedia.org/wiki/Mayor_of_Chicago", "https://en.wikipedia.org/wiki/Rahm_Emanuel", "https://en.wikipedia.org/wiki/O._W._Wilson"]
math = [179424691, 179425661, 179426231, 179426333, 179425819, 179426549]

# Dict to associate checked out jobs with job ids from the system
checkouts={}

# A dict to store finished jobs
finished = {}

# Check what kind of job is being requested and pull a new one from the appropriate list
# registering the id the job is being checked out under and returning none if theres nothing
# left to process for that job type
def checkout_packet(job_id, job_type):
	if job_type == "math":
		res = math.pop()
		to_ret = {"math": res} 
	elif job_type == "url":
		res =  urls.pop()
		to_ret = {"url": res}
	if res is not None:
		checkouts[str(res)] = job_id
		return to_ret
	else:
		return None

# Simply adds the finished results to our results dictionary
def process_jobs_results(results):
	finished[str(results.arg)] = results.res
	print finished

# define the queues and priority order
queues = [util.Queue("math"), util.Queue("url")]

# Loop through adding new jobs and processing completed ones 
# (in a larger system you can have 2 seperate processes handle these tasks)
while True:
	# adds new jobs to any empty queues using the checkout packet fucntion
	controller.add_new_jobs(checkout_packet, queues)
	time.sleep(10)
	# processes up to 5 complete jobs using the process job results function
	controller.process_complete_jobs(5, process_job_results)
	time.sleep(10)

