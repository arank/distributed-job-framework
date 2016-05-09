import controller
import util

urls = ["https://en.wikipedia.org/wiki/Harvard_University", "https://en.wikipedia.org/wiki/Michelle_Obama", "https://en.wikipedia.org/wiki/Mayor_of_Chicago", "https://en.wikipedia.org/wiki/Rahm_Emanuel", "https://en.wikipedia.org/wiki/O._W._Wilson"]
math = [179424691, 179425661, 179426231, 179426333, 179425819, 179426549]
checkouts={}

while True:
	controller.add_new_jobs()

queues = [util.Queue("url"), util.Queue("math")]

def checkout_packet(job_id, job_type):
