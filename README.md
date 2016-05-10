# Distributed Job Framework
This is a set of python libraries to run a distributed jobs on for our CS262 final project.

For running an arbitrary set of functions, that have a simple set of text argument inputs and outputs, multiple times with different arguments and different priorities across machines, it may be nessecary to distribute the task intelligently and efficiency. Our system provides a framework in the form of 2 python libraries that allows you to do this. A user can import worker.py to get access to the get_work function which takes a priority ordered list of job queues that hold arguments and a function defining how to process those arguments into a json to return, and automatically pulls jobs from queues in priority order to process. A user can import controller.py to get access to a set of functions that let you load arguments into the priority queue while tracking failed jobs and ensuring the system runs at any scale without overloading queues and losing data.

This is dependent on Python redis libary and Python mySQLdb libary.

To configure you will need a redis server running (locally or on a seperate machine) and a mySQL instance with table scrapes with an autoincrement int field scrape_id and null int fields checkout_time process_time and checkin_time.

Fill in the appropriate global variables in controller.py and util.py to connect to the database and redis instance.

Look at the example_server.py and the example_cralwer.py files to get a sense of how the system works.



