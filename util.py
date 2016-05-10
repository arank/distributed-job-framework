import redis

POOL = redis.ConnectionPool(host='104.196.119.85', port=6379, db=0)
r = redis.Redis(connection_pool=POOL)

class Queue(object):
    """An abstract FIFO queue based on redis"""
    def __init__(self, queue_name=None):
        #create new queue
        if queue_name is None:
            local_id = r.incr(queue)
            queue_name = "queue:%s" %(local_id)
            
        self.id_name = queue_name

    def push(self, element):
        """Push an element to the tail of the queue""" 
        id_name = self.id_name
        push_element = r.lpush(id_name, element)

    def pop(self):
        """Pop an element from the head of the queue"""
        id_name = self.id_name
        popped_element = r.rpop(id_name)
        return popped_element

    def length(self):
        id_name = self.id_name
        queue_len = r.llen(id_name)
        return queue_len
