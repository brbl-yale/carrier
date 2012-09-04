#!/usr/bin/python

"""
    Lightweight Thread-Pool library for accelerating conversion process
    
    Code credit: http://code.activestate.com/recipes/577187-python-thread-pool/
"""

from Queue import Queue
from threading import Thread

"""
  @class    Worker
  @inherits Thread
  
  Lightweight worker class that continually performs a set of tasks (function
  pointers)
"""
class Worker(Thread):
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            func(*args, **kargs)
            self.tasks.task_done()

"""
  @class    ThreadPool

  A managed ThreadPool that can have tasks added to it, the tasks are
  continually consumed by Worker threads
"""
class ThreadPool:
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for x in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        self.tasks.put((func, args, kargs))

    def await_completion(self):
        self.tasks.join()

########## CODE TAKEN FROM ABOVE DEMO #################
if __name__ == '__main__':
    from random import randrange
    delays = [randrange(1, 10) for i in range(50)]
    
    from time import sleep
    def wait_delay(d):
        print 'sleeping for (%d)sec' % d
        sleep(d)
    
    # 1) Init a Thread pool with the desired number of threads
    pool = ThreadPool(20)
    
    for i, d in enumerate(delays):
        # print the percentage of tasks placed in the queue
        print '%.2f%c' % ((float(i)/float(len(delays)))*100.0,'%')
        
        # 2) Add the task to the queue
        pool.add_task(wait_delay, d)
    
    # 3) Wait for completion
    pool.await_completion()

