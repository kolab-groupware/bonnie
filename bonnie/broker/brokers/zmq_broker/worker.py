import time

class Worker(object):
    state = None
    job = None

    def __init__(self, worker_id=None, state=b"READY", job=None):
        self.worker_id = worker_id
        self.state = state
        self.job = job
        self.timestamp = time.time()

    def set_status(self, state):
        self.state = state
        self.timestamp = time.time()
