import time

class Collector(object):
    state = None
    job = None

    def __init__(self, collector_id=None, state=b"READY", interests=[]):
        self.collector_id = collector_id
        self.state = state
        self.interests = interests
        self.timestamp = time.time()

    def set_status(self, state):
        self.state = state
        self.timestamp = time.time()
