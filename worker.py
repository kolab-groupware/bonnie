#!/usr/bin/python

import signal
from bonnie.worker import BonnieWorker

if __name__ == "__main__":
    worker = BonnieWorker()
    signal.signal(signal.SIGTERM, worker.terminate)
    worker.run()

