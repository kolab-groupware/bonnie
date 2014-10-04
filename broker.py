#!/usr/bin/python

import signal
from bonnie.broker import BonnieBroker

if __name__ == "__main__":
    broker = BonnieBroker()
    signal.signal(signal.SIGTERM, broker.terminate)
    broker.run()
