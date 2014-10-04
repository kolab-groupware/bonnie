#!/usr/bin/python

import signal
from bonnie.collector import BonnieCollector

if __name__ == "__main__":
    collector = BonnieCollector()
    signal.signal(signal.SIGTERM, collector.terminate)
    collector.run()
