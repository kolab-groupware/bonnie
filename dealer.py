#!/usr/bin/python

import json
import os
import sys

from bonnie.dealer import BonnieDealer

if __name__ == "__main__":
    notification = sys.stdin.read().strip()

    newpid = os.fork()

    if newpid == 0:
        dealer = BonnieDealer()
        dealer.run(notification)
