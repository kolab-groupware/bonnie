#!/usr/bin/python

import json
import sys

from bonnie.dealer import BonnieDealer

if __name__ == "__main__":
    notification = sys.stdin.read().strip()

    dealer = BonnieDealer()
    dealer.run(notification)
