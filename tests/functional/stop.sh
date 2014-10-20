#!/bin/bash

killall -q broker.py worker.py collector.py
rm -f /tmp/bonnie.pid