#!/bin/bash

BASEDIR=$1
: ${BASEDIR:="."}

killall -q broker.py worker.py collector.py

cd $BASEDIR

./broker.py -d 8 2>/dev/null > /dev/null &
./worker.py -d 8 2>/dev/null > /dev/null &
./collector.py -d 8 2>/dev/null > /dev/null &
