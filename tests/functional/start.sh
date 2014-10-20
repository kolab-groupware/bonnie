#!/bin/bash

BASEDIR=$1
: ${BASEDIR:="."}

if [ -f /tmp/bonnie.pid ]; then
  echo "Bonnie already running (/tmp/bonnie.pid exists)"
  exit
fi

killall -q broker.py worker.py collector.py

cd $BASEDIR

./broker.py -d 8 2>/dev/null > /dev/null &
./worker.py -d 8 2>/dev/null > /dev/null &
./collector.py -d 8 2>/dev/null > /dev/null &

touch /tmp/bonnie.pid
