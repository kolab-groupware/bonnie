# -*- coding: utf-8 -*-
# Copyright 2010-2014 Kolab Systems AG (http://www.kolabsys.com)
#
# Jeroen van Meeuwen (Kolab Systems) <vanmeeuwen a kolabsys.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 3 or, at your option, any later
# version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Library General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307,
# USA.
#

"""
    This is the ZMQ broker implementation for Bonnie.
"""

import copy
from multiprocessing import Process
import random
import re
import sys
import time
import zmq

import bonnie
from routers import *
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.broker.ZMQBroker')

import collector
import job
import worker

class ZMQBroker(object):
    def register(self, callback):
        """
            Register myself as a broker.
        """
        callback({ '_all': self.run })

    def cb_collector_router_recv(self, message, *args, **kw):
        """
            Callback on receiving messages on the collector router
            stream.
        """
        log.debug("Collector router: %r" % (message), level=8)
        print "Collector router: %r" % (message)

        identity = message[0]
        cmd = message[1]

        if cmd == b'STATE':
            state = message[2]
            collector.set_state(identity, state)

    def cb_dealer_router_recv(self, message, *args, **kw):
        """
            Callback on receiving messages on the dealer router
            stream.
        """
        log.debug("Dealer router: %r" % (message), level=8)
        print "Dealer router: %r" % (message)

        dealer_identity = message[0]
        notification = message[1]

        job.add(dealer_identity, notification)

        self.dealer_router.send_multipart([dealer_identity, b'ACK' ])

    def cb_worker_controller_router_recv(self, message, *args, **kw):
        """
            Callback on receiving messages on the worker controller
            router stream.
        """
        log.debug("Worker controller: %r" % (message), level=8)
        print "Worker controller: %r" % (message)

        worker_identity = message[0]
        cmd = message[1]

        if cmd == b'STATE':
            state = message[2]
            worker.set_state(worker_identity, state)

        elif cmd == b'DONE':
            job_uuid = message[2]
            job.set_state(job_uuid, cmd)

        else:
            log.error("Worker controller unknown cmd: %s" % (cmd))

    def cb_worker_router_recv(self, message, *args, **kw):
        """
            Callback on receiving messages on the worker router
            stream.
        """
        log.debug("Worker router: %r" % (message), level=8)
        print "Worker router: %r" % (message)

        worker_identity = message[0]
        cmd = message[1]
        job_uuid = message[2]

        if cmd == b'TAKE':
            worker.send_job(worker_identity, job_uuid)
        else:
            log.error("Worker router unknown cmd %s" % (cmd))

    def run(self):
        log.info("Starting the collector router...")
        self.collector_router = CollectorRouter(
                callback=self.cb_collector_router_recv
            )
        self.collector_router.start()

        log.info("Starting the dealer router...")
        self.dealer_router = DealerRouter(
                callback=self.cb_dealer_router_recv
            )
        self.dealer_router.start()

        log.info("Starting the worker router...")
        self.worker_router = WorkerRouter(
                callback=self.cb_worker_router_recv
            )
        self.worker_router.start()

        log.info("Starting the worker controller router...")
        self.worker_controller_router = WorkerControllerRouter(
                callback=self.cb_worker_controller_router_recv
            )
        self.worker_controller_router.start()

        self.run_broker()

    def run_broker(self, *args, **kw):
        while True:
            self._expire_workers()
            self._unlock_jobs()
            self._send_jobs()

            print "Jobs: %d, Workers: %d" % (
                    job.count_by_state(b'PENDING'),
                    worker.count_by_state(b'READY')
                )

            time.sleep(1)

    def _expire_workers(self):
        worker.expire()

    def _send_jobs(self):
        workers = worker.select_by_state(b'READY')

        for _job in job.select_by_state(b'PENDING'):
            if len(workers) < 1:
                break

            _worker = workers[random.randint(0,(len(workers)-1))]
            worker.send_job(_worker.identity, _job.uuid)

            self.worker_controller_router.send_multipart([_worker.identity, b'TAKE', _job.uuid])

            workers = worker.select_by_state(b'READY')

    def _unlock_jobs(self):
        job.unlock()
