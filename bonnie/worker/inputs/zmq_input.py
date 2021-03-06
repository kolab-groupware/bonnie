# -*- coding: utf-8 -*-
# Copyright 2010-2014 Kolab Systems AG (http://www.kolabsys.com)
#
# Jeroen van Meeuwen (Kolab Systems) <vanmeeuwen a kolabsys.com>
# Thomas Bruederli (Kolab Systems) <bruederli a kolabsys.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

"""

    This is a worker node pulling jobs from a ZMQ broker.

"""

import os
import random
import socket
import time
import zmq

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.worker.ZMQInput')

class ZMQInput(object):
    running = False

    def __init__(self, *args, **kw):
        self.state = b"READY"
        self.job_uuid = None
        self.lastping = 0
        self.report_timestamp = 0
        self.busy_start = 0

    def name(self):
        return 'zmq_input'

    def register(self, *args, **kw):
        pass

    def report_state(self):
        if self.busy_start < (time.time() - 90):
            self.set_state_ready()

        log.debug("[%s] reporting state: %s" % (self.identity, self.state), level=8)
        message = [b"STATE", self.state]

        if not self.job_uuid == None and self.state == b'BUSY':
            message.append(self.job_uuid)

        self.controller.send_multipart(message)
        self.report_timestamp = time.time()

    def run(self, callback=None, report=None):
        self.identity = u"Worker-%s-%d" % (socket.getfqdn(),os.getpid())
        log.info("[%s] starting", self.identity)

        self.context = zmq.Context()

        zmq_controller_address = conf.get('worker', 'zmq_controller_address')

        if zmq_controller_address == None:
            zmq_controller_address = "tcp://localhost:5572"

        self.controller = self.context.socket(zmq.DEALER)
        self.controller.identity = (self.identity).encode('ascii')
        self.controller.connect(zmq_controller_address)

        zmq_worker_router_address = conf.get('worker', 'zmq_worker_router_address')

        if zmq_worker_router_address == None:
            zmq_worker_router_address = "tcp://localhost:5573"

        self.worker = self.context.socket(zmq.DEALER)
        self.worker.identity = (self.identity).encode('ascii')
        self.worker.connect(zmq_worker_router_address)

        self.poller = zmq.Poller()
        self.poller.register(self.controller, zmq.POLLIN)
        self.poller.register(self.worker, zmq.POLLIN)

        self.running = True
        self.lastping = time.time()
        self.report_state()

        poller_timeout = int(conf.get('worker', 'zmq_poller_timeout', 100))

        while self.running:
            try:
                sockets = dict(self.poller.poll(poller_timeout))
            except KeyboardInterrupt, e:
                log.info("zmq.Poller KeyboardInterrupt")
                break
            except Exception, e:
                log.error("zmq.Poller error: %r", e)
                sockets = dict()

            now = time.time()

            if self.report_timestamp < (time.time() - 10):
                self.report_state()

            if self.controller in sockets:
                if sockets[self.controller] == zmq.POLLIN:
                    _message = self.controller.recv_multipart()
                    log.debug("[%s] Controller message: %r" % (self.identity, _message), level=9)

                    if _message[0] == b"STATE":
                        self.report_state()

                    if _message[0] == b"TAKE":
                        if not self.state == b"READY":
                            self.report_state()

                        else:
                            self.job_uuid = _message[1]
                            self.take_job(self.job_uuid)

            if self.worker in sockets:
                if sockets[self.worker] == zmq.POLLIN:
                    _message = self.worker.recv_multipart()
                    log.debug("[%s] Worker message: %r" % (self.identity, _message), level=9)

                    if _message[0] == "JOB":
                        self.job_uuid = _message[1]

                        # TODO: Sanity checking
                        #if _message[1] == self.job_uuid:
                        #jobs = []
                        if not callback == None:
                            (notification, jobs) = callback(_message[2])
                        else:
                            jobs = []

                        if len(jobs) == 0:
                            self.controller.send_multipart([b"DONE", self.job_uuid])
                        else:
                            log.debug("[%s] Has jobs: %r" % (self.identity, jobs), level=8)

                            if b'POSTPONE' in jobs:
                                self.controller.send_multipart([b'POSTPONE', self.job_uuid])
                            else:
                                self.controller.send_multipart([b'COLLECT', b" ".join(jobs), self.job_uuid, notification])

                        self.set_state_ready()

            if report is not None and self.lastping < (now - random.randint(300,600)):
                report()
                self.lastping = now

        log.info("[%s] shutting down", self.identity)
        self.worker.close()

    def set_state_busy(self, _job_id):
        log.debug("[%s] Set state to BUSY" % (self.identity), level=9)
        self.report_timestamp = time.time()
        self.busy_start = time.time()
        self.state = b"BUSY"
        self.job_uuid = _job_id

    def set_state_ready(self):
        log.debug("[%s] Set state to READY" % (self.identity), level=9)
        self.report_timestamp = time.time()
        self.state = b"READY"
        self.job_uuid = None

    def take_job(self, _job_id):
        log.debug("[%s] Accept job %s" % (self.identity, _job_id), level=9)
        self.set_state_busy(_job_id)
        self.worker.send_multipart([b"GET", _job_id])
