# -*- coding: utf-8 -*-
#
# Copyright 2010-2014 Kolab Systems AG (http://www.kolabsys.com)
#
# Jeroen van Meeuwen (Kolab Systems) <vanmeeuwen a kolabsys.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 3 or, at your option, any later
# version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307,
# USA.
#

"""

    This is a worker node pulling jobs from a ZMQ broker.

"""

import os
import socket
import time
import zmq

import bonnie
conf = bonnie.getConf()

class ZMQInput(object):
    def __init__(self, *args, **kw):
        self.identity = u"Worker-%s-%d" % (socket.getfqdn(),os.getpid())

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

        self.state = b"READY"
        self.job_id = None
        self.report_timestamp = time.time()

    def name(self):
        return 'zmq_input'

    def register(self, *args, **kw):
        pass

    def report_state(self):
        print "reporting state", self.state
        self.controller.send_multipart([b"STATE", self.state])
        self.report_timestamp = time.time()

    def run(self, callback=None):
        print "%s starting" % (self.identity)

        self.report_state()

        while True:
            sockets = dict(self.poller.poll(1000))

            print "polled"

            if self.report_timestamp < (time.time() - 60):
                self.report_state()

            if self.controller in sockets:
                if sockets[self.controller] == zmq.POLLIN:
                    _message = self.controller.recv_multipart()
                    print _message

                    if _message[0] == b"STATE":
                        self.report_state()

                    if _message[0] == b"TAKE":
                        if not self.state == b"READY":
                            self.report_state()

                        else:
                            _job_id = _message[1]
                            self.take_job(_job_id)

            if self.worker in sockets:
                if sockets[self.worker] == zmq.POLLIN:
                    _message = self.worker.recv_multipart()
                    print _message

                    if _message[0] == "JOB":
                        # TODO: Sanity checking
                        #if _message[1] == self.job_id:
                        if not callback == None:
                            (status, jobs) = callback(_message[2])

                        if len(jobs) == 0:
                            self.controller.send_multipart([b"DONE", _message[1]])
                        else:
                            print "I have jobs:", jobs

                        for job in jobs:
                            self.controller.send_multipart([job, _message[1]])

                        self.set_state_ready()

        self.worker.close()

    def set_state_busy(self):
        #print "set state to busy"
        self.controller.send_multipart([b"STATE", b"BUSY", b"%s" % (self.job_id)])
        self.state = b"BUSY"

    def set_state_ready(self):
        #print "set state to ready"
        self.controller.send_multipart([b"STATE", b"READY"])
        self.state = b"READY"
        self.job_id = None

    def take_job(self, _job_id):
        print "taking job", _job_id
        self.set_state_busy()
        self.worker.send_multipart([b"GET", _job_id])
        self.job_id = _job_id

