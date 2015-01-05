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

    This is a collector node pulling jobs from a ZMQ broker.

"""

import datetime
import json
import os
import socket
import time
import zmq
from zmq.eventloop import ioloop, zmqstream

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.collector.ZMQInput')

class ZMQInput(object):
    report_timestamp = 0
    state = b"READY"

    def __init__(self, *args, **kw):
        self.interests = []
        self.context = zmq.Context()

        zmq_broker_address = conf.get('collector', 'zmq_broker_address')

        if zmq_broker_address == None:
            zmq_broker_address = "tcp://localhost:5571"

        self.identity = u"Collector-%s" % (socket.getfqdn())
        self.collector = self.context.socket(zmq.DEALER)
        self.collector.identity = (self.identity).encode('ascii')
        self.collector.connect(zmq_broker_address)

        self.stream = zmqstream.ZMQStream(self.collector)
        self.ioloop = ioloop.IOLoop.instance()

    def name(self):
        return 'zmq_input'

    def register(self, *args, **kw):
        pass

    def report_state(self, new_timeout=False):
        log.debug("[%s] Reporting state %s, %r" % (self.identity, self.state, self.interests), level=9)

        if self.report_timestamp < (time.time() - 10):
            self.collector.send_multipart([b"STATE", self.state, " ".join(self.interests)])
            self.report_timestamp = time.time()

        if new_timeout:
            self.ioloop.add_timeout(datetime.timedelta(seconds=10), self.report_state_with_timeout)

    def report_state_with_timeout(self):
        self.report_state(new_timeout=True)

    def run(self, callback=None, interests=[]):
        log.info("[%s] starting", self.identity)

        # report READY state with interests
        self.interests = interests
        self.notify_callback = callback

        self.stream.on_recv(self._cb_on_recv_multipart)
        self.ioloop.add_timeout(datetime.timedelta(seconds=10), self.report_state_with_timeout)
        self.ioloop.start()

    def _cb_on_recv_multipart(self, message):
        """
            Receive a message on the Collector Router.
        """
        log.debug("Collector Router Message: %r" % (message), level=8)
        cmd = message[0]

        if cmd == b"STATE":
            self.report_state()
        elif self.state == b'BUSY':
            self.report_state()
        else:
            job_uuid = message[1]
            notification = message[2]

            if not self.notify_callback == None:
                self.notify_callback(cmd, job_uuid, notification)

    def callback_done(self, job_uuid, result, threads = 0):
        log.debug("Handler callback done for job %s: %r" % (job_uuid, result), level=8)
        log.debug("Threads available: %d" % (threads), level=8)

        if threads > 0:
            self.state = b'READY'
        else:
            self.state = b'BUSY'

        self.collector.send_multipart([b"DONE", job_uuid, result])
        log.info("Job %s DONE by %s" % (job_uuid, self.identity))

    def terminate(self):
        log.info("[%s] shutting down", self.identity)
        ioloop.IOLoop.instance().stop()
        self.collector.close()
