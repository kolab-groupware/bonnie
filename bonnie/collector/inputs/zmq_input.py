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

    This is a collector node pulling jobs from a ZMQ broker.

"""

import json
import os
import socket
import time
import zmq

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.collector.ZMQInput')

class ZMQInput(object):
    state = b"READY"
    running = False

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

        self.poller = zmq.Poller()
        self.poller.register(self.collector, zmq.POLLIN)

    def name(self):
        return 'zmq_input'

    def register(self, *args, **kw):
        pass

    def report_state(self, interests=[]):
        log.debug("[%s] Reporting state %s, %r" % (self.identity, self.state, self.interests), level=9)
        self.collector.send_multipart([b"STATE", self.state, " ".join(self.interests)])
        self.report_timestamp = time.time()

    def run(self, callback=None, interests=[]):
        log.info("[%s] starting", self.identity)

        self.running = True

        # report READY state with interests
        self.interests = interests
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

            if self.report_timestamp < (time.time() - 60):
                self.report_state()

            if self.collector in sockets:
                if sockets[self.collector] == zmq.POLLIN:
                    _message = self.collector.recv_multipart()

                    if _message[0] == b"STATE":
                        self.report_state()

                    else:
                        if not self.state == b"READY":
                            self.report_state()

                        else:
                            _job_uuid = _message[1]
                            _notification = _message[2]

                            if not callback == None:
                                result = callback(_message[0], _notification)

                            self.report_timestamp = time.time()
                            self.collector.send_multipart([b"DONE", _job_uuid, result])

        log.info("[%s] shutting down", self.identity)
        self.collector.close()
