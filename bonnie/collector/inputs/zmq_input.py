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

class ZMQInput(object):
    def __init__(self, *args, **kw):
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

    def run(self, callback=None):
        print "running"
        while True:
            sockets = dict(self.poller.poll(1000))

            self.collector.send_multipart([b"READY"])

            if self.collector in sockets:
                if sockets[self.collector] == zmq.POLLIN:
                    _message = self.collector.recv_multipart()
                    _job_uuid = _message[0]
                    _notification = _message[1]

                    if not callback == None:
                        result = callback(_notification)

                    self.collector.send_multipart([b"DONE", _job_uuid, result])

        self.collector.close()
