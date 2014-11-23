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

import os
import socket
import zmq
from zmq.eventloop import ioloop, zmqstream

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.dealer.ZMQOutput')

class ZMQOutput(object):
    def __init__(self, *args, **kw):
        self.context = zmq.Context()

        ioloop.install()

        zmq_broker_address = conf.get('dealer', 'zmq_broker_address')

        if zmq_broker_address == None:
            zmq_broker_address = "tcp://localhost:5570"

        self.dealer = self.context.socket(zmq.DEALER)
        self.dealer.identity = (u"Dealer-%s-%s" % (socket.getfqdn(), os.getpid())).encode('ascii')
        self.dealer.connect(zmq_broker_address)

        self.dealer_stream = zmqstream.ZMQStream(self.dealer)
        self.dealer_stream.on_recv(self.stop)

    def name(self):
        return 'zmq_output'

    def register(self, *args, **kw):
        return self.run

    def run(self, notification):
        log.debug("[%s] Notification received: %r" % (self.dealer.identity, notification), level=9)
        self.dealer.send(notification)

        ioloop.IOLoop.instance().start()

    def stop(self, message, *args, **kw):
        cmd = message[0]

        if not cmd == b'ACK':
            log.error("Unknown cmd %s" % (cmd))

        ioloop.IOLoop.instance().stop()

        self.dealer.close()
        self.context.term()

