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

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.dealer.ZMQOutput')

class ZMQOutput(object):
    def __init__(self, *args, **kw):
        self.context = zmq.Context()

        zmq_broker_address = conf.get('dealer', 'zmq_broker_address')

        if zmq_broker_address == None:
            zmq_broker_address = "tcp://localhost:5570"

        self.dealer = self.context.socket(zmq.DEALER)
        self.dealer.identity = (u"Dealer-%s" % (socket.getfqdn())).encode('ascii')
        self.dealer.connect(zmq_broker_address)

        self.poller = zmq.Poller()
        self.poller.register(self.dealer, zmq.POLLIN)

    def name(self):
        return 'zmq_output'

    def register(self, *args, **kw):
        return self.run

    def run(self, notification):
        log.debug("[%s] Notification received: %r" % (self.dealer.identity, notification), level=9)
        self.dealer.send(notification)

        received_reply = False
        while not received_reply:
            sockets = dict(self.poller.poll(1000))
            if self.dealer in sockets:
                if sockets[self.dealer] == zmq.POLLIN:
                    _reply = self.dealer.recv_multipart()
                    log.debug("[%s] Reply: %r" % (self.dealer.identity, _reply), level=9)
                    if _reply[0] == b"ACK":
                        received_reply = True

        self.dealer.close()
        self.context.term()

