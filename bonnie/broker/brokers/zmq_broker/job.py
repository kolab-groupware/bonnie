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

import hashlib
import time
from bonnie.broker.persistence import db, PersistentBase

class Job(PersistentBase):
    __tablename__ = 'jobs'
    # use binary types because ZMQ requires binary strings
    uuid = db.Column(db.LargeBinary(128), primary_key=True)
    type = db.Column(db.String(16))
    state = db.Column(db.String(16))
    timestamp = db.Column(db.Float)
    notification = db.Column(db.LargeBinary)
    worker_id = db.Column(db.String(64))
    client_id = db.Column(db.String(64))
    collector_id = db.Column(db.LargeBinary(64))
    command = db.Column(db.LargeBinary(32))

    def __init__(self, state=None, notification=None, worker_id=None, client_id=None, collector_id=None):
        self.uuid = "%s.%s" % (hashlib.sha224(notification).hexdigest(), time.time())
        self.state = state
        self.notification = notification
        self.worker_id = worker_id
        self.client_id = client_id
        self.collector_id = collector_id
        self.timestamp = time.time()
        self.command = None

        if self.client_id == None:
            if self.collector_id == None:
                self.type = None
            else:
                self.type = 'Collector'
        else:
            self.type = 'Dealer'

    def set_state(self, state):
        self.state = state

    def set_worker(self, worker_id):
        self.worker_id = worker_id

    def set_command(self, cmd):
        self.command = cmd
