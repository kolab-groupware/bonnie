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

class Job(object):
    uuid = None
    state = None
    timestamp = None
    notification = None
    worker_id = None
    client_id = None
    collector_id = None
    command = None

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
