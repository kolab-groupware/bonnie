# -*- coding: utf-8 -*-
# Copyright 2010-2014 Kolab Systems AG (http://www.kolabsys.com)
#
# Thomas Bruederli <bruederli at kolabsys.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 3 or, at your option, any later version
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#

"""
    Base handler for an event notification of type 'Message*'
"""

import bonnie
from bonnie.worker.handlers import HandlerBase

class MessageHandlerBase(HandlerBase):
    event = None

    def __init__(self, *args, **kw):
        HandlerBase.__init__(self, *args, **kw)
        self.log = bonnie.getLogger('bonnie.worker.' + self.event)

    def run(self, notification):
        # message notifications require message headers
        if not notification.has_key('messageHeaders'):
            self.log.debug("Adding HEADER job for " + self.event, level=8)
            return (notification, [ b"HEADER" ])

        return (notification, [])

