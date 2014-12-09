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
    Base handler for an event notification of type 'MessageNew'
"""

import bonnie
from bonnie.worker.handlers import MessageAppendHandler

conf = bonnie.getConf()

class MessageNewHandler(MessageAppendHandler):
    event = 'MessageNew'

    def __init__(self, *args, **kw):
        super(MessageNewHandler, self).__init__(*args, **kw)

    def run(self, notification):
        # call super for some basic notification processing
        (notification, jobs) = super(MessageAppendHandler, self).run(notification)

        relevant = False

        if 'archive' in self.features:
            relevant = True

        if 'backup' in self.features:
            relevant = True

        if not relevant:
            return (notification, jobs)

        if not notification.has_key('messageContent') or notification['messageContent'] in [None, ""]:
            self.log.debug("Adding FETCH job for " + self.event, level=8)
            return (notification, [ b"FETCH" ])

        return (notification, jobs)

