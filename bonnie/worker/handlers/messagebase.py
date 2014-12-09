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

import bonnie

from bonnie.utils import parse_imap_uri
from bonnie.worker.handlers import HandlerBase

conf = bonnie.getConf()

class MessageHandlerBase(HandlerBase):
    event = None

    def __init__(self, *args, **kw):
        super(MessageHandlerBase, self).__init__(*args, **kw)
        self.log = bonnie.getLogger('bonnie.worker.' + self.event)

    def run(self, notification):
        # call super for some basic notification processing
        (notification, jobs) = super(MessageHandlerBase, self).run(notification)

        relevant = False

        if 'archive' in self.features:
            relevant = True

        if 'backup' in self.features:
            relevant = True

        if not relevant:
            return (notification, jobs)

        # Insert the URI UID (if it exists) in to uidset for further handlers
        if notification.has_key('uri') and notification.has_key('uidset'):
            uri = parse_imap_uri(notification['uri'])

            if uri.has_key('UID'):
                notification['uidset'] = [ uri['UID'] ]

        # message notifications require message headers
        if not notification.has_key('messageHeaders'):
            self.log.debug("Adding HEADER job for " + self.event, level=8)
            jobs.append(b"HEADER")
            return (notification, jobs)

        return (notification, jobs)

