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
    Changelog handler for groupware objects
"""
import re
import time
import bonnie

from dateutil.tz import tzutc
from bonnie.worker.handlers import HandlerBase

log = bonnie.getLogger('bonnie.worker.changelog')

# timestamp (* 100) at the year 2010
REVBASE = 94668480000

class ChangelogHandler(HandlerBase):
    events = ['MessageAppend','vnd.cmu.MessageMove']

    def __init__(self, *args, **kw):
        HandlerBase.__init__(self, *args, **kw)

    def register(self, callback):
        kw = { 'callback': self.run }
        interests = dict((event,kw) for event in self.events)
        self.worker = callback(interests)

    def run(self, notification):
        # message notifications require message headers
        if not notification.has_key('messageHeaders'):
            return (notification, [ b"FETCH" ] if notification['event'] == 'MessageAppend' else [ b"HEADER" ])

        # check if this is a groupware object
        object_type = None
        msguid = notification['uidset'] if notification.has_key('uidset') else None
        headers = notification['messageHeaders'][msguid] if notification['messageHeaders'].has_key(msguid) else None
        if headers and headers.has_key('X-Kolab-Type') and headers.has_key('Subject'):
            match = re.match(r"application/x-vnd.kolab.(\w+)", headers['X-Kolab-Type'])
            if match:
                object_type = match.group(1)

        # assign a revision number based on the current time
        if object_type is not None:
            notification['revision'] = int(round(time.time() * 100 - REVBASE))
            # TODO: save object type and UUID in separate fields?
            # These are translated into headers.X-Kolab-Type and headers.Subject by the output module

        log.debug("Object type %r detected in event %r" % (object_type, notification['event']), level=8)

        # TODO: suppress MessageTrash/MessageExpunge events when followed by a MessageAppend for the same object

        return (notification, [])
