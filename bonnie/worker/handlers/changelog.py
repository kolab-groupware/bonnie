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
    Changelog handler for groupware objects
"""
import re
import time
import bonnie

from bonnie.worker.handlers import HandlerBase

log = bonnie.getLogger('bonnie.worker.changelog')

# timestamp (* 10) at the year 2014
REVBASE = 13885344000


class ChangelogHandler(HandlerBase):
    events = ['MessageAppend', 'vnd.cmu.MessageMove']

    def __init__(self, *args, **kw):
        HandlerBase.__init__(self, *args, **kw)

    def register(self, callback):
        kw = {'callback': self.run}
        interests = dict((event, kw) for event in self.events)
        self.worker = callback(interests)

    def run(self, notification):
        # message notifications require message headers
        if not notification.has_key('messageHeaders'):
            if notification['event'] == 'MessageAppend':
                return (notification, [b"FETCH"])
            else:
                return (notification, [b"HEADER"])

        # check if this is a groupware object
        object_type = None

        if notification.has_key('uidset'):
            msguid = notification['uidset']
        else:
            msguid = None

        # TODO: May need to iterate over message UIDs
        if isinstance(msguid, list):
            msguid = msguid.pop()

        if notification['messageHeaders'].has_key(msguid):
            headers = notification['messageHeaders'][msguid]
        else:
            headers = None

        if headers:
            if headers.has_key('X-Kolab-Type') and headers.has_key('Subject'):
                match = re.match(
                    r"application/x-vnd.kolab.(\w+)",
                    headers['X-Kolab-Type']
                )

                if match:
                    object_type = match.group(1)

        # assign a revision number based on the current time
        if object_type is not None:
            notification['revision'] = int(round(time.time() * 10 - REVBASE))

            # TODO: save object type and UUID in separate fields?
            # These are translated into headers.X-Kolab-Type and
            # headers.Subject by the output module

        log.debug(
            "Object type %r detected in event %r" % (
                object_type,
                notification['event']
            ),
            level=8
        )

        # TODO: suppress MessageTrash/MessageExpunge events when followed by a
        # MessageAppend for the same object

        return (notification, [])

