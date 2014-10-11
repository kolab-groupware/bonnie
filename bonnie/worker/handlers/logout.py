# -*- coding: utf-8 -*-
# Copyright 2010-2014 Kolab Systems AG (http://www.kolabsys.com)
#
# Jeroen van Meeuwen (Kolab Systems) <vanmeeuwen a kolabsys.com>
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
    Base handler for an event notification of type 'Logout'
"""

import time
import datetime

from dateutil.tz import tzutc
from dateutil.parser import parse
from bonnie.worker.handlers import HandlerBase

class LogoutHandler(HandlerBase):
    event = 'Logout'

    def __init__(self, *args, **kw):
        HandlerBase.__init__(self, *args, **kw)

    def run(self, notification):
        # lookup corresponding Login event and update that record with logout_time
        # and suppress separate logging of this event with notification['_suppress_output'] = True
        if notification.has_key('vnd.cmu.sessionId'):
            now = datetime.datetime.now(tzutc())
            attempts = 4
            while attempts > 0:
                results = self.worker.storage.select(
                    query=[
                        ('event', '=', 'Login'),
                        ('session_id', '=', notification['vnd.cmu.sessionId'])
                    ],
                    index='logstash-*',
                    doctype='logs',
                    fields='user,@timestamp',
                    limit=1
                )
                if results['total'] > 0:
                    login_event = results['hits'][0]

                    try:
                        timestamp = parse(login_event['@timestamp'])
                    except:
                        timestamp = now

                    delta = now - timestamp

                    # update Login event record
                    self.worker.storage.set(
                        key=login_event['_id'],
                        index=login_event['_index'],
                        doctype=login_event['_doctype'],
                        value={
                            'logout_time': datetime.datetime.strftime(now, "%Y-%m-%dT%H:%M:%S.%fZ"),
                            'duration': delta.days * 24 * 3600 + delta.seconds + delta.microseconds / 1e6
                        }
                    )
                    notification['_suppress_output'] = True
                    return (notification, [])

                attempts -= 1
                time.sleep(1) # wait for storage and try again

            # push back into the job queue, the corresponding Login event may not yet have been processed.
            return (notification, [b"PUSHBACK"])

        return super(LogoutHandler, self).run(notification)