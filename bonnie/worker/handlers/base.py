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
conf = bonnie.getConf()

class HandlerBase(object):
    features = []

    def __init__(self, *args, **kw):
        self.features = conf.get('bonnie', 'features')

        if self.features == None:
            self.features = ""

        self.features = [x.strip() for x in self.features.split(',')]

    def register(self, callback):
        interests = {
                self.event: {
                        'callback': self.run
                    }
            }

        self.worker = callback(interests)

    def run(self, notification):
        # resolve user_id from storage
        if notification.has_key('user') and not notification.has_key('user_id'):
            user_data = notification['user_data'] if notification.has_key('user_data') else None
            notification['user_id'] = self.worker.storage.resolve_username(notification['user'], user_data, force=notification.has_key('user_data'))

        # if storage has no entry, fetch user record from collector
        if notification.has_key('user') and notification['user_id'] is None and not notification.has_key('user_data'):
            notification['user_data'] = None  # avoid endless loop if GETUSERDATA fails
            return (notification, [ b"GETUSERDATA" ])

        # don't store user data in notification
        if notification.has_key('user_data'):
            del notification['user_data']

        return (notification, [])
