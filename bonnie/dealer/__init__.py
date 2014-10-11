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

import json
import outputs

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.dealer')

class BonnieDealer(object):
    def __init__(self, *args, **kw):
        self.output_modules = {}

        for _class in outputs.list_classes():
            __class = _class()
            self.output_modules[__class] = __class.register(callback=self.register_output)

    def register_output(self, interests):
        pass

    def accept_notification(self, notification):
        parsed = json.loads(notification)
        event = parsed['event']
        user = parsed['user'] if parsed.has_key('user') else None

        blacklist_events = conf.get('dealer', 'blacklist_events').split(',')
        blacklist_users  = conf.get('dealer', 'blacklist_users').split(',')

        # ignore blacklisted events for blacklisted users
        if event in blacklist_events and user is not None and user in blacklist_users:
            return False

        return True

    def run(self, notification):
        if self.accept_notification(notification):
            output_modules = conf.get('dealer', 'output_modules')
            for _output in self.output_modules.keys():
                if _output.name() == output_modules:
                    _output.run(notification)
        else:
            log.info("Ignoring notification %s", notification)
