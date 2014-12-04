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
    This is the broker for Bonnie.
"""
import brokers

from bonnie.daemon import BonnieDaemon

class BonnieBroker(BonnieDaemon):
    """

    """
    pidfile = "/var/run/bonnie/broker.pid"

    def __init__(self, *args, **kw):
        super(BonnieBroker, self).__init__(*args, **kw)

        #: Interests are registered here.
        self.broker_interests = {}

        self.broker_modules = {}

        for _class in brokers.list_classes():
            module = _class()
            module.register(callback=self.register_broker)
            self.broker_modules[_class] = module

    def register_broker(self, interests):
        """
            Register a broker based on interests
        """

        for interest,how in interests.iteritems():
            if not self.broker_interests.has_key(interest):
                self.broker_interests[interest] = []

            self.broker_interests[interest].append(how)

    def run(self, *args, **kw):
        for interest, hows in self.broker_interests.iteritems():
            for how in hows:
                how()

    def terminate(self, *args, **kw):
        for module in self.broker_modules.values():
            if hasattr(module, 'terminate'):
                module.terminate()
            else:
                module.running = False
