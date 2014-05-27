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

import elasticsearch
import os
import subprocess
import time
import urllib
import urlparse

import inputs

import bonnie
conf = bonnie.getConf()

class BonnieCollector(object):
    input_interests = {}
    input_modules = {}

    def __init__(self, *args, **kw):
        for _class in inputs.list_classes():
            __class = _class()
            self.input_modules[__class] = __class.register(callback=self.register_input)

    def event_notification(self, notification):
        """
            Our goal is to collect whatever message contents
            for the messages referenced in the notification.
        """
        print "going to run with", notification
        #notification = self.retrieve_messages(notification)
        #self.output(notification)

    def register_input(self, interests):
        self.input_interests = interests

    def run(self):
        input_module = conf.get('bonnie', 'input_module')
        for _input in self.input_modules.keys():
            if _input.name() == input_module:
                _input.run(callback=self.event_notification)

