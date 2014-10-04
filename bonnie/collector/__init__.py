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

import os
import inputs
import handlers

import bonnie
from bonnie.utils import parse_imap_uri

conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.collector')

class BonnieCollector(object):
    input_interests = {}
    input_modules = {}

    handler_interests = {}
    handler_modules = {}


    def __init__(self, *args, **kw):
        # TODO: read active input module from config collector.input_modules
        for _class in inputs.list_classes():
            module = _class()
            module.register(callback=self.register_input)
            self.input_modules[_class] = module

        # TODO: read active handler module from config collector.handler_modules
        for _class in handlers.list_classes():
            handler = _class()
            handler.register(callback=self.register_handler)
            self.handler_modules[_class] = handler

    def execute(self, command, notification):
        """
            Dispatch collector job to the according handler(s)
        """
        log.debug("Executing collection command %s" % (command), level=8)

        if self.handler_interests.has_key(command):
            for interest in self.handler_interests[command]:
                notification = interest['callback'](notification=notification)

        return notification

    def register_input(self, interests):
        self.input_interests = interests

    def register_handler(self, interests={}):
        for interest,how in interests.iteritems():
            if not self.handler_interests.has_key(interest):
                self.handler_interests[interest] = []

            self.handler_interests[interest].append(how)

    def run(self):
        input_modules = conf.get('collector', 'input_modules').split(',')
        for _input in self.input_modules.values():
            if _input.name() in input_modules:
                _input.run(callback=self.execute, interests=self.handler_interests.keys())

    def terminate(self, *args, **kw):
        for _input in self.input_modules.values():
            if hasattr(_input, 'terminate'):
                _input.terminate()
            else:
                _input.running = False

