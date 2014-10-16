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
import handlers
import inputs
import outputs
import storage

from bonnie.translate import _
from bonnie.daemon import BonnieDaemon

import bonnie
conf = bonnie.getConf()

class BonnieWorker(BonnieDaemon):
    pidfile = "/var/run/bonnie/worker.pid"

    handler_interests = { '_all': [] }
    input_interests = {}
    storage_interests = {}
    output_interests = {}

    handler_modules = {}
    input_modules = {}
    storage_modules = {}
    output_modules = {}

    def __init__(self, *args, **kw):
        super(BonnieWorker, self).__init__(*args, **kw)

        for _class in handlers.list_classes():
            __class = _class()
            __class.register(callback=self.register_handler)
            self.handler_modules[_class] = __class

        for _class in inputs.list_classes():
            __class = _class()
            __class.register(callback=self.register_input)
            self.input_modules[_class] = __class

        output_modules = conf.get('worker', 'output_modules').split(',')
        for _class in outputs.list_classes():
            _output = _class()
            if _output.name() in output_modules:
                _output.register(callback=self.register_output)
                self.output_modules[_class] = _output

        storage_modules = conf.get('worker', 'storage_modules').split(',')
        for _class in storage.list_classes():
            _storage = _class()
            if _storage.name() in storage_modules:
                _storage.register(callback=self.register_storage)
                self.storage_modules[_class] = _storage
                self.storage = _storage

    def event_notification(self, notification):
        """
            Input an event notification in to our process.

            One by one, handlers that have shown an interest will be
            handed over the entire event notification, and are expected
            to return the (new) version of their event notification.
        """

        notification = json.loads(notification)

        event = notification['event']

        jobs = []

        if self.handler_interests.has_key(event):
            for interest in self.handler_interests[event]:
                (notification, _jobs) = self.interest_callback(interest, notification)
                jobs.extend(_jobs)

        for interest in self.handler_interests['_all']:
            (notification, _jobs) = self.interest_callback(interest, notification)
            jobs.extend(_jobs)

        # trigger storage modules which registered interest in particular notification properties
        if len(jobs) == 0:
            for prop,storage_interests in self.storage_interests.iteritems():
                if notification.has_key(prop):
                    for interest in storage_interests:
                        (notification, _jobs) = self.interest_callback(interest, notification)
                        jobs.extend(_jobs)

        # finally send notification to output handlers if no jobs remaining
        if len(jobs) == 0 and not notification.has_key('_suppress_output'):
            if self.output_interests.has_key(event):
                for interest in self.output_interests[event]:
                    (notification, _jobs) = self.interest_callback(interest, notification)
                    jobs.extend(_jobs)

            if not notification.has_key('_suppress_output') and self.output_interests.has_key('_all'):
                for interest in self.output_interests['_all']:
                    (notification, _jobs) = self.interest_callback(interest, notification)
                    jobs.extend(_jobs)

        return notification, list(set(jobs))

    def interest_callback(self, interest, notification):
        """
            Helper method to call an interest callback
        """
        kw = interest['kw'] if interest.has_key('kw') else {}
        kw['notification'] = notification
        return interest['callback'](**kw)

    def register_handler(self, interests={}):
        """
            A handler registers itself with a set of interests, based on
            the event notification type.

            For example, for a handler to subscribe to a 'MessageAppend'
            event notification type, it would register itself with the
            following interests, provided it wants its method 'run' to
            be executed:

                { 'MessageAppend': { 'callback': self.run } }
        """
        for interest,how in interests.iteritems():
            if not self.handler_interests.has_key(interest):
                self.handler_interests[interest] = []

            self.handler_interests[interest].append(how)

        return self

    def register_input(self, interests):
        self.input_interests = interests
        return self

    def register_output(self, interests):
        for interest,how in interests.iteritems():
            if not self.output_interests.has_key(interest):
                self.output_interests[interest] = []

            self.output_interests[interest].append(how)

        return self

    def register_storage(self, interests):
        for interest,how in interests.iteritems():
            if not self.storage_interests.has_key(interest):
                self.storage_interests[interest] = []

            self.storage_interests[interest].append(how)

        return self

    def run(self):
        input_modules = conf.get('worker', 'input_modules').split(',')
        for _input in self.input_modules.values():
            if _input.name() in input_modules:
                _input.run(callback=self.event_notification)

    def terminate(self, *args, **kw):
        for _input in self.input_modules.values():
            if hasattr(_input, 'terminate'):
                _input.terminate()
            else:
                _input.running = False