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

import bonnie
conf = bonnie.getConf()
from bonnie.translate import _

class BonnieWorker(object):
    handler_interests = { '_all': [] }
    input_interests = {}
    storage_interests = {}
    output_interests = {}

    handler_modules = {}
    input_modules = {}
    storage_modules = {}
    output_modules = {}

    def __init__(self, *args, **kw):

        daemon_group = conf.add_cli_parser_option_group(_("Daemon Options"))

        daemon_group.add_option(
                "--fork",
                dest    = "fork_mode",
                action  = "store_true",
                default = False,
                help    = _("Fork to the background.")
            )

        daemon_group.add_option(
                "-p",
                "--pid-file",
                dest    = "pidfile",
                action  = "store",
                default = "/var/run/bonnie/worker.pid",
                help    = _("Path to the PID file to use.")
            )

        conf.finalize_conf()

        for _class in handlers.list_classes():
            __class = _class()
            self.handler_modules[__class] = __class.register(callback=self.register_handler)

        for _class in inputs.list_classes():
            __class = _class()
            self.input_modules[__class] = __class.register(callback=self.register_input)

        for _class in outputs.list_classes():
            __class = _class()
            self.output_modules[__class] = __class.register(callback=self.register_output)

        for _class in storage.list_classes():
            __class = _class()
            self.storage_modules[__class] = __class.register(callback=self.register_storage)

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
#                print self.handler_interests[event]
                (notification, _jobs) = interest['callback'](notification=notification)
                jobs.extend(_jobs)

        for interest in self.handler_interests['_all']:
            (notification, _jobs) = interest['callback'](notification=notification)
            jobs.extend(_jobs)

        if len(jobs) == 0:
            for interest in self.output_interests['_all']:
                (notification, _jobs) = interest['callback'](notification=notification)
                jobs.extend(_jobs)

        print jobs

        return notification, jobs

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

    def register_input(self, interests):
        self.input_interests = interests

    def register_output(self, interests):
        for interest,how in interests.iteritems():
            if not self.output_interests.has_key(interest):
                self.output_interests[interest] = []

            self.output_interests[interest].append(how)

    def register_storage(self, interests):
        self.storage_interests = interests

    def run(self):
        input_modules = conf.get('worker', 'input_modules')
        for _input in self.input_modules.keys():
            if _input.name() == input_modules:
                _input.run(callback=self.event_notification)

