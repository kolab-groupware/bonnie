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

import os
import sys
import inputs
import handlers
import multiprocessing
from distutils import version

from bonnie.utils import parse_imap_uri
from bonnie.daemon import BonnieDaemon

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.collector')

class BonnieCollector(BonnieDaemon):
    pidfile = "/var/run/bonnie/collector.pid"

    def __init__(self, *args, **kw):
        super(BonnieCollector, self).__init__(*args, **kw)

        self.input_interests = {}
        self.input_modules = {}

        self.handler_interests = {}

        num_threads = int(conf.get('collector', 'num_threads', 5))

        if version.StrictVersion(sys.version[:3]) >= version.StrictVersion("2.7"):
            self.pool = multiprocessing.Pool(num_threads, self._worker_process_start, (), 1)
        else:
            self.pool = multiprocessing.Pool(num_threads, self._worker_process_start, ())

    def _worker_process_start(self, *args, **kw):
        log.debug("Worker process %s initializing" % (multiprocessing.current_process().name), level=1)

    def execute(self, commands, job_uuid, notification):
        """
            Dispatch collector job to the according handler(s)
        """
        log.debug("Executing collection for %s" % (commands), level=8)

        # execute this asynchronously in a child process
        self.pool.apply_async(
            async_execute_handlers,
            (
                commands.split(),
                notification,
                job_uuid
            ),
            callback = self._execute_callback
        )

    def _execute_callback(self, result):
        (notification, job_uuid) = result

        # pass result back to input module(s)
        input_modules = conf.get('collector', 'input_modules').split(',')
        for _input in self.input_modules.values():
            if _input.name() in input_modules:
                _input.callback_done(job_uuid, notification)

    def register_input(self, interests):
        self.input_interests = interests

    def register_handler(self, interests={}):
        for interest,how in interests.iteritems():
            if not self.handler_interests.has_key(interest):
                self.handler_interests[interest] = []

            self.handler_interests[interest].append(how)

    def run(self):
        for _class in inputs.list_classes():
            module = _class()
            module.register(callback=self.register_input)
            self.input_modules[_class] = module

        for _class in handlers.list_classes():
            handler = _class()
            handler.register(callback=self.register_handler)

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

        self.pool.close()


def async_execute_handlers(commands, notification, job_uuid):
    """
        Routine to execute handlers for the given commands and notification

        To be run an an asynchronous child process.
    """
    # register handlers with the interrests again in this subprocess
    handler_interests = {}

    def register_handler(interests={}):
        for interest,how in interests.iteritems():
            if not handler_interests.has_key(interest):
                handler_interests[interest] = []

            handler_interests[interest].append(how)

    for _class in handlers.list_classes():
        handler = _class()
        handler.register(callback=register_handler)

    log.debug("async_execute_handlers %r for job %r" % (commands, job_uuid), level=8)

    for command in commands:
        if handler_interests.has_key(command):
            for interest in handler_interests[command]:
                notification = interest['callback'](notification=notification)

    return (notification, job_uuid)

