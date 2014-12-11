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

import json
import time
import handlers
import inputs
import outputs
import storage
import signal

from bonnie.translate import _
from bonnie.daemon import BonnieDaemon
from multiprocessing import Process

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.worker')

class BonnieWorker(BonnieDaemon):
    """
        Bonnie Worker specific version of a :class:`Bonnie Daemon <bonnie.daemon.BonnieDaemon>`
    """

    #: The process ID file to use.
    pidfile = "/var/run/bonnie/worker.pid"

    def __init__(self, *args, **kw):
        worker_group = conf.add_cli_parser_option_group("Worker Options")

        worker_group.add_option(
                "-n",
                "--num-childs",
                dest    = "num_childs",
                action  = "store",
                default = None,
                help    = "Number of child processes to spawn"
            )

        super(BonnieWorker, self).__init__(*args, **kw)

        self.childs = []
        self.manager = False
        self.running = False

    def run(self):
        """
            The Bonnie worker main loop, responsible for creating
            :class:`worker processes
            <bonnie.worker.BonnieWorkerProcess>`
        """
        num_childs = conf.num_childs or conf.get('worker', 'num_childs')
        if num_childs is not None:
            num_childs = int(num_childs)

        if num_childs is None or num_childs < 1:
            main = BonnieWorkerProcess()
            self.childs.append(main)
            main.run()  # blocking
        else:
            conf.fork_mode = False
            self.manager = True
            self.running = True

            while self.running:
                # (re)start child worker processes
                while len(self.childs) < num_childs:
                    p = Process(target=self.run_child)
                    self.childs.append(p)
                    p.start()

                # check states of child processes
                for p in self.childs:
                    if not p.is_alive():
                        log.info("Restarting dead worker process %r", p.pid)
                        self.childs.remove(p)

                time.sleep(10)

        log.info("Shutting down worker manager")

    def run_child(self):
        """
            Target method for :class:`worker processes
            <bonnie.worker.BonnieWorkerProcess>`
        """
        BonnieWorkerProcess(as_child=True).run()

    def terminate(self, *args, **kw):
        """
            Stop the worker daemon.
        """
        self.running = False
        for p in self.childs:
            p.terminate()

        if self.manager:
            for p in self.childs:
                p.join()

class BonnieWorkerProcess(object):
    #: Holder of interests registered by handlers
    handler_interests = { '_all': [] }

    #: Holder of interests registered by input channel modules
    input_interests = {}

    #: Holder of interests registered by storage channel modules
    storage_interests = {}

    #: Holder of interests registered by output channel modules
    output_interests = {}

    #: Lists registered handler modules
    handler_modules = {}
    #: Lists registered input modules
    input_modules = {}
    #: Lists registered storage modules
    storage_modules = {}
    #: Lists registered output modules
    output_modules = {}

    output_exclude_events = []
    def __init__(self, as_child=False, *args, **kw):
        if as_child:
            signal.signal(signal.SIGTERM, self.terminate)

        for _class in handlers.list_classes():
            __class = _class()
            __class.register(callback=self.register_handler)
            self.handler_modules[_class] = __class

        for _class in inputs.list_classes():
            __class = _class()
            __class.register(callback=self.register_input)
            self.input_modules[_class] = __class

        output_modules = [x.strip() for x in conf.get('worker', 'output_modules', '').split(',')]

        for _class in outputs.list_classes():
            _output = _class()
            if _output.name() in output_modules:
                _output.register(callback=self.register_output)
                self.output_modules[_class] = _output

        storage_modules = [x.strip() for x in conf.get('worker', 'storage_modules', '').split(',')]
        for _class in storage.list_classes():
            _storage = _class()
            if _storage.name() in storage_modules:
                _storage.register(callback=self.register_storage)
                self.storage_modules[_class] = _storage
                self.storage = _storage

        output_exclude_events = conf.get('worker', 'output_exclude_events', '')

        self.output_exclude_events = [x.strip() for x in output_exclude_events.split(',')]

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
                (notification, _jobs) = self.interest_callback(
                        interest,
                        notification
                    )

                if len(_jobs) > 0:
                    log.debug(
                            "Handler interest %r for event %s returns jobs: %r" % (
                                    interest,
                                    event,
                                    _jobs
                                ),
                            level = 6
                        )

                    jobs.extend(_jobs)

        for interest in self.handler_interests['_all']:
            (notification, _jobs) = self.interest_callback(
                    interest,
                    notification
                )

            if len(_jobs) > 0:
                log.debug(
                        "Handler interest %r for _all returns jobs: %r" % (
                                interest,
                                _jobs
                            ),
                        level = 6
                    )

                jobs.extend(_jobs)

        jobs = list(set(jobs))

        if len(jobs) > 0:
            log.debug(
                    "Handler interests included jobs: %r" % (jobs),
                    level = 6
                )

        handler_jobs_num = len(jobs)

        # trigger storage modules which registered interest in
        # particular notification properties.
        for prop,storage_interests in self.storage_interests.iteritems():
            if notification.has_key(prop):
                for interest in storage_interests:
                    log.debug(
                            "Storage interest %s for property %s to be executed" % (
                                    interest,
                                    prop
                                ),
                            level = 6
                        )

                    (notification, _jobs) = self.interest_callback(
                            interest,
                            notification
                        )

                    if len(_jobs) > 0:
                        log.debug(
                                "Storage interest %r for %r returns jobs: %r" % (
                                        interest,
                                        prop,
                                        _jobs
                                    ),
                                level = 6
                            )

                        jobs.extend(_jobs)

            else:
                log.debug(
                        "Storage interests for property %s not (yet) relevant" % (
                                prop
                            ),
                        level = 6
                    )

        jobs = list(set(jobs))

        if len(jobs) > handler_jobs_num:
            log.debug(
                    "Storage interests included additional jobs: %r" % (
                            jobs
                        ),
                    level = 6
                )

        # finally send notification to output handlers if no jobs remaining
        if len(jobs) == 0 and not notification.has_key('_suppress_output') and not event in self.output_exclude_events:
            if self.output_interests.has_key(event):
                for interest in self.output_interests[event]:
                    (notification, _jobs) = self.interest_callback(interest, notification)
                    jobs.extend(_jobs)

            if not notification.has_key('_suppress_output') and self.output_interests.has_key('_all'):
                for interest in self.output_interests['_all']:
                    (notification, _jobs) = self.interest_callback(interest, notification)
                    jobs.extend(_jobs)

        log.debug("Event notification %r processed. Jobs = %r" % (event, jobs), level=7)

        return json.dumps(notification), list(set(jobs))

    def input_report(self):
        """
            Periodic callbacks from the input main loop.
            Forward to all handler, storage and output modules
        """
        for _handler in self.handler_modules.values():
            if hasattr(_handler, 'report'):
                _handler.report()

        for _storage in self.storage_modules.values():
            if hasattr(_storage, 'report'):
                _storage.report()

        for _output in self.output_modules.values():
            if hasattr(_output, 'report'):
                _output.report()

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

            For example, for a handler to subscribe to a *MessageAppend*
            event notification type, it would register itself with the
            following interests, provided it wants its method ``run`` to
            be executed::

                { 'MessageAppend': { 'callback': self.run } }

        """
        for interest,how in interests.iteritems():
            if not self.handler_interests.has_key(interest):
                self.handler_interests[interest] = []

            self.handler_interests[interest].append(how)

        return self

    def register_input(self, interests={}):
        self.input_interests = interests
        return self

    def register_output(self, interests={}):
        """
            Output channel module registration.
        """
        for interest,how in interests.iteritems():
            if not self.output_interests.has_key(interest):
                self.output_interests[interest] = []

            self.output_interests[interest].append(how)

        return self

    def register_storage(self, interests={}):
        """
            Storage channel module registration.
        """
        for interest,how in interests.iteritems():
            if not self.storage_interests.has_key(interest):
                self.storage_interests[interest] = []

            self.storage_interests[interest].append(how)

        return self

    def run(self):
        input_modules = conf.get('worker', 'input_modules')
        if input_modules == None:
            log.error("No input modules configured for worker")
            return

        input_modules = [x.strip() for x in input_modules.split(',')]

        for _input in self.input_modules.values():
            if _input.name() in input_modules:
                _input.run(callback=self.event_notification, report=self.input_report)

    def terminate(self, *args, **kw):
        for _input in self.input_modules.values():
            if hasattr(_input, 'terminate'):
                _input.terminate()
            else:
                _input.running = False
