# -*- coding: utf-8 -*-
# Copyright 2010-2014 Kolab Systems AG (http://www.kolabsys.com)
#
# Jeroen van Meeuwen (Kolab Systems) <vanmeeuwen a kolabsys.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 3 or, at your option, any later
# version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Library General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307,
# USA.
#

"""
    This is the broker for Bonnie.
"""
import sys, os
import signal
import traceback
import brokers

from bonnie.utils import daemonize

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.broker')

class BonnieBroker(object):
    broker_interests = {}
    broker_modules = {}

    def __init__(self, *args, **kw):
        daemon_group = conf.add_cli_parser_option_group("Daemon Options")

        daemon_group.add_option(
                "--fork",
                dest    = "fork_mode",
                action  = "store_true",
                default = False,
                help    = "Fork to the background."
            )

        daemon_group.add_option(
                "-p",
                "--pid-file",
                dest    = "pidfile",
                action  = "store",
                default = "/var/run/bonnie/broker.pid",
                help    = "Path to the PID file to use."
            )

        conf.finalize_conf()

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
        exitcode = 0
        terminate = True

        try:
            pid = 1
            if conf.fork_mode:
                pid = daemonize()

            if pid == 0:
                log.remove_stdout_handler()
                signal.signal(signal.SIGTERM, self.terminate)
                self.write_pid()
                self.do_broker()
            elif not conf.fork_mode:
                self.do_broker()

        except SystemExit, errcode:
            terminate = False
            exitcode = errcode

        except KeyboardInterrupt:
            exitcode = 1
            log.info("Interrupted by user")

        except (AttributeError, TypeError) as errmsg:
            exitcode = 1
            traceback.print_exc()
            print >> sys.stderr, "Traceback occurred, please report a " + \
                "bug at https://issues.kolab.org"

        except:
            exitcode = 2
            traceback.print_exc()
            print >> sys.stderr, "Traceback occurred, please report a " + \
                "bug at https://issues.kolab.org"

        if terminate:shutdown
            self.terminate()

        sys.exit(exitcode)

    def do_broker(self):
        for interest, hows in self.broker_interests.iteritems():
            for how in hows:
                how()

    def terminate(self, *args, **kw):
        for module in self.broker_modules.values():
            if hasattr(module, 'terminate'):
                module.terminate()
            else:
                module.running = False

    def write_pid(self):
        pid = os.getpid()
        fp = open(conf.pidfile, 'w')
        fp.write("%d\n" % (pid))
        fp.close()
