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
import grp
import pwd
import signal
import traceback

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie')

class BonnieDaemon(object):
    """
        A standard daemon process abstraction layer for Bonnie.

        This class provides the following capabilities for Bonnie
        daemons (through inheritance):

        *   standard command-line options
        *   :func:`dropping privileges <drop_privileges>`
        *   :func:`signal handling <signal_handlers>`
    """
    pidfile = "/var/run/bonnie/bonnie.pid"

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
                default = self.pidfile,
                help    = "Path to the PID file to use."
            )

        daemon_group.add_option(
                "-u",
                "--user",
                dest    = "process_username",
                action  = "store",
                default = "kolab",
                help    = "Run as user USERNAME",
                metavar = "USERNAME"
            )

        daemon_group.add_option(
                "-g",
                "--group",
                dest    = "process_groupname",
                action  = "store",
                default = "kolab",
                help    = "Run as group GROUPNAME",
                metavar = "GROUPNAME"
            )

        conf.finalize_conf()

    def run(self, *args, **kw):
        """
            The daemon main loop. Override this function in a
            :class:`BonnieDaemon` sub-class.
        """
        pass

    def start(self, *args, **kw):
        """
            Start the daemon
        """
        exitcode = 0
        terminate = True

        if conf.fork_mode:
            self.drop_privileges()

        try:
            pid = 1
            if conf.fork_mode:
                pid = daemonize()

            if pid == 0:
                self.write_pid()
                self.signal_handlers()
                self.run(*args, **kw)
            elif not conf.fork_mode:
                self.signal_handlers()
                self.run(*args, **kw)
            else:
                terminate = False

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

        if terminate:
            self.terminate()

        sys.exit(exitcode)

    def terminate(self, *args, **kw):
        """
            Daemon shutdown function
        """
        self.remove_pid()

    def signal_handlers(self):
        """
            Register process signal handlers
        """
        signal.signal(signal.SIGTERM, self.terminate)

    def write_pid(self):
        """
            Write the process ID to the configured pid file
        """
        pid = os.getpid()
        fp = open(conf.pidfile, 'w')
        fp.write("%d\n" % (pid))
        fp.close()

    def remove_pid(self, *args, **kw):
        """
            Remove our PID file.
        """
        if os.access(conf.pidfile, os.R_OK):
            try:
                os.remove(conf.pidfile)
            except:
                pass

        raise SystemExit

    def drop_privileges(self):
        try:
            try:
                (ruid, euid, suid) = os.getresuid()
                (rgid, egid, sgid) = os.getresgid()
            except AttributeError, errmsg:
                ruid = os.getuid()
                rgid = os.getgid()

            if ruid == 0:
                # Means we can setreuid() / setregid() / setgroups()
                if rgid == 0:
                    # Get group entry details
                    try:
                        (
                            group_name,
                            group_password,
                            group_gid,
                            group_members
                        ) = grp.getgrnam(conf.process_groupname)

                    except KeyError:
                        print >> sys.stderr, "Group %s does not exist" % (conf.process_groupname)
                        sys.exit(1)

                    # Set real and effective group if not the same as current.
                    if not group_gid == rgid:
                        log.debug("Switching real and effective group id to %d" % (group_gid), level=8)
                        os.setregid(group_gid, group_gid)

                if ruid == 0:
                    # Means we haven't switched yet.
                    try:
                        (
                            user_name,
                            user_password,
                            user_uid,
                            user_gid,
                            user_gecos,
                            user_homedir,
                            user_shell
                        ) = pwd.getpwnam(conf.process_username)

                    except KeyError:
                        print >> sys.stderr, "User %s does not exist" % (conf.process_username)
                        sys.exit(1)


                    # Set real and effective user if not the same as current.
                    if not user_uid == ruid:
                        log.debug("Switching real and effective user id to %d" % (user_uid), level=8)
                        os.setreuid(user_uid, user_uid)

        except:
            log.error("Could not change real and effective uid and/or gid")


def daemonize():
    """
        This forks the current process into a daemon.
    """
    # do the UNIX double-fork magic, see Stevens' "Advanced
    # Programming in the UNIX Environment" for details (ISBN 0201563177)
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0) # exit first parent.
    except OSError, e:
        print >> sys.stderr, "Fork #1 failed: (%d) %s" % (e.errno, e.strerror)
        sys.exit(1)

    # Decouple from parent environment.
    # os.chdir("/")
    os.umask(0)
    os.setsid()

    # Do second fork.
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0) # exit second parent.
    except OSError, e:
        print >> sys.stderr, "Fork #2 failed: (%d) %s" % (e.errno, e.strerror)
        sys.exit(1)

    return pid
