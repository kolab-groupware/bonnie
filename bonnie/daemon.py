import os
import sys
import signal
import traceback
import bonnie
conf = bonnie.getConf()

class BonnieDaemon(object):
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

        conf.finalize_conf()

    def run(self, *args, **kw):
        """
            The daemon main loop
        """
        pass

    def start(self, *args, **kw):
        """
            Start the daemon
        """
        exitcode = 0
        terminate = True

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
        pass

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
        raise sys.exit(1)

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
