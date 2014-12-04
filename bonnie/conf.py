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

import logging
import os
from optparse import OptionParser
from ConfigParser import SafeConfigParser

import bonnie
log = bonnie.getLogger('bonnie.conf')
from bonnie.translate import _

class Conf(object):
    cfg_parser = None

    def __init__(self):
        self.create_options()

        self.config = SafeConfigParser()
        if os.path.exists('/etc/bonnie/bonnie.conf'):
            self.config.read('/etc/bonnie/bonnie.conf')
        elif os.path.exists(os.path.abspath(os.path.dirname(__file__) + '/../conf/bonnie.conf')):
            self.config.read(os.path.abspath(os.path.dirname(__file__) + '/../conf/bonnie.conf'))

        self.defaults = Defaults()

    def add_cli_parser_option_group(self, name):
        return self.cli_parser.add_option_group(name)

    def check_config(self, val=None):
        """
            Checks self.config_file or the filename passed using 'val'
            and returns a SafeConfigParser instance if everything is OK.
        """

        if not val == None:
            config_file = val
        else:
            config_file = self.config_file

        if not os.access(config_file, os.R_OK):
            log.error(_("Configuration file %s not readable") % config_file)

        config = SafeConfigParser()
        log.debug(_("Reading configuration file %s") % config_file, level=8)
        try:
            config.read(config_file)
        except:
            log.error(_("Invalid configuration file %s") % config_file)

        if not config.has_section("bonnie"):
            log.warning(_("No master configuration section [bonnie] in configuration file %s") % config_file)

        return config

    def create_options(self):
        """
            Create the OptionParser for the options passed to us from runtime
            Command Line Interface.
        """

        # Enterprise Linux 5 does not have an "epilog" parameter to OptionParser
        try:
            self.cli_parser = OptionParser(epilog=epilog)
        except:
            self.cli_parser = OptionParser()

        ##
        ## Runtime Options
        ##
        runtime_group = self.cli_parser.add_option_group(_("Runtime Options"))
        runtime_group.add_option(   "-c", "--config",
                                    dest    = "config_file",
                                    action  = "store",
                                    default = "/etc/bonnie/bonnie.conf",
                                    help    = _("Configuration file to use"))

        runtime_group.add_option(   "-d", "--debug",
                                    dest    = "debuglevel",
                                    type    = 'int',
                                    default = 0,
                                    help    = _("Set the debugging " + \
                                        "verbosity. Maximum is 9, tracing " + \
                                        "protocols like LDAP, SQL and IMAP."))

        runtime_group.add_option(   "-l",
                                    dest    = "loglevel",
                                    type    = 'str',
                                    default = "CRITICAL",
                                    help    = _("Set the logging level. " + \
                                        "One of info, warn, error, " + \
                                        "critical or debug"))

        runtime_group.add_option(   "--logfile",
                                    dest    = "logfile",
                                    action  = "store",
                                    default = "/var/log/bonnie/worker.log",
                                    help    = _("Log file to use"))

        runtime_group.add_option(   "-q", "--quiet",
                                    dest    = "quiet",
                                    action  = "store_true",
                                    default = False,
                                    help    = _("Be quiet."))

    def finalize_conf(self,fatal=True):
        self.parse_options(fatal=fatal)

        # The defaults can some from;
        # - a file we ship with the packages
        # - a customly supplied file (by customer)
        # - a file we write out
        # - this python class

        # This is where we check our parser for the defaults being set there.
        self.set_defaults_from_cli_options()

        self.options_set_from_config()

        # Also set the cli options
        if hasattr(self,'cli_keywords') and not self.cli_keywords == None:
            for option in self.cli_keywords.__dict__.keys():
                retval = False
                if hasattr(self, "check_setting_%s" % (option)):
                    exec("retval = self.check_setting_%s(%r)" % (option, self.cli_keywords.__dict__[option]))

                    # The warning, error or confirmation dialog is in the check_setting_%s() function
                    if not retval:
                        continue

                    log.debug(_("Setting %s to %r (from CLI, verified)") % (option, self.cli_keywords.__dict__[option]), level=8)
                    setattr(self,option,self.cli_keywords.__dict__[option])
                else:
                    log.debug(_("Setting %s to %r (from CLI, not checked)") % (option, self.cli_keywords.__dict__[option]), level=8)
                    setattr(self,option,self.cli_keywords.__dict__[option])

    def get(self, section, key, default=None, quiet=False):
        """
            Get a configuration option from our store, the configuration file,
            or an external source if we have some sort of function for it.

            TODO: Include getting the value from plugins through a hook.
        """
        retval = False

        if not self.cfg_parser:
            self.read_config()

        #log.debug(_("Obtaining value for section %r, key %r") % (section, key), level=8)

        if self.cfg_parser.has_option(section, key):
            try:
                return self.cfg_parser.get(section, key)
            except:
                self.read_config()
                return self.cfg_parser.get(section, key)

        if not quiet:
            log.warning(_("Option %s/%s does not exist in config file %s, pulling from defaults") % (section, key, self.config_file))
            if hasattr(self.defaults, "%s_%s" % (section,key)):
                return getattr(self.defaults, "%s_%s" % (section,key))
            elif hasattr(self.defaults, "%s" % (section)):
                if key in getattr(self.defaults, "%s" % (section)):
                    _dict = getattr(self.defaults, "%s" % (section))
                    return _dict[key]
                else:
                    log.warning(_("Option does not exist in defaults."))
            else:
                log.warning(_("Option does not exist in defaults."))

        return default

    def load_config(self, config):
        """
            Given a SafeConfigParser instance, loads a configuration
            file and checks, then sets everything it can find.
        """
        for section in self.defaults.__dict__.keys():
            if not config.has_section(section):
                continue

            for key in self.defaults.__dict__[section].keys():
                retval = False
                if not config.has_option(section, key):
                    continue

                if isinstance(self.defaults.__dict__[section][key], int):
                    value = config.getint(section,key)
                elif isinstance(self.defaults.__dict__[section][key], bool):
                    value = config.getboolean(section,key)
                elif isinstance(self.defaults.__dict__[section][key], str):
                    value = config.get(section,key)
                elif isinstance(self.defaults.__dict__[section][key], list):
                    value = eval(config.get(section,key))
                elif isinstance(self.defaults.__dict__[section][key], dict):
                    value = eval(config.get(section,key))

                if hasattr(self,"check_setting_%s_%s" % (section,key)):
                    exec("retval = self.check_setting_%s_%s(%r)" % (section,key,value))
                    if not retval:
                        # We just don't set it, check_setting_%s should have
                        # taken care of the error messages
                        continue

                if not self.defaults.__dict__[section][key] == value:
                    if key.count('password') >= 1:
                        log.debug(_("Setting %s_%s to '****' (from configuration file)") % (section,key), level=8)
                    else:
                        log.debug(_("Setting %s_%s to %r (from configuration file)") % (section,key,value), level=8)
                    setattr(self,"%s_%s" % (section,key),value)

    def options_set_from_config(self):
        """
            Sets the default configuration options from a
            configuration file. Configuration file may be
            customized using the --config CLI option
        """

        log.debug(_("Setting options from configuration file"), level=4)

        # Check from which configuration file we should get the defaults
        # Other then default?
        self.config_file = "/etc/bonnie/bonnie.conf"

        if hasattr(self,'cli_keywords') and not self.cli_keywords == None:
            if not self.cli_keywords.config_file == "/etc/bonnie/bonnie.conf":
                self.config_file = self.cli_keywords.config_file

        config = self.check_config()
        self.load_config(config)

    def parse_options(self, fatal=True):
        """
            Parse options passed to our call.
        """

        if fatal:
            (self.cli_keywords, self.cli_args) = self.cli_parser.parse_args()

    def read_config(self, value=None):
        """
            Reads the configuration file, sets a self.cfg_parser.
        """

        if not value:
            value = self.defaults.config_file

            if hasattr(self, 'cli_keywords') and not self.cli_keywords == None:
                    value = self.cli_keywords.config_file

        self.cfg_parser = SafeConfigParser()
        self.cfg_parser.read(value)

        if hasattr(self, 'cli_keywords') and hasattr(self.cli_keywords, 'config_file'):
            self.cli_keywords.config_file = value
        self.defaults.config_file = value
        self.config_file = value

    def set_defaults_from_cli_options(self):
        for long_opt in self.cli_parser.__dict__['_long_opt'].keys():
            if long_opt == "--help":
                continue
            setattr(self,self.cli_parser._long_opt[long_opt].dest,self.cli_parser._long_opt[long_opt].default)

        # But, they should be available in our class as well
        for option in self.cli_parser.defaults.keys():
            log.debug(_("Setting %s to %r (from the default values for CLI options)") % (option, self.cli_parser.defaults[option]), level=8)
            setattr(self,option,self.cli_parser.defaults[option])

class Defaults(object):
    def __init__(self):
        self.loglevel = logging.CRITICAL
        self.config_file = "/etc/bonnie/bonnie.conf"

