# -*- coding: utf-8 -*-
# Copyright 2010-2013 Kolab Systems AG (http://www.kolabsys.com)
#
# Jeroen van Meeuwen (Kolab Systems) <vanmeeuwen a kolabsys.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 3 or, at your option, any later version
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#

import logging
import threading

from bonnie.logger import Logger
logging.setLoggerClass(Logger)

API_VERSION = 1

def getLogger(name):
    """
        Return the correct logger class.
    """
    logging.setLoggerClass(Logger)

    log = logging.getLogger(name=name.replace(".", "_"))
    return log

log = getLogger('bonnie')

from bonnie.conf import Conf
conf = Conf()

def getConf():
    _data = threading.local()
    if hasattr(_data, 'conf'):
        log.debug(_("Returning thread local configuration"))
        return _data.conf

    return conf


