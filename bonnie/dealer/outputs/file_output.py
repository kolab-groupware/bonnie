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
import socket

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.dealer.FileOutput')

class FileOutput(object):
    def __init__(self, *args, **kw):
        self.file_output = conf.get("dealer", "file_path")

    def name(self):
        return 'file_output'

    def register(self, *args, **kw):
        return self.run

    def run(self, notification):
        log.debug("Notification received: %r" % (notification), level=9)
        if self.file_output == None:
            return notification

        fp = open(self.file_output, 'a')
        fp.write("%r\n" % (notification))
        fp.close()
