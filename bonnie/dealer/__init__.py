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

import outputs

import bonnie
conf = bonnie.getConf()

class BonnieDealer(object):
    output_modules = {}

    def __init__(self, *args, **kw):
        for _class in outputs.list_classes():
            __class = _class()
            self.output_modules[__class] = __class.register(callback=self.register_output)

    def register_output(self, interests):
        self.output_interests = interests

    def run(self, notification):
        output_modules = conf.get('dealer', 'output_modules')
        for _output in self.output_modules.keys():
            if _output.name() == output_modules:
                _output.run(notification)
