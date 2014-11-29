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
import json
import time

from twisted.trial import unittest
from subprocess import call
from bonnie.dealer import BonnieDealer
from bonnie.worker.storage import ElasticSearchStorage

pwd = os.path.dirname(__file__)
basedir = os.path.join(pwd, '..', '..')

import bonnie
conf = bonnie.getConf()
conf.finalize_conf()

class TestBonnieFunctional(unittest.TestCase):
    attempts = 12

    def setUp(self):
        self.storage = ElasticSearchStorage()
        self.storage.es.indices.delete(index='logstash-*', ignore=[400, 404])
        self.storage.es.indices.delete(index='objects', ignore=[400, 404])

        call([os.path.join(pwd, 'start.sh'), basedir])
        time.sleep(1)

    def tearDown(self):
        call([os.path.join(pwd, 'stop.sh')])
        time.sleep(2)

    def query_log(self, query):
        attempts = self.attempts
        while attempts > 0:
            attempts -= 1
            res = self.storage.select(query, index='logstash-*', doctype='logs', sortby='@timestamp:desc')
            if res and res['total'] > 0:
                return res['hits']

            time.sleep(1)
            # print "query retry", attempts

        return None

    def storage_get(self, key, index, doctype):
        attempts = self.attempts
        while attempts > 0:
            attempts -= 1
            res = self.storage.get(key, index=index, doctype=doctype)
            if res is not None:
                return res
            time.sleep(1)
            # print "get retry", attempts

        return None

