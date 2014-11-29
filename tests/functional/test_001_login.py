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

from . import TestBonnieFunctional
from bonnie.dealer import BonnieDealer

import bonnie
conf = bonnie.getConf()

class TestBonnieLogin(TestBonnieFunctional):

    def test_001_login(self):
        login = {
            'event': 'Login',
            'user': 'john.doe@example.org',
            'vnd.cmu.sessionId': 'kolab-sess-test-12345',
            'clientIP': '::1',
            'serverDomain': 'example.org',
        }

        dealer = BonnieDealer()
        dealer.run(json.dumps(login))

        events = self.query_log([('event','=','Login')])
        self.assertEqual(len(events), 1)

        event = events[0]
        self.assertTrue(event.has_key('user_id'))
        self.assertTrue(event['session_id'], login['vnd.cmu.sessionId'])
        self.assertEqual(event['@version'], bonnie.API_VERSION)

        del dealer
        time.sleep(1)

        logout = {
            'event': 'Logout',
            'user': 'john.doe@example.org',
            'vnd.cmu.sessionId': 'kolab-sess-test-12345',
            'clientIP': '::1'
        }

        dealer = BonnieDealer()
        dealer.run(json.dumps(logout))

        events = self.query_log([('event','=','Login'), ('logout_time','=','*')])
        self.assertEqual(len(events), 1)

        event = events[0]
        self.assertTrue(event.has_key('logout_time'))
        self.assertTrue(event.has_key('duration'))

        # check objects/users entry
        user = self.storage_get(event['user_id'], index='objects', doctype='user')

        self.assertIsInstance(user, dict)
        self.assertEqual(user['user'], login['user'])
        self.assertEqual(user['cn'], 'John Doe')
