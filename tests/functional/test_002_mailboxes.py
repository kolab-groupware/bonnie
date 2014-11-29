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

from . import TestBonnieFunctional
from bonnie.dealer import BonnieDealer

import bonnie
conf = bonnie.getConf()

class TestBonnieMailboxes(TestBonnieFunctional):

    def test_mailboxcreate(self):
        dealer = BonnieDealer()

        notification = {
            'event': 'MailboxCreate',
            'user': 'john.doe@example.org',
            'uri': 'imap://john.doe@example.org@kolab.example.org/Calendar;UIDVALIDITY=12345'
        }

        dealer.run(json.dumps(notification))

        events = self.query_log([('event','=','MailboxCreate')])
        self.assertEqual(len(events), 1)

        event = events[0]
        self.assertTrue(event.has_key('folder_id'))
        self.assertTrue(event.has_key('folder_uniqueid'))
        self.assertTrue(event.has_key('user_id'))

        # check objects/folder entry
        folder = self.storage_get(event['folder_id'], index='objects', doctype='folder')

        self.assertIsInstance(folder, dict)
        self.assertEqual(folder['uniqueid'], event['folder_uniqueid'])
        self.assertEqual(folder['name'], 'Calendar')
        self.assertEqual(folder['type'], 'event')
        self.assertEqual(folder['owner'], 'john.doe@example.org')

        self.assertIsInstance(folder['metadata'], dict)
        self.assertIsInstance(folder['acl'], dict)
        self.assertTrue(folder['acl'].has_key(event['user_id']))
        self.assertTrue(folder['acl'][event['user_id']].startswith('lrswi'))

        # check objects/user entry
        user = self.storage_get(event['user_id'], index='objects', doctype='user')

        self.assertIsInstance(user, dict)
        self.assertEqual(user['user'], notification['user'])
        self.assertEqual(user['cn'], 'John Doe')
