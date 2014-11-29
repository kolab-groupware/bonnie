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
import bonnie
from bonnie.collector import BonnieCollector
from bonnie.collector.handlers import MessageDataHandler, IMAPDataHandler
from twisted.trial import unittest


class TestBonnieCollector(unittest.TestCase):

    def setUp(self):
        # patch bonnie.utils.imap_mailbox_fs_path() to point to local resources folder
        self.patch(bonnie.utils, 'imap_mailbox_fs_path', self._imap_mailbox_fs_path)

    def _imap_mailbox_fs_path(self, uri):
        pwd = os.path.dirname(__file__)
        return os.path.join(pwd, 'resources')

    def test_retrieve_headers_for_messages(self):
        coll = MessageDataHandler()
        notification = { 'uri': 'imap://john.doe@example.org@kolab.example.org/Calendar', 'uidset': '3' }
        notification = json.loads(coll.retrieve_headers_for_messages(json.dumps(notification)))

        self.assertTrue(notification.has_key('messageHeaders'))
        self.assertIsInstance(notification['messageHeaders'], dict)
        self.assertTrue(notification['messageHeaders'].has_key('3'))

        headers = notification['messageHeaders']['3']
        self.assertIsInstance(headers, dict)
        self.assertEqual(headers['Subject'], 'Test')
        self.assertEqual(headers['From'][0], 'John Doe <john.doe@example.org>')

    def test_retrieve_contents_for_messages(self):
        coll = MessageDataHandler()
        notification = { 'uri': 'imap://john.doe@example.org@kolab.example.org/Calendar', 'uidset': '3' }
        notification = json.loads(coll.retrieve_contents_for_messages(json.dumps(notification)))

        self.assertTrue(notification.has_key('messageContent'))
        self.assertIsInstance(notification['messageContent'], dict)
        self.assertTrue(notification['messageContent'].has_key('3'))
        self.assertIsInstance(notification['messageContent']['3'], unicode)

        self.assertTrue(notification.has_key('messageHeaders'))
        self.assertIsInstance(notification['messageHeaders'], dict)
        self.assertTrue(notification['messageHeaders'].has_key('3'))
        self.assertIsInstance(notification['messageHeaders']['3'], dict)

    def test_get_imap_folder_metadata(self):
        coll = IMAPDataHandler()
        notification = { 'uri': 'imap://john.doe@example.org@kolab.example.org/Calendar' }
        notification = json.loads(coll.get_imap_folder_metadata(json.dumps(notification)))

        self.assertTrue(notification.has_key('metadata'))
        self.assertIsInstance(notification['metadata'], dict)
        self.assertEqual(notification['metadata']['/shared/vendor/kolab/folder-type'], 'event')

    def test_get_imap_folder_acl(self):
        coll = IMAPDataHandler()
        notification = { 'uri': 'imap://john.doe@example.org@kolab.example.org/Calendar' }
        notification = json.loads(coll.get_imap_folder_acl(json.dumps(notification)))

        self.assertTrue(notification.has_key('acl'))
        self.assertIsInstance(notification['acl'], dict)
        self.assertEqual(notification['acl']['john.doe@example.org'], 'lrswipkxtecdan')

    def test_zz_execute(self):
        coll = BonnieCollector()

        notification = { 'uri': 'imap://john.doe@example.org@kolab.example.org/Calendar;UID=3' }
        result = json.loads(coll.execute('FETCH', json.dumps(notification)))

        self.assertTrue(result.has_key('messageContent'))
        self.assertTrue(result.has_key('uidset'))
        self.assertEqual(result['uidset'], '3')

        notification['vnd.cmu.oldUidset'] = '3'
        result = json.loads(coll.execute('HEADER', json.dumps(notification)))
        self.assertTrue(result.has_key('messageHeaders'))

        result = json.loads(coll.execute('GETMETADATA', json.dumps(notification)))
        self.assertTrue(result.has_key('metadata'))

        result = json.loads(coll.execute('GETACL', json.dumps(notification)))
        self.assertTrue(result.has_key('acl'))
