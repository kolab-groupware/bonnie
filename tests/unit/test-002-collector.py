import os
import json

from bonnie.collector import BonnieCollector
from twisted.trial import unittest


class TestBonnieCollector(unittest.TestCase):

    def setUp(self):
        pass

    def _patch_imap_folder_path(self):
        # patch BonnieCollector.get_imap_folder_path() to point to local resources folder
        self.patch(BonnieCollector, "get_imap_folder_path", self._get_imap_folder_path)

    def _get_imap_folder_path(self, uri):
        pwd = os.path.dirname(__file__)
        return os.path.join(pwd, 'resources')

    def test_get_imap_folder_path(self):
        coll = BonnieCollector()
        path = coll.get_imap_folder_path("imap://john.doe@example.org@kolab.example.org/Calendar/Personal%20Calendar;UID=3")
        self.assertEqual(path, "/var/spool/imap/domain/e/example.org/j/user/john^doe/Calendar/Personal Calendar")

    def test_retrieve_headers_for_messages(self):
        self._patch_imap_folder_path()
        coll = BonnieCollector()
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
        self._patch_imap_folder_path()
        coll = BonnieCollector()
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

    def test_execute(self):
        self._patch_imap_folder_path()
        coll = BonnieCollector()

        notification = { 'uri': 'imap://john.doe@example.org@kolab.example.org/Calendar;UID=3' }
        result = json.loads(coll.execute('FETCH', json.dumps(notification)))

        self.assertTrue(result.has_key('messageContent'))
        self.assertTrue(result.has_key('uidset'))
        self.assertEqual(result['uidset'], '3')

        notification['vnd.cmu.oldUidset'] = '3'
        result = json.loads(coll.execute('HEADER', json.dumps(notification)))
        self.assertTrue(result.has_key('messageHeaders'))
