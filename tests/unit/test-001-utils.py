import os
import json
from bonnie.utils import expand_uidset
from bonnie.utils import parse_imap_uri
from bonnie.utils import mail_message2dict
from bonnie.utils import decode_message_headers
from bonnie.utils import imap_folder_path
from bonnie.utils import imap_mailbox_fs_path
from email import message_from_string
from twisted.trial import unittest


class TestBonnieUtils(unittest.TestCase):

    def setUp(self):
        pass

    def _get_resource(self, filename):
        pwd = os.path.dirname(__file__)
        filepath = os.path.join(pwd, 'resources', filename)
        fp = open(filepath, 'r')
        data = fp.read()
        fp.close()
        return data

    def test_expand_uidset(self):
        self.assertEqual(expand_uidset('3'), ['3'])
        self.assertEqual(expand_uidset('3,5'), ['3','5'])
        self.assertEqual(expand_uidset('3:5'), ['3','4','5'])

    def test_parse_imap_uri(self):
        url = parse_imap_uri("imap://john.doe@example.org@kolab.example.org/Calendar/Personal%20Calendar;UIDVALIDITY=1411487702/;UID=3")
        self.assertEqual(url['host'],   'kolab.example.org')
        self.assertEqual(url['user'],   'john.doe')
        self.assertEqual(url['domain'], 'example.org')
        self.assertEqual(url['path'],   'Calendar/Personal Calendar')
        self.assertEqual(url['UID'],    '3')

    def test_decode_message_headers(self):
        message = message_from_string(self._get_resource('3.'))
        headers = decode_message_headers(message)

        self.assertEqual(len(headers['From']), 1)
        self.assertEqual(len(headers['To']), 2)
        self.assertEqual(headers['To'][0], u'Br\u00fcderli, Thomas <thomas.bruederli@example.org>')
        self.assertEqual(headers['Content-Type'], 'text/plain')
        self.assertEqual(headers['Date'], '2014-09-24T04:52:00Z')
        self.assertEqual(headers['Subject'], 'Test')

    def test_mail_message2dict(self):
        message = mail_message2dict(self._get_resource('event_mime_message.eml'))

        self.assertIsInstance(message, dict)
        self.assertEqual(message['Subject'], '253E800C973E9FB99D174669001DB19B-FCBB6C4091F28CA0')
        self.assertEqual(message['X-Kolab-Type'], 'application/x-vnd.kolab.event')
        self.assertEqual(len(message['@parts']), 2)

        xmlpart = message['@parts'][1]
        self.assertEqual(xmlpart['Content-Type'], 'application/calendar+xml; charset=UTF-8; name=kolab.xml')

        message2 = mail_message2dict("FOO")
        self.assertIsInstance(message2, dict)
        self.assertEqual(message2['@body'], "FOO")

    def test_imap_folder_path(self):
        p1 = imap_folder_path("imap://john.doe@example.org@kolab.example.org/Calendar;UID=3")
        self.assertEqual(p1, "user/john.doe/Calendar@example.org")

        p2 = imap_folder_path("imap://john.doe@example.org@kolab.example.org/INBOX;UIDVALIDITY=1411487702")
        self.assertEqual(p2, "user/john.doe@example.org")

        # test shared folders (but how are they referred in the uri?)
        p3 = imap_folder_path("imap://kolab.example.org/Shared%20Folders/shared/Project-X%40example.org;UIDVALIDITY=1412093781/;UID=2")
        self.assertEqual(p3, "shared/Project-X@example.org")

    def test_imap_mailbox_fs_path(self):
        path = imap_mailbox_fs_path("imap://john.doe@example.org@kolab.example.org/Calendar/Personal%20Calendar;UID=3")
        self.assertEqual(path, "/var/spool/imap/domain/e/example.org/j/user/john^doe/Calendar/Personal Calendar")
