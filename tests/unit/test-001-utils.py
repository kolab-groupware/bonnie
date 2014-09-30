import os
from bonnie.utils import parse_imap_uri
from bonnie.utils import mail_message2dict
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

    def test_parse_imap_uri(self):
        url = parse_imap_uri("imap://john.doe@example.org@kolab33.example.org/Calendar/Personal%20Calendar;UIDVALIDITY=1411487702/;UID=3")
        self.assertEqual(url['host'],   'kolab33.example.org')
        self.assertEqual(url['user'],   'john.doe')
        self.assertEqual(url['domain'], 'example.org')
        self.assertEqual(url['path'],   'Calendar/Personal Calendar')
        self.assertEqual(url['UID'],    '3')

    def test_mail_message2dict(self):
        message = mail_message2dict(self._get_resource('event_mime_message.eml'))

        self.assertIsInstance(message, dict)
        self.assertEqual(message['Subject'], '253E800C973E9FB99D174669001DB19B-FCBB6C4091F28CA0')
        self.assertEqual(message['X-Kolab-Type'], 'application/x-vnd.kolab.event')
        self.assertEqual(len(message['_parts']), 2)

        xmlpart = message['_parts'][1]
        self.assertEqual(xmlpart['Content-Type'], 'application/calendar+xml; charset=UTF-8; name=kolab.xml')

        message2 = mail_message2dict("FOO")
        self.assertIsInstance(message2, dict)
        self.assertEqual(message2['_data'], "FOO")
        