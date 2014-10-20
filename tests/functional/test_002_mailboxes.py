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
