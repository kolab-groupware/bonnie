import os
import json
import time

from . import TestBonnieFunctional
from bonnie.dealer import BonnieDealer

pwd = os.path.dirname(__file__)
basedir = os.path.join(pwd, '..', '..')

import bonnie
conf = bonnie.getConf()

class TestBonnieMailboxCreate(TestBonnieFunctional):

    def test_mailboxcreate(self):
        dealer = BonnieDealer()

        notification = {
            'event_id': 12345,
            'event': 'MailboxCreate',
            'user': 'john.doe@example.org',
            'uri': 'imap://john.doe@example.org@kolab.example.org/Calendar;UIDVALIDITY=12345'
        }

        dealer.run(json.dumps(notification))

        events = self.query_log([('event','=','MailboxCreate')])
        self.assertEqual(len(events), 1)

        event = events[0]
        self.assertTrue(event.has_key('folder_id'))
        self.assertTrue(event.has_key('user_id'))

        # TODO: check object records for folder and user
