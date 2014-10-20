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