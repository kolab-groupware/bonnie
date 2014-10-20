import json

from . import TestBonnieFunctional
from bonnie.dealer import BonnieDealer
from email import message_from_string

import bonnie
conf = bonnie.getConf()

class TestBonnieMessageEvents(TestBonnieFunctional):

    def test_001_messagenew(self):
        # we assume "messageHeaders" and "messageContent" payload already being collected
        messagenew = {
            "event": "MessageNew",
            "messageSize": 976,
            "messages": 6,
            "modseq": 20,
            "pid": 2340,
            "service": "lmtpunix",
            "timestamp": "2014-10-20T13:34:14.966+02:00",
            "uidnext": 7,
            "uidset": "6",
            "uri": "imap://john.doe@example.org@kolab.example.org/INBOX;UIDVALIDITY=1411487714/;UID=6",
            "user": "john.doe@example.org",
            "vnd.cmu.midset": [ "<a8486f5db6ec207de9b9f069850546ee@example.org>" ],
            "vnd.cmu.sessionId": "kolab.example.org-2340-1413804854-1",
            "vnd.cmu.unseenMessages": 3,
            "messageHeaders": {
                "6": {
                    "Content-Transfer-Encoding": "7bit",
                    "Content-Type": "text/plain",
                    "Date": "2014-10-20T11:32:41Z",
                    "From": [ "Br\u00fcederli, Thomas <john.doe@example.org>" ],
                    "MIME-Version": "1.0",
                    "Message-ID": "<a8486f5db6ec207de9b9f069850546ee@example.org>",
                    "Received": "from kolab.example.org ([unix socket])\r\n\t by kolab.example.org (Cyrus git2.5+0-Kolab-2.5-67.el6.kolab_3.4) with LMTPA;\r\n\t Mon, 20 Oct 2014 13:34:14 +0200",
                    "Return-Path": "<john.doe@example.org>",
                    "Subject": "MessageNew event test",
                    "To": [ "Doe, John <john.doe@example.org>" ],
                    "X-Sender": "john.doe@example.org",
                    "X-Sieve": "CMU Sieve 2.4",
                    "X-Spam-Flag": "NO",
                    "X-Spam-Level": "",
                    "X-Spam-Score": "-0.002",
                    "X-Spam-Status": "No, score=-0.002 tagged_above=-10 required=6.2\r\n\ttests=[NO_RECEIVED=-0.001, NO_RELAYS=-0.001] autolearn=ham",
                    "X-Virus-Scanned": "amavisd-new at example.org"
                }
            },
            "messageContent": {
                "6": "Return-Path: <john.doe@example.org>\r\nReceived: from kolab.example.org ([unix socket])\r\n\t by kolab.example.org (Cyrus git2.5+0-Kolab-2.5-67.el6.kolab_3.4) with LMTPA;\r\n\t Mon, 20 Oct 2014 13:34:14 +0200\r\nX-Sieve: CMU Sieve 2.4\r\nX-Virus-Scanned: amavisd-new at example.org\r\nX-Spam-Flag: NO\r\nX-Spam-Score: -0.002\r\nX-Spam-Level: \r\nX-Spam-Status: No, score=-0.002 tagged_above=-10 required=6.2\r\n\ttests=[NO_RECEIVED=-0.001, NO_RELAYS=-0.001] autolearn=ham\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=US-ASCII;\r\n format=flowed\r\nContent-Transfer-Encoding: 7bit\r\nDate: Mon, 20 Oct 2014 13:32:41 +0200\r\nFrom: =?UTF-8?Q?Br=C3=BCederli=2C_Thomas?= <john.doe@example.org>\r\nTo: \"Doe, John\" <john.doe@example.org>\r\nSubject: MessageNew event test\r\nMessage-ID: <a8486f5db6ec207de9b9f069850546ee@example.org>\r\nX-Sender: john.doe@example.org\r\n\r\nThis message should trigger the MessageNew event for john.doe...\r\n...and MessageAppend to /Sent for the sender.\r\n"
            }
        }

        dealer = BonnieDealer()
        dealer.run(json.dumps(messagenew))

        events = self.query_log([('event','=','MessageNew')])
        self.assertEqual(len(events), 1)

        event = events[0]
        self.assertEqual(event['uidset'], '6')
        self.assertEqual(event['service'], 'lmtpunix')
        self.assertEqual(event['session_id'], messagenew['vnd.cmu.sessionId'])
        self.assertEqual(event['@timestamp'], '2014-10-20T11:34:14.966000Z')
        self.assertEqual(len(event['vnd.cmu.midset']), 1)

        self.assertIsInstance(event['headers'], dict)
        self.assertTrue(event['headers']['Message-ID'] in event['vnd.cmu.midset'])
        self.assertTrue(event['headers']['Subject'] in messagenew['messageHeaders']['6']['Subject'])

        # check if message payload is parsable
        message = message_from_string(event['message'].encode('utf8','replace'))
        self.assertEqual(message['Subject'], event['headers']['Subject'])

        # check objects/folder entry
        self.assertTrue(event.has_key('folder_id'))
        folder = self.storage_get(event['folder_id'], index='objects', doctype='folder')

        self.assertIsInstance(folder, dict)
        self.assertEqual(folder['uniqueid'], event['folder_uniqueid'])
        self.assertEqual(folder['name'], 'INBOX')
        self.assertEqual(folder['owner'], 'john.doe@example.org')

        # check objects/user entry
        self.assertTrue(event.has_key('user_id'))
        user = self.storage_get(event['user_id'], index='objects', doctype='user')

        self.assertIsInstance(user, dict)
        self.assertEqual(user['user'], messagenew['user'])


    def test_002_messageappend(self):
        messageappend = {
            "event": "MessageAppend",
            "flagNames": "\seen",
            "messageSize": 555,
            "messages": 6,
            "modseq": 12,
            "pid": 2222,
            "service": "imap",
            "timestamp": "2014-10-20T13:33:27.062+02:00",
            "uidnext": 9,
            "uidset": "8",
            "uri": "imap://john.doe@example.org@kolab.example.org/Sent;UIDVALIDITY=1411487701/;UID=8",
            "user": "john.doe@example.org",
            "vnd.cmu.envelope": "(\"Mon, 20 Oct 2014 13:33:26 +0200\" \"MessageNew event test\" ((\"=?UTF-8?Q?Br=C3=BCederli=2C_Thomas?=\" NIL \"john.doe\" \"example.org\")) ((\"=?UTF-8?Q?Br=C3=BCederli=2C_Thomas?=\" NIL \"john.doe\" \"example.org\")) ((\"=?UTF-8?Q?Br=C3=BCederli=2C_Thomas?=\" NIL \"john.doe\" \"example.org\")) ((\"Doe, John\" NIL \"john.doe\" \"example.org\")) NIL NIL NIL \"<20f46a82b8584c1518fbeac7bad5f05b@example.org>\")",
            "vnd.cmu.midset": [ "<20f46a82b8584c1518fbeac7bad5f05b@example.org>" ],
            "vnd.cmu.sessionId": "kolab.example.org-2222-1413804806-1",
            "vnd.cmu.unseenMessages": 0,
            "folder_id": "76b8cd8f85bb435d17fe28d576db64a7",
            "folder_uniqueid": "f356a1a9-f897-454f-9ada-5646fe4c4117",
            "messageHeaders": {
                "8": {
                    "Content-Transfer-Encoding": "7bit",
                    "Content-Type": "text/plain",
                    "Date": "2014-10-20T11:31:11Z",
                    "From": [ "Br\u00fcederli, Thomas <john.doe@example.org>" ],
                    "MIME-Version": "1.0",
                    "Message-ID": "<20f46a82b8584c1518fbeac7bad5f05b@example.org>",
                    "Subject": "MessageNew event test",
                    "To": [ "Doe, John <john.doe@example.org>" ],
                    "User-Agent": "Kolab 3.1/Roundcube 1.1-git",
                    "X-Sender": "john.doe@example.org"
                }
            },
            "messageContent": {
                "8": "MIME-Version: 1.0\r\nContent-Type: text/plain; charset=US-ASCII;\r\n format=flowed\r\nContent-Transfer-Encoding: 7bit\r\nDate: Mon, 20 Oct 2014 13:31:11 +0200\r\nFrom: =?UTF-8?Q?Br=C3=BCederli=2C_Thomas?= <john.doe@example.org>\r\nTo: \"Doe, John\" <john.doe@example.org>\r\nSubject: MessageNew event test\r\nMessage-ID: <44ef83beb911cb9cd82e8dc7a29467a9@example.org>\r\nX-Sender: john.doe@example.org\r\nUser-Agent: Kolab 3.1/Roundcube 1.1-git\r\n\r\nThis message should trigger the MessageNew event for john.doe...\r\n...and MessageAppend to /Sent for the sender."
            }
        }

        dealer = BonnieDealer()
        dealer.run(json.dumps(messageappend))

        events = self.query_log([('event','=','MessageAppend')])
        self.assertEqual(len(events), 1)

        event = events[0]
        self.assertTrue(event.has_key('user_id'))
        self.assertIsInstance(event['headers'], dict)
        self.assertEqual(len(event['headers']['To']), 1)
        self.assertEqual(event['headers']['Content-Type'], 'text/plain')
        self.assertTrue(event['headers']['Message-ID'] in event['vnd.cmu.midset'])


    def test_003_messageread(self):
        messageread = {
            "event": "MessageRead",
            "messages": 3,
            "modseq": 64,
            "pid": 802,
            "service": "imap",
            "timestamp": "2014-10-20T13:04:09.077+02:00",
            "uidnext": 7,
            "uidset": "4",
            "uri": "imap://john.doe@example.org@kolab.example.org/INBOX;UIDVALIDITY=1411487701",
            "user": "john.doe@example.org",
            "vnd.cmu.midset": [ "<e0ffe5d5a1569a35c1b62791390a48d5@example.org>" ],
            "vnd.cmu.sessionId": "kolab.example.org-802-1413803049-1",
            "vnd.cmu.unseenMessages": 0
        }

        dealer = BonnieDealer()
        dealer.run(json.dumps(messageread))

        # query by message-ID
        events = self.query_log([('vnd.cmu.midset','=','<e0ffe5d5a1569a35c1b62791390a48d5@example.org>')])
        self.assertEqual(len(events), 1)

        event = events[0]
        self.assertEqual(event['uidset'], '4')
        self.assertTrue(event.has_key('user_id'))
        self.assertTrue(event.has_key('folder_id'))

    def test_004_flagsclear(self):
        flagsclear = {
            "event": "FlagsClear",
            "flagNames": "\Seen",
            "messages": 3,
            "modseq": 47,
            "pid": 489,
            "service": "imap",
            "timestamp": "2014-10-20T13:03:31.348+02:00",
            "uidnext": 7,
            "uidset": "4",
            "uri": "imap://john.doe@example.org@kolab.example.org/INBOX;UIDVALIDITY=1411487701",
            "user": "john.doe@example.org",
            "vnd.cmu.midset": [ "<e0ffe5d5a1569a35c1b62791390a48d5@example.org>" ],
            "vnd.cmu.sessionId": "kolab.example.org-489-1413803011-1",
            "vnd.cmu.unseenMessages": 1
        }

        dealer = BonnieDealer()
        dealer.run(json.dumps(flagsclear))

        # query by message-ID
        events = self.query_log([('vnd.cmu.midset','=','<e0ffe5d5a1569a35c1b62791390a48d5@example.org>')])
        self.assertEqual(len(events), 1)

        event = events[0]
        self.assertEqual(event['uidset'], '4')
        self.assertEqual(event['flag_names'], '\Seen')
        self.assertEqual(event['vnd.cmu.unseenMessages'], 1)
        self.assertTrue(event.has_key('user_id'))
        self.assertTrue(event.has_key('folder_id'))

    def test_005_messagetrash(self):
        messagetrash = {
            "event": "MessageTrash",
            "messages": 2,
            "modseq": 104,
            "pid": 1248,
            "service": "imap",
            "timestamp": "2014-10-20T13:10:59.546+02:00",
            "uidnext": 38,
            "uidset": "36",
            "uri": "imap://john.doe@example.org@kolab.example.org/Calendar;UIDVALIDITY=1411487702",
            "user": "john.doe@example.org",
            "vnd.cmu.midset": [ "NIL" ],
            "vnd.cmu.sessionId": "kolab.example.org-1248-1413803459-1",
            "vnd.cmu.unseenMessages": 2,
            "messageHeaders": {
                "36": {
                    "Content-Type": "multipart/mixed",
                    "Date": "2014-10-20T11:23:40Z",
                    "From": [ " <thomas.bruederli@example.org>" ],
                    "MIME-Version": "1.0",
                    "Subject": "253E800C973E9FB99D174669001DB19B-FCBB6C4091F28CA0",
                    "To": [ " <thomas.bruederli@example.org>" ],
                    "User-Agent": "Kolab 3.1/Roundcube 1.1-git",
                    "X-Kolab-Mime-Version": "3.0",
                    "X-Kolab-Type": "application/x-vnd.kolab.event"
                }
            }
        }

        dealer = BonnieDealer()
        dealer.run(json.dumps(messagetrash))

        events = self.query_log([('event','=','MessageTrash')])
        self.assertEqual(len(events), 1)

        event = events[0]
        self.assertTrue(event['headers']['Subject'] in messagetrash['messageHeaders']['36']['Subject'])

        