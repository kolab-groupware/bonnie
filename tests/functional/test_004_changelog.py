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

class TestBonnieChangelog(TestBonnieFunctional):

    def test_001_changelog(self):
        # we assume "messageHeaders" and "messageContent" payload already being collected
        messageappend = {
            "event": "MessageAppend",
            "messageSize": 2932,
            "messages": 2,
            "modseq": 107,
            "pid": 1248,
            "service": "imap",
            "timestamp": "2014-10-20T13:10:59.516+02:00",
            "uidnext": 38,
            "uidset": "37",
            "uri": "imap://john.doe@example.org@kolab.example.org/Calendar;UIDVALIDITY=1411487702/;UID=37",
            "user": "john.doe@example.org",
            "vnd.cmu.envelope": "(\"Mon, 20 Oct 2014 13:10:59 +0200\" \"253E800C973E9FB99D174669001DB19B-FCBB6C4091F28CA0\" ((NIL NIL \"john.doe\" \"example.org\")) ((NIL NIL \"john.doe\" \"example.org\")) ((NIL NIL \"john.doe\" \"example.org\")) ((NIL NIL \"john.doe\" \"example.org\")) NIL NIL NIL NIL)",
            "vnd.cmu.midset": [ "NIL" ],
            "vnd.cmu.sessionId": "kolab.example.org-1248-1413803459-1",
            "vnd.cmu.unseenMessages": 2,
            "messageHeaders": {
                "37": {
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
            },
            "messageContent": {
                "37": "MIME-Version: 1.0\r\nContent-Type: multipart/mixed;\r\n boundary=\"=_46bc539ab7a6c0a8bd4d2ddbf553df00\"\r\nFrom: thomas.bruederli@example.org\r\nTo: thomas.bruederli@example.org\r\nDate: Mon, 20 Oct 2014 13:23:40 +0200\r\nX-Kolab-Type: application/x-vnd.kolab.event\r\nX-Kolab-Mime-Version: 3.0\r\nSubject: 253E800C973E9FB99D174669001DB19B-FCBB6C4091F28CA0\r\nUser-Agent: Kolab 3.1/Roundcube 1.1-git\r\n\r\n--=_46bc539ab7a6c0a8bd4d2ddbf553df00\r\nContent-Transfer-Encoding: quoted-printable\r\nContent-Type: text/plain; charset=ISO-8859-1\r\n\r\nThis is a Kolab Groupware object....\r\n\r\n--=_46bc539ab7a6c0a8bd4d2ddbf553df00\r\nContent-Transfer-Encoding: 8bit\r\nContent-Type: application/calendar+xml; charset=UTF-8;\r\n name=kolab.xml\r\nContent-Disposition: attachment;\r\n filename=kolab.xml;\r\n size=1954\r\n\r\n<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?>\r\n<icalendar xmlns=\"urn:ietf:params:xml:ns:icalendar-2.0\">\r\n\r\n  <vcalendar>\r\n    <properties>\r\n      <prodid>\r\n        <text>Roundcube-libkolab-1.1 Libkolabxml-1.1</text>\r\n      </prodid>\r\n      <version>\r\n        <text>2.0</text>\r\n      </version>\r\n      <x-kolab-version>\r\n        <text>3.1.0</text>\r\n      </x-kolab-version>\r\n    </properties>\r\n    <components>\r\n      <vevent>\r\n        <properties>\r\n          <uid>\r\n            <text>253E800C973E9FB99D174669001DB19B-FCBB6C4091F28CA0</text>\r\n          </uid>\r\n          <created>\r\n            <date-time>2014-09-23T23:31:23Z</date-time>\r\n          </created>\r\n          <dtstamp>\r\n            <date-time>2014-10-20T11:23:40Z</date-time>\r\n          </dtstamp>\r\n          <sequence>\r\n            <integer>28</integer>\r\n          </sequence>\r\n          <class>\r\n            <text>PUBLIC</text>\r\n          </class>\r\n          <dtstart>\r\n            <parameters>\r\n              <tzid>\r\n                <text>/kolab.org/Europe/Berlin</text>\r\n              </tzid>\r\n            </parameters>\r\n            <date-time>2014-10-20T14:00:00</date-time>\r\n          </dtstart>\r\n          <dtend>\r\n            <parameters>\r\n              <tzid>\r\n                <text>/kolab.org/Europe/Berlin</text>\r\n              </tzid>\r\n            </parameters>\r\n            <date-time>2014-10-20T16:00:00</date-time>\r\n          </dtend>\r\n          <summary>\r\n            <text>Today</text>\r\n          </summary>\r\n          <description>\r\n            <text>(new revision)</text>\r\n          </description>\r\n          <organizer>\r\n            <parameters>\r\n              <cn>\r\n                <text>Br\u00fcederli, Thomas</text>\r\n              </cn>\r\n            </parameters>\r\n            <cal-address>mailto:%3Cthomas.bruederli%40example.org%3E</cal-address>\r\n          </organizer>\r\n        </properties>\r\n      </vevent>\r\n    </components>\r\n  </vcalendar>\r\n\r\n</icalendar>\r\n\r\n--=_46bc539ab7a6c0a8bd4d2ddbf553df00--\r\n"
            }
        }

        dealer = BonnieDealer()
        dealer.run(json.dumps(messageappend))

        # query by subject (i.e. object UUID)
        events = self.query_log([('headers.Subject','=','253E800C973E9FB99D174669001DB19B-FCBB6C4091F28CA0')])
        self.assertEqual(len(events), 1)

        event = events[0]
        self.assertIsInstance(event['headers'], dict)
        self.assertEqual(event['headers']['X-Kolab-Mime-Version'], '3.0')
        self.assertEqual(event['headers']['X-Kolab-Type'], 'application/x-vnd.kolab.event')

        self.assertTrue(event.has_key('user_id'))
        self.assertTrue(event.has_key('revision'))

