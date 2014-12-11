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

import datetime

from dateutil.parser import parse
from dateutil.tz import tzutc

import riak
import json

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.worker.RiakOutput')

class RiakOutput(object):
    def __init__(self, *args, **kw):
        riak_output_address = conf.get(
                'worker',
                'riak_output_address'
            )

        if riak_output_address == None:
            riak_output_address = 'localhost'

        self.riak = riak.Riak(
                host=riak_output_address
            )

    def name(self):
        return 'riak_output'

    def register(self, callback):
        self.worker = callback({'_all': { 'callback': self.run }})

    def notification2log(self, notification):
        """
            Convert the given event notification record into a valid log entry
        """
        keymap = {
                'timestamp':                None,
                'clientIP':                 'client_ip',
                'clientPort':               None,
                'serverPort':               None,
                'serverDomain':             'domain',
                'aclRights':                'acl_rights',
                'aclSubject':               'acl_subject',
                'mailboxID':                'mailbox_id',
                'messageSize':              'message_size',
                'messageHeaders':           None,
                'messageContent':           None,
                'bodyStructure':            None,
                'metadata':                 None,
                'acl':                      None,
                'flagNames':                'flag_names',
                'diskUsed':                 'disk_used',
                'vnd.cmu.oldUidset':        'olduidset',
                'vnd.cmu.sessionId':        'session_id',
                'vnd.cmu.midset':           'message_id',
                'vnd.cmu.unseenMessages':   'unseen_messages',
            }

        log = { '@version': bonnie.API_VERSION }

        for key,val in notification.iteritems():
            newkey = keymap[key] if keymap.has_key(key) else key
            if newkey is not None:
                # convert NIL values into None which is more appropriate
                if isinstance(val, list):
                    val = [x for x in val if not x == "NIL"]
                elif val == "NIL":
                    val = None

                log[newkey] = val

        return log

    def run(self, notification):
        # The output should have UTC timestamps, but gets "2014-05-16T12:55:53.870+02:00"
        try:
            timestamp = parse(notification['timestamp']).astimezone(tzutc())
        except:
            timestamp = datetime.datetime.now(tzutc())

        notification['@timestamp'] = datetime.datetime.strftime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        index = 'logstash-%s' % (datetime.datetime.strftime(timestamp, "%Y.%m.%d"))
        jobs = []

        # for notifications concerning multiple messages, create entries for each message
        if notification.has_key('messageHeaders') and isinstance(notification['messageHeaders'], dict) and len(notification['messageHeaders']) > 0:
            for uid,headers in notification['messageHeaders'].iteritems():
                notification['uidset']  = uid
                notification['headers'] = headers
                notification['message'] = None
                if notification.has_key('messageContent') and notification['messageContent'].has_key(uid):
                    notification['message'] = notification['messageContent'][uid]
                    # no need for bodystructure if we have the real message content
                    notification.pop('bodyStructure', None)

                # remove vnd.cmu.envelope if we have headers
                notification.pop('vnd.cmu.envelope', None)

                try:
                    self.riak.create(
                        index=index,
                        doc_type='logs',
                        body=self.notification2log(notification)
                    )
                except Exception, errmsg:
                    log.warning("Riak create exception: %r", e)
                    jobs.append(b'POSTPONE')
                    break

        else:
            try:
                self.riak.create(
                    index=index,
                    doc_type='logs',
                    body=self.notification2log(notification)
                )
            except Exception, errmsg:
                log.warning("Riak create exception: %r", e)
                jobs.append(b'POSTPONE')

        return (notification, jobs)
