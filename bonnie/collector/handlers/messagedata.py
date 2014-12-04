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
import time
import json

from email import message_from_string

import bonnie
from bonnie import utils
from bonnie.utils import expand_uidset
from bonnie.utils import parse_imap_uri
from bonnie.utils import decode_message_headers

conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.collector.MessageDataHandler')

class MessageDataHandler(object):

    def __init__(self, *args, **kw):
        pass

    def register(self, callback):
        interests = {
                'FETCH':  {
                        'callback': self.retrieve_contents_for_messages
                    },
                'HEADER': {
                        'callback': self.retrieve_headers_for_messages
                    }
            }

        callback(interests)

    def retrieve_contents_for_messages(self, notification):
        notification = json.loads(notification)
        log.debug("FETCH for %r" % (notification), level=9)

        (messageContents, messageHeaders) = self._retrieve_contents_for_messages(notification)

        # set new notification properties, even if empty
        notification['messageContent'] = messageContents
        notification['messageHeaders'] = messageHeaders

        return json.dumps(notification)

    def retrieve_headers_for_messages(self, notification):
        notification = json.loads(notification)
        log.debug("HEADERS for %r" % (notification), level=9)

        (messageContents, messageHeaders) = self._retrieve_contents_for_messages(notification, True)

        # set new notification properties, even if empty
        notification['messageHeaders'] = messageHeaders

        return json.dumps(notification)

    def _retrieve_contents_for_messages(self, notification, headers_only=False):
        """
            Helper method to deliver message contents/headers
        """
        messageContents = {}
        messageHeaders = {}

        # split the uri parameter into useful parts
        uri = parse_imap_uri(notification['uri'])

        if notification.has_key('uidset'):
            message_uids = expand_uidset(notification['uidset'])
        elif notification.has_key('vnd.cmu.oldUidset'):
            message_uids = expand_uidset(notification['vnd.cmu.oldUidset'])
        elif uri.has_key('UID'):
            message_uids = [ uri['UID'] ]
            notification['uidset'] = ','.join(message_uids)

        # resolve uri into a mailbox path on the local file stystem
        mailbox_path = utils.imap_mailbox_fs_path(uri)

        log.debug("Using mailbox path: %r" % (mailbox_path), level=8)

        # mailbox exists, try reading the message files
        if os.path.exists(mailbox_path):
            for message_uid in message_uids:
                # using message file path like /var/spool/imap/domain/e/example.org/k/lists/kolab^org/devel/lists/kolab.org/devel@example.org/1.
                message_file_path = "%s/%s." % (mailbox_path, message_uid)

                log.debug("Open message file: %r" % (message_file_path), level=8)

                attempts = 5
                while attempts > 0:
                    attempts -= 1

                    if os.access(message_file_path, os.R_OK):
                        fp = open(message_file_path, 'r')

                        if headers_only:
                            data = ''
                            for line in fp:
                                data += line
                                if line.strip() == '':
                                    break;
                            data += "\r\n"
                        else:
                            data = fp.read()

                        fp.close()

                        # use email lib to parse message headers
                        try:
                            # find header delimiter
                            pos = data.find("\r\n\r\n")
                            message = message_from_string(data[0:pos])
                            headers = decode_message_headers(message)
                        except:
                            headers = dict()
                            messageHeaders[message_uid] = headers

                        # append raw message data and parsed headers
                        messageContents[message_uid] = data
                        messageHeaders[message_uid] = headers
                        break

                    elif attempts > 0:
                        log.debug("Failed to open message file %r; retry %d more times" % (
                            message_file_path, attempts
                        ), level=5)
                    else:
                        log.warning("Failed to open message file %r for uri=%s; uid=%s" % (
                            message_file_path, notification, message_uid
                        ))

                    time.sleep(1)
                # end while
            # end for
        else:
            log.warning("Mailbox path %r does not exits for uri=%s" % (mailbox_path, notification['uri']))

        return (messageContents, messageHeaders)
