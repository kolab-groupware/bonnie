# -*- coding: utf-8 -*-
# Copyright 2010-2014 Kolab Systems AG (http://www.kolabsys.com)
#
# Thomas Bruederli <bruederli at kolabsys.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 3 or, at your option, any later version
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#

import json
import os

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
            'FETCH':  { 'callback': self.retrieve_contents_for_messages },
            'HEADER': { 'callback': self.retrieve_headers_for_messages }
        }

        callback(interests)

    def retrieve_contents_for_messages(self, notification):
        messageContents = {}
        messageHeaders = {}

        notification = json.loads(notification)
        log.debug("FETCH for %r" % (notification), level=9)

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

        # using message file path /var/spool/imap/domain/e/example.org/k/lists/kolab^org/devel/lists/kolab.org/devel@example.org/1.
        for message_uid in message_uids:
            message_file_path = "%s/%s." % (mailbox_path, message_uid)

            log.debug("Open message file: %r" % (message_file_path), level=8)

            if os.access(message_file_path, os.R_OK):
                fp = open(message_file_path, 'r')
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

                # append raw message data and parsed headers
                messageContents[message_uid] = data
                messageHeaders[message_uid] = headers

            else:
                log.warning("Failed to open message file %r for uri=%s; uid=%s" % (message_file_path, notification, message_uid))

        # set new notification properties, even if empty
        notification['messageContent'] = messageContents
        notification['messageHeaders'] = messageHeaders

        return json.dumps(notification)

    def retrieve_headers_for_messages(self, notification):
        messageHeaders = {}

        notification = json.loads(notification)
        log.debug("HEADERS for %r" % (notification), level=9)

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

        for message_uid in message_uids:
            message_file_path = "%s/%s." % (mailbox_path, message_uid)

            log.debug("Open message file: %r" % (message_file_path), level=8)

            # read file line by line until we reach an empty line
            if os.access(message_file_path, os.R_OK):
                data = ''
                fp = open(message_file_path, 'r')
                for line in fp:
                    data += line
                    if line.strip() == '':
                        break;

                fp.close()

                # use email lib to parse message headers
                try:
                    message = message_from_string(data)
                    headers = decode_message_headers(message)
                except Exception, e:
                    log.warning("Failed to parse MIME message headers: %r", e)
                    headers = data

                messageHeaders[message_uid] = headers

            else:
                log.warning("Failed to open message file %r for uri=%s; uid=%s" % (message_file_path, notification, message_uid))
                # TODO: fall back to vnd.cmu.envelope property

        notification['messageHeaders'] = messageHeaders

        return json.dumps(notification)
