# -*- coding: utf-8 -*-
#
# Copyright 2010-2014 Kolab Systems AG (http://www.kolabsys.com)
#
# Jeroen van Meeuwen (Kolab Systems) <vanmeeuwen a kolabsys.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 3 or, at your option, any later
# version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307,
# USA.
#

import elasticsearch
import json
import os
import subprocess
import time
import urllib
import urlparse
import inputs

from email import message_from_string

import bonnie
from bonnie.utils import expand_uidset
from bonnie.utils import parse_imap_uri
from bonnie.utils import mail_message2dict
from bonnie.utils import decode_message_headers

conf = bonnie.getConf()
log = bonnie.getLogger('collector')

class BonnieCollector(object):
    input_interests = {}
    input_modules = {}

    def __init__(self, *args, **kw):
        for _class in inputs.list_classes():
            __class = _class()
            self.input_modules[__class] = __class.register(callback=self.register_input)

    def get_imap_folder_path(self, uri):
        """
            Translate the folder name in to a fully qualified folder path such as it
            would be used by a cyrus administrator.
        """
        if isinstance(uri, str):
            uri = parse_imap_uri(uri)

        username = uri['user']
        domain = uri['domain']
        folder_name = uri['path']

        # Through filesystem
        # To get the mailbox path, use:
        # TODO: Assumption #1 is we are using virtual domains, and this domain does
        # TODO: Assumption #2 is the mailbox in question is a user mailbox
        # TODO: Assumption #3 is we use the unix hierarchy separator

        # Translate the folder name in to a fully qualified folder path such as it
        # would be used by a cyrus administrator.
        #
        # TODO: Other Users (covered, the netloc has the username the suffix is the
        # original folder name).
        #
        # TODO: Shared Folders.
        if not username == None:
            if folder_name == "INBOX":
                folder_path = os.path.join('user', '/%s@%s' % (username, domain))
            else:
                folder_path = os.path.join('user', username, '%s@%s' % (folder_name, domain))
        else:
            folder_path = folder_name

        # TODO: Check if this file exists and is actually executable
        # New in Python 2.7:
        if hasattr(subprocess, 'check_output'):
            mailbox_path = subprocess.check_output(
                    ["/usr/lib/cyrus-imapd/mbpath", folder_path]
                ).strip()
        else:
            # Do it the old-fashioned way
            p1 = subprocess.Popen(
                    ["/usr/lib/cyrus-imapd/mbpath", folder_path],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )

            (stdout, stderr) = p1.communicate()
            mailbox_path = stdout.strip()

        # TODO: Assumption #4 is we use altnamespace
        if not folder_name == "INBOX":
            if not len(folder_name.split('@')) > 0:
                mailbox_path = os.path.join(mailbox_path, folder_name)

        return mailbox_path

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
        mailbox_path = self.get_imap_folder_path(uri)
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
                    print "Message Headers:", pos, data[0:pos]
                    message = message_from_string(data[0:pos])
                    headers = decode_message_headers(message)
                except:
                    headers = dict()

                # append raw message data and parsed headers
                messageContents[message_uid] = data
                messageHeaders[message_uid] = headers

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
        mailbox_path = self.get_imap_folder_path(uri)
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

        notification['messageHeaders'] = messageHeaders

        return json.dumps(notification)

    def execute(self, command, notification):
        """
            Our goal is to collect whatever message contents
            for the messages referenced in the notification.
        """
        log.debug("Executing collection command %s" % (command), level=8)
        if command == "FETCH":
            notification = self.retrieve_contents_for_messages(notification)
        elif command == "HEADER":
            notification = self.retrieve_headers_for_messages(notification)
        elif command == "GETMETADATA":
            pass
        elif command == "GETACL":
            pass

        return notification
        #self.output(notification)

    def register_input(self, interests):
        self.input_interests = interests

    def run(self):
        input_modules = conf.get('collector', 'input_modules')
        for _input in self.input_modules.keys():
            if _input.name() == input_modules:
                _input.run(callback=self.execute)

