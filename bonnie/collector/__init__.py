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

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('collector')

class BonnieCollector(object):
    input_interests = {}
    input_modules = {}

    def __init__(self, *args, **kw):
        for _class in inputs.list_classes():
            __class = _class()
            self.input_modules[__class] = __class.register(callback=self.register_input)

    def expand_uidset(uidset):
        _uids = []
        for _uid in uidset.split(','):
            if len(_uid.split(':')) > 1:
                for __uid in range((int)(_uid.split(':')[0]), (int)(_uid.split(':')[1])+1):
                    _uids.append("%d" % (__uid))
            else:
                _uids.append(str(_uid))

        return _uids

    def retrieve_contents_for_messages(self, notification):
        messageContents = {}

        notification = json.loads(notification)
        log.debug("FETCH for %r" % (notification), level=9)

        split_uri = urlparse.urlsplit(notification['uri'])

        if len(split_uri.netloc.split('@')) == 3:
            (username, domain, server) = split_uri.netloc.split('@')
        elif len(split_uri.netloc.split('@')) == 2:
            (username, server) = split_uri.netloc.split('@')
            domain = None
        elif len(split_uri.netloc.split('@')) == 1:
            username = None
            domain = None
            server = split_uri.netloc

        # First, .path == '/Calendar/Personal%20Calendar;UIDVALIDITY=$x[/;UID=$y]
        # Take everything after the first slash, and omit any INBOX/ stuff.
        path_part = '/'.join([x for x in split_uri.path.split('/') if not x == "INBOX"][1:])

        # Second, .path == 'Calendar/Personal%20Calendar;UIDVALIDITY=$x[/;UID=$y]
        # Take everything before the first ';' (but only actually take everything
        # before the first ';' in the next step, here we still have use for it).
        # TODO: parse the path/query parameters into a dict
        path_part = path_part.split(';')

        if notification.has_key('uidset'):
            message_uids = expand_uidset(notification['uidset'])
        elif notification.has_key('vnd.cmu.oldUidset'):
            message_uids = expand_uidset(notification['vnd.cmu.oldUidset'])
        elif len(path_part) == 3:
            # Use or abuse the length of path_parts at this moment to extract the message UID
            message_uids = [ path_part[2].split('=')[1] ]
            notification['uidset'] = message_uids


        # Third, .path == 'Calendar/Personal%20Calendar
        # Decode the url encoding
        folder_name = urllib.unquote(path_part[0])

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
                folder_path = 'user/%s@%s' % (username, domain)
            else:
                folder_path = 'user/%s/%s@%s' % (username, folder_name, domain)
        else:
            folder_path = folder_name

        # TODO: Check if this file exists and is actually executable
        # New in Python 2.7:
        if hasattr(subprocess, 'check_output'):
            mailbox_path = subprocess.check_output(
                    ["/usr/lib/cyrus-imapd/mbpath", folder_path]
                ).strip()

            log.debug("Using mailbox path: %r" % (mailbox_path), level=8)

        else:
            # Do it the old-fashioned way
            p1 = subprocess.Popen(
                    ["/usr/lib/cyrus-imapd/mbpath", folder_path],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )

            (stdout, stderr) = p1.communicate()

            mailbox_path = stdout.strip()
            log.debug("Using mailbox path: %r" % (mailbox_path), level=8)

        # TODO: Assumption #4 is we use altnamespace
        if not folder_name == "INBOX":
            if not len(folder_name.split('@')) > 0:
                mailbox_path = "%s/%s" % (mailbox_path, folder_name)

        # using message file path /var/spool/imap/domain/e/example.org/k/lists/kolab^org/devel/lists/kolab.org/devel@example.org/1.
        for message_uid in message_uids:
            message_file_path = "%s/%s." % (mailbox_path, message_uid)

            log.debug("Open message file: %r" % (message_file_path), level=8)

            if os.access(message_file_path, os.R_OK):
                fp = open(message_file_path, 'r')
                data = fp.read()
                fp.close()

                # TODO: parse mime message and return a dict
                messageContents[message_uid] = data

        notification['messageContent'] = messageContents

        return json.dumps(notification)

    def retrieve_headers_for_messages(self, notification):
        messageHeaders = {}

        notification = json.loads(notification)
        log.debug("HEADERS for %r" % (notification), level=9)

        # TODO: resovle message file system path
        # TODO: read file line by line until we reach an empty line
        # TODO: decode quoted-pritable or base64 encoded headers

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

