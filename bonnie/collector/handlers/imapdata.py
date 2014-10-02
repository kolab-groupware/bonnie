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
import pykolab
from pykolab.imap import IMAP

import bonnie
from bonnie.utils import parse_imap_uri
from bonnie.utils import imap_folder_path

conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.collector.IMAPDataHandler')

class IMAPDataHandler(object):
    """
        Collector handler to provide metadata from IMAP
    """

    def __init__(self, *args, **kw):
        # load pykolab conf
        conf = pykolab.getConf()
        if not hasattr(conf, 'defaults'):
            conf.finalize_conf()

        self.imap = IMAP()

    def register(self, callback):
        interests = {
            'GETMETADATA':  { 'callback': self.get_imap_folder_metadata },
            'GETACL':  { 'callback': self.get_imap_folder_acl }
        }

        callback(interests)

    def get_imap_folder_metadata(self, notification):
        notification = json.loads(notification)
        log.debug("GETMETADATA for %r" % (notification), level=9)

        # split the uri parameter into useful parts
        uri = parse_imap_uri(notification['uri'])
        folder_path = imap_folder_path(uri)

        # get metadata using pykolab's imap module
        metadata = {}
        try:
            self.imap.connect()
            metadata = self.imap.get_metadata(folder_path)[folder_path]
            self.imap.disconnect()
        except Exception, e:
            log.warning("Failed to get metadata for %r: %r", folder_path, e)

        notification['metadata'] = metadata

        return json.dumps(notification)

    def get_imap_folder_acl(self, notification):
        notification = json.loads(notification)
        log.debug("GETACL for %r" % (notification), level=9)

        # split the uri parameter into useful parts
        uri = parse_imap_uri(notification['uri'])
        folder_path = imap_folder_path(uri)

        # get folder acls using pykolab's imap module
        acls = {}
        try:
            self.imap.connect()
            acls = self.imap.list_acls(folder_path)
            self.imap.disconnect()
        except Exception, e:
            log.warning("Failed to get ACLs for %r: %r", folder_path, e)

        notification['acl'] = acls

        return json.dumps(notification)
