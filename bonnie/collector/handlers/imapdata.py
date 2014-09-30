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
log = bonnie.getLogger('collector.IMAPDataHandler')

class IMAPDataHandler(object):
    """
        Collector handler to provide metadata from IMAP
    """

    def __init__(self, *args, **kw):
        # load pykolab conf
        conf = pykolab.getConf()
        if not hasattr(conf, 'defaults'):
            conf.finalize_conf()

    def register(self, callback):
        interests = {
            'GETMETADATA':  { 'callback': self.get_imap_folder_metadata }
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
            imap = IMAP()
            imap.connect()
            metadata = imap.get_metadata(folder_path)[folder_path]
            imap.disconnect()
        except Exception, e:
            print e
            log.warning("Failed to get metadata for %r: %r", folder_path, e)

        notification['metadata'] = metadata

        return json.dumps(notification)
