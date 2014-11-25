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
from pykolab.auth import Auth

import bonnie
from bonnie.utils import parse_imap_uri
from bonnie.utils import imap_folder_path

conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.collector.LDAPDataHandler')

class LDAPDataHandler(object):
    """
        Collector handler to provide user data from LDAP
    """

    def __init__(self, *args, **kw):
        # load pykolab conf
        self.pykolab_conf = pykolab.getConf()
        if not hasattr(self.pykolab_conf, 'defaults'):
            self.pykolab_conf.finalize_conf(fatal=False)

        self.ldap = Auth()
        self.ldap.connect()

    def register(self, callback):
        interests = {
            'GETUSERDATA': { 'callback': self.get_user_data }
        }

        callback(interests)

    def get_user_data(self, notification):
        notification = json.loads(notification)
        log.debug("GETUSERDATA for %r" % (notification), level=9)

        if notification.has_key('user'):
            try:
                user_dn = self.ldap.find_user_dn(notification['user'], True)
                log.debug("User DN for %s: %r" % (notification['user'], user_dn), level=8)
            except Exception, e:
                log.error("LDAP connection error: %r", e)
                user_dn = None

            if user_dn:
                unique_attr = self.pykolab_conf.get('ldap', 'unique_attribute', 'nsuniqueid')
                user_rec = self.ldap.get_entry_attributes(None, user_dn, [unique_attr, 'cn'])
                log.debug("User attributes: %r" % (user_rec), level=8)

                if user_rec and user_rec.has_key(unique_attr):
                    user_rec['dn'] = user_dn
                    user_rec['id'] = user_rec[unique_attr]
                    del user_rec[unique_attr]
            else:
                user_rec = None

            notification['user_data'] = user_rec

        return json.dumps(notification)
