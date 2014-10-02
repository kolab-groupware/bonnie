# -*- coding: utf-8 -*-
# Copyright 2010-2014 Kolab Systems AG (http://www.kolabsys.com)
#
# Jeroen van Meeuwen (Kolab Systems) <vanmeeuwen a kolabsys.com>
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

"""
    Base handler for an event notification of type 'MailboxCreate'
"""

import bonnie
from bonnie.worker.handlers import HandlerBase

class MailboxCreateHandler(HandlerBase):
    event = 'MailboxCreate'

    def __init__(self, *args, **kw):
        HandlerBase.__init__(self, *args, **kw)
        self.log = bonnie.getLogger('bonnie.worker.' + self.event)

    def run(self, notification):
        # mailbox notifications require metadata
        if not notification.has_key('metadata'):
            self.log.debug("Adding GETMETADATA job for " + self.event, level=8)
            return (notification, [ b"GETMETADATA" ])

        # extract uniqueid from metadata -> triggers the storage module
        if notification['metadata'].has_key('/shared/vendor/cmu/cyrus-imapd/uniqueid'):
            notification['folder_uniqueid'] = notification['metadata']['/shared/vendor/cmu/cyrus-imapd/uniqueid']

        return (notification, [])
