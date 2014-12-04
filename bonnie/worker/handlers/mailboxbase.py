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

"""
    Base handler for an event notification of type 'MailboxRename'
"""

from bonnie.worker.handlers import HandlerBase

class MailboxHandlerBase(HandlerBase):
    event = None

    def __init__(self, *args, **kw):
        HandlerBase.__init__(self, *args, **kw)

    def run(self, notification):
        # call super for some basic notification processing
        (notification, jobs) = super(MailboxHandlerBase, self).run(notification)

        # mailbox notifications require metadata
        if not notification.has_key('metadata'):
            jobs.append(b"GETMETADATA")
            return (notification, jobs)

        # extract uniqueid from metadata -> triggers the storage module
        if notification['metadata'].has_key('/shared/vendor/cmu/cyrus-imapd/uniqueid'):
            notification['folder_uniqueid'] = notification['metadata']['/shared/vendor/cmu/cyrus-imapd/uniqueid']

        return (notification, jobs)
