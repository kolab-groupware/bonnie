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

__package__ = 'bonnie.worker.handlers'

import os

import bonnie

from base import HandlerBase
from mailboxbase import MailboxHandlerBase
from messagebase import MessageHandlerBase

from aclchange import AclChangeHandler
from flagsclear import FlagsClearHandler
from flagsset import FlagsSetHandler
from login import LoginHandler
from logout import LogoutHandler
from mailboxcreate import MailboxCreateHandler
from mailboxdelete import MailboxDeleteHandler
from mailboxrename import MailboxRenameHandler
from mailboxsubscribe import MailboxSubscribeHandler
from mailboxunsubscribe import MailboxUnsubscribeHandler
from messageappend import MessageAppendHandler
from messagecopy import MessageCopyHandler
from messageexpire import MessageExpireHandler
from messageexpunge import MessageExpungeHandler
from messagemove import MessageMoveHandler
from messagenew import MessageNewHandler
from messageread import MessageReadHandler
from messagetrash import MessageTrashHandler
from quotaexceeded import QuotaExceededHandler
from quotawithin import QuotaWithinHandler
from quotachange import QuotaChangeHandler
from changelog import ChangelogHandler

__all__ = [
        'AclChangeHandler',
        'FlagsClearHandler',
        'FlagsSetHandler',
        'LoginHandler',
        'LogoutHandler',
        'MailboxHandlerBase',
        'MailboxCreateHandler',
        'MailboxDeleteHandler',
        'MailboxRenameHandler',
        'MailboxSubscribeHandler',
        'MailboxUnsubscribeHandler',
        'MessageAppendHandler',
        'MessageCopyHandler',
        'MessageExpireHandler',
        'MessageExpungeHandler',
        'MessageMoveHandler',
        'MessageNewHandler',
        'MessageReadHandler',
        'MessageTrashHandler',
        'QuotaExceededHandler',
        'QuotaWithinHandler',
        'QuotaChangeHandler'
    ]

conf = bonnie.getConf()

def list_classes():
    return [
            AclChangeHandler,
            FlagsClearHandler,
            FlagsSetHandler,
            LoginHandler,
            LogoutHandler,
            MailboxCreateHandler,
            MailboxDeleteHandler,
            MailboxRenameHandler,
            MailboxSubscribeHandler,
            MailboxUnsubscribeHandler,
            MessageAppendHandler,
            MessageCopyHandler,
            MessageExpireHandler,
            MessageExpungeHandler,
            MessageMoveHandler,
            MessageNewHandler,
            MessageReadHandler,
            MessageTrashHandler,
            QuotaExceededHandler,
            QuotaWithinHandler,
            QuotaChangeHandler,
            ChangelogHandler
        ]
