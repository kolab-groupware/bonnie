
__package__ = 'bonnie.worker.handlers'

import os

import bonnie

from base import HandlerBase

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
from messagebase import MessageHandlerBase
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
