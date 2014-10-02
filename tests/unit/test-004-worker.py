import json
import elasticsearch
import bonnie

from bonnie.worker.storage import ElasticSearchStorage
from twisted.trial import unittest

class MockupElasticsearch(object):
    """
        Dummy mockup to avoid real connections to ES
    """

    def __init__(self, *args, **kw):
        pass

    def search(self, *args, **kw):
        pass

    def create(self, *args, **kw):
        pass

    def update(self, *args, **kw):
        pass


class TestBonnieWorker(unittest.TestCase):

    def setUp(self):
        self.patch(elasticsearch, 'Elasticsearch', MockupElasticsearch)

    def test_001_notificaton2folder(self):
        notification = {
            'event': 'MailboxCreate',
            'uri': 'imap://john.doe@example.org@kolab.example.org/Calendar/Subcal;UIDVALIDITY=12345',
            'metadata': {
                '/shared/vendor/cmu/cyrus-imapd/partition':          'default',
                '/shared/vendor/cmu/cyrus-imapd/lastupdate':         '23-Sep-2014 11:55:15 -0400',
                '/shared/vendor/cmu/cyrus-imapd/duplicatedeliver':   'false',
                '/shared/vendor/cmu/cyrus-imapd/pop3newuidl':        'true',
                '/shared/vendor/cmu/cyrus-imapd/size':               '0',
                '/shared/vendor/cmu/cyrus-imapd/sharedseen':         'false',
                '/shared/vendor/kolab/folder-type':                  'event',
                '/shared/vendor/cmu/cyrus-imapd/uniqueid':           '28614de9-3614-422d-be5d-5716605ef0fc',
            },
            'acl': {
                'john.doe@example.org': "lrswipkxtecdan",
                'anyone': 'lrs',
            }
        }
        storage = ElasticSearchStorage()
        folder = storage.notificaton2folder(notification)
        folder_id = folder['id']

        self.assertTrue(folder.has_key('body'))
        self.assertEqual(folder['body']['uniqueid'], notification['folder_uniqueid'])
        self.assertEqual(folder['body']['server'], 'kolab.example.org')
        self.assertEqual(folder['body']['owner'], 'john.doe@example.org')
        self.assertEqual(folder['body']['uri'], 'imap://john.doe@example.org@kolab.example.org/Calendar/Subcal')

        # check changes in ignored metadata
        notification['metadata']['/shared/vendor/cmu/cyrus-imapd/lastupdate'] = '24-Sep-2014 16:21:19 -0400'
        notification['metadata']['/shared/vendor/cmu/cyrus-imapd/size'] = '88'
        notification['acl'] = {
            'anyone': 'lrs',
            'john.doe@example.org': "lrswipkxtecdan",
        }
        notification['uri'] = 'imap://john.doe@example.org@kolab.example.org/Calendar/RENAMED;UIDVALIDITY=123456'
        folder = storage.notificaton2folder(notification)
        self.assertEqual(folder['id'], folder_id)

        # detect ACL changes
        notification['acl'] = {
            'john.doe@example.org': "lrswipkxtecdan",
            'anyone': 'lrswd',
        }
        folder = storage.notificaton2folder(notification)
        self.assertNotEqual(folder['id'], folder_id)
        