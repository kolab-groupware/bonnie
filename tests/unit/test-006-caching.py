import time

from twisted.trial import unittest
from bonnie.worker.storage import CachedDict

class TestBonnieCaching(unittest.TestCase):

    def test_cached_dict(self):
        # dict with 10 seconds TTL
        d = CachedDict(10)
        d['one'] = 'ONE'
        d['two'] = 'TWO'

        self.assertEqual(len(d), 2)
        self.assertTrue(d.has_key('one'))
        self.assertFalse(d.has_key('none'))
        self.assertEqual(d['one'], 'ONE')
        # internal sorting is influenced by the expiry time
        # but sorting in dicts is irrelevant in most cases
        self.assertEqual(sorted(d.keys()), ['one','two'])
        self.assertEqual(sorted(d.values()), ['ONE','TWO'])
        self.assertEqual(sorted(d.items()), [ ('one','ONE'), ('two','TWO') ])

        time.sleep(5)
        d['five'] = 'FIVE'
        time.sleep(1)
        d['six'] = 'SIX'
        self.assertEqual(len(d), 4)
        self.assertEqual(d.pop('five'), 'FIVE')
        self.assertEqual(len(d), 3)

        # let the first two items expire
        time.sleep(5)
        self.assertEqual(len(d), 1)
        self.assertFalse(d.has_key('one'))

        # test iterator
        # 'five' was popped, thus only 'six' remains
        for k,v in d:
            self.assertEqual(k, 'six')
            self.assertEqual(v, 'SIX')

        # all expired
        time.sleep(5)
        self.assertEqual(len(d), 0)
        self.assertEqual(len(d.data), 3)

        # expunge internal cache
        d.expunge()
        self.assertEqual(len(d.data), 0)
        