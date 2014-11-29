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
