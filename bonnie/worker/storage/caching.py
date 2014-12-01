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

class CachedDict(object):
    """
        dict-like class which drops items after the given TTL
    """
    def __init__(self, ttl=60):
        # TODO: use memcache for distributed memory-based caching
        self.ttl = ttl
        self.data = {}

    def remove(self, key):
        self.data.remove(key)

    def pop(self, key, default=None):
        item = self.data.pop(key)
        return item[0] if item is not None else default

    def update(self, other):
        expire = int(time.time()) + self.ttl
        self.data.update(dict((k, (v, expire)) for k, v in other.items()))

    def keys(self):
        now = int(time.time())
        return [k for k, v in self.data.items() if v[1] > now]

    def values(self):
        now = int(time.time())
        return [v[0] for v in self.data.values() if v[1] > now]

    def items(self):
        now = int(time.time())
        return dict((k, v[0]) for k, v in self.data.items() if v[1] > now).items()

    def iteritems(self):
        return self.items().iteritems()

    def has_key(self, key):
        return self.data.has_key(key) and self.data[key][1] > int(time.time())

    def expunge(self):
        now = int(time.time())
        self.data = dict((k, v) for k, v in self.data.items() if v[1] > now)

    def __getitem__(self, key):
        return self.data[key][0]

    def __setitem__(self, key, value):
        self.data[key] = (value, int(time.time()) + self.ttl)

    def __contains__(self, key):
        return self.has_key(key)

    def __len__(self):
        return len(self.keys())

    def __iter__(self):
        return self.items().__iter__()
