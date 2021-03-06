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

from caching import CachedDict

__all__ = [
        'CachedDict'
    ]

try:
    from elasticsearch_storage import ElasticSearchStorage
    __all__.append('ElasticSearchStorage')
except ImportError, errmsg:
    pass

try:
    from riak_storage import RiakStorage
    __all__.append('RiakStorage')
except ImportError, errmsg:
    pass

def list_classes():
    classes = []

    if 'ElasticSearchStorage' in __all__:
        classes.append(ElasticSearchStorage)

    if 'RiakStorage' in __all__:
        classes.append(RiakStorage)

    return classes
