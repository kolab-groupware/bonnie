# -*- coding: utf-8 -*-
# Copyright 2010-2014 Kolab Systems AG (http://www.kolabsys.com)
#
# Jeroen van Meeuwen (Kolab Systems) <vanmeeuwen a kolabsys.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 3 or, at your option, any later
# version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Library General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307,
# USA.
#

import time

from bonnie.broker.state import init_db, Collector

def add(identity, state = b'READY'):
    db = init_db('collectors')
    db.add(Collector(identity, state))
    db.commit()

def set_state(identity, state):
    db = init_db('collectors')
    entry = None

    entry = db.query(Collector).filter_by(identity=identity).first()

    if entry == None:
        add(identity)
    else:
        entry.state = state
        db.commit()
