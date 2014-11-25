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

from bonnie.broker.state import init_db, Collector, Interest

def add(identity, state = b'READY', interests = []):
    db = init_db('collectors')
    db.add(Collector(identity, state))
    db.commit()
    collector = db.query(Collector).filter_by(identity=identity).first()
    for cmd in interests:
        interest = db.query(Interest).filter_by(cmd=cmd).first()

        if interest == None:
            db.add(Interest(cmd))
            db.commit()
            interest = db.query(Interest).filter_by(cmd=cmd).first()

        collector.interests.append(interest)

    db.commit()

def count():
    db = init_db('collectors')
    result = db.query(Collector).count()
    return result

def count_by_state(state):
    db = init_db('collectors')
    result = db.query(Collector).filter_by(state=state).count()
    return result

def select(identity):
    db = init_db('collectors')
    collector = db.query(Collector).filter_by(identity=identity).first()
    return collector

def select_by_state(state):
    db = init_db('collectors')
    collectors = db.query(Collector).filter_by(state=state).all()
    return collectors

def set_state(identity, state, interests = []):
    db = init_db('collectors')

    collector = db.query(Collector).filter_by(identity=identity).first()

    if collector == None:
        db.add(Collector(identity, state))
        db.commit()
        collector = db.query(Collector).filter_by(identity=identity).first()

    collector.state = state

    for cmd in interests:
        interest = db.query(Interest).filter_by(cmd=cmd).first()

        if interest == None:
            db.add(Interest(cmd))
            db.commit()
            interest = db.query(Interest).filter_by(cmd=cmd).first()

        collector.interests.append(interest)

    db.commit()

def update(identity, **kw):
    db = init_db('collectors')

    collector = db.query(Collector).filter_by(identity=identity).first()

    for attr, value in kw.iteritems():
        setattr(collector, attr, value)

    db.commit()
