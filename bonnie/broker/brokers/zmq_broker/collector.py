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

import datetime
import time

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.broker.ZMQBroker')

from bonnie.broker.state import init_db, Collector, Job, Interest

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
    else:
        collector.state = state
        collector.timestamp = datetime.datetime.utcnow()
        db.commit()

    if state == b'READY':
        collector.job = None

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

    if collector == None:
        db.add(Collector(identity))
        db.commit()
        collector = db.query(Collector).filter_by(identity=identity).first()

    for attr, value in kw.iteritems():
        setattr(collector, attr, value)

    db.commit()

def expire():
    db = init_db('collectors')

    for collector in db.query(Collector).filter(Collector.timestamp <= (datetime.datetime.utcnow() - datetime.timedelta(0, 90)), Collector.state == b'STALE').all():
        log.debug("Purging collector %s as very stale" % (collector.identity), level=7)
        for _job in db.query(Job).filter_by(collector=collector.identity).all():
            _job.state = b'PENDING'
            _job.timestamp = datetime.datetime.utcnow()

        db.delete(collector)
        db.commit()

    for collector in db.query(Collector).filter(Collector.timestamp <= (datetime.datetime.utcnow() - datetime.timedelta(0, 90)), Collector.state != b'STALE').all():
        log.debug("Marking collector %s as stale" % (collector.identity), level=7)
        for _job in db.query(Job).filter_by(collector=collector.identity).all():
            _job.state = b'PENDING'
            _job.timestamp = datetime.datetime.utcnow()

        collector.state = b'STALE'
        collector.timestamp = datetime.datetime.utcnow()
        db.commit()
