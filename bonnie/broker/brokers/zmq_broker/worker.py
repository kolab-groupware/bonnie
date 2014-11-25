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

import datetime
import time

from bonnie.broker.state import init_db, Job, Worker

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.broker.ZMQBroker')

def add(identity, state = b'READY'):
    db = init_db('workers')
    db.add(Worker(identity, state))
    db.commit()

def count():
    db = init_db('workers')
    result = db.query(Worker).count()
    return result

def count_by_state(state):
    db = init_db('workers')
    result = db.query(Worker).filter_by(state=state).count()
    return result

def select_by_state(state):
    db = init_db('workers')
    result = db.query(Worker).filter_by(state=state).all()
    return result

def select(identity):
    db = init_db('workers')
    result = db.query(Worker).filter_by(identity=identity).first()
    return result

def send_job(identity, job_uuid):
    db = init_db('workers')

    job = db.query(Job).filter_by(uuid=job_uuid).first()
    worker = db.query(Worker).filter_by(identity=identity).first()

    job.state = b'ALLOC'
    job.timestamp = datetime.datetime.utcnow()
    worker.timestamp = datetime.datetime.utcnow()
    worker.state = b'BUSY'
    worker.job = job.id
    db.commit()

def set_job(identity, job_uuid):
    db = init_db('workers')
    job = db.query(Job).filter_by(uuid=job_uuid).first()

    if job == None:
        return

    worker = db.query(Worker).filter_by(identity=identity).first()
    worker.job = job.id
    job.worker = worker.id
    db.commit()

def set_state(identity, state):
    db = init_db('workers')
    worker = db.query(Worker).filter_by(identity=identity).first()

    if worker == None:
        db.add(Worker(identity, state))
        db.commit()

    else:
        worker.state = state
        worker.timestamp = datetime.datetime.utcnow()
        db.commit()

def expire():
    db = init_db('workers')
    for worker in db.query(Worker).filter(Worker.timestamp <= (datetime.datetime.utcnow() - datetime.timedelta(0, 120)), Worker.state == b'STALE').all():
        log.info("Purging worker %s as very stale" % (worker.identity))
        db.delete(worker)

    for worker in db.query(Worker).filter(Worker.timestamp <= (datetime.datetime.utcnow() - datetime.timedelta(0, 60)), Worker.state != b'STALE').all():
        log.info("Marking worker %s as stale" % (worker.identity))
        worker.state = b'STALE'

    db.commit()
