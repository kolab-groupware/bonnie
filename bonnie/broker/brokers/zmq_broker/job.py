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

from sqlalchemy.exc import IntegrityError

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.broker.ZMQBroker')

from bonnie.broker.state import init_db, Job, Worker

def add(dealer, notification, job_type='worker'):
    """
        Add a new job.
    """
    db = init_db('jobs')
    try:
        db.add(Job(dealer, notification, job_type))
        db.commit()
    except IntegrityError, errmsg:
        db.rollback()

def count():
    db = init_db('jobs')
    result = db.query(Job).count()
    return result

def count_by_state(state):
    db = init_db('jobs')
    result = db.query(Job).filter_by(state=state).count()
    return result

def count_by_type(job_type):
    db = init_db('jobs')
    result = db.query(Job).filter_by(job_type=job_type).all()
    return result

def select(job_uuid):
    db = init_db('jobs')
    result = db.query(Job).filter_by(uuid=job_uuid).first()
    return result

def select_all():
    db = init_db('jobs')
    result = db.query(Job).all()
    return result

def select_by_state(state):
    db = init_db('jobs')
    result = db.query(Job).filter_by(state=state).all()
    return result

def select_by_type(job_type):
    db = init_db('jobs')
    result = db.query(Job).filter_by(job_type=job_type).all()
    return result

def select_by_type_and_state(job_type, state, limit=-1):
    db = init_db('jobs')

    if limit == -1:
        result = db.query(Job).filter_by(job_type=job_type, state=state).order_by(Job.timestamp).all()
    else:
        result = db.query(Job).filter_by(job_type=job_type, state=state).order_by(Job.timestamp).limit(limit).all()

    return result

def select_for_collector(identity):
    db = init_db('jobs')
    result = db.query(Job).filter_by(collector=identity, job_type='collector', state=b'PENDING').first()
    if not result == None:
        result.state = b'ALLOC'
        result.timestamp = datetime.datetime.utcnow()
    return result

def set_state(uuid, state):
    db = init_db('jobs')
    for job in db.query(Job).filter_by(uuid=uuid).all():
        job.state = state
        job.timestamp = datetime.datetime.utcnow()
    db.commit()

def set_job_type(uuid, job_type):
    db = init_db('jobs')
    job = db.query(Job).filter_by(uuid=uuid).first()
    job.job_type = job_type
    job.timestamp = datetime.datetime.utcnow()
    db.commit()

def update(job_uuid, **kw):
    db = init_db('jobs')
    job = db.query(Job).filter_by(uuid=job_uuid).first()

    for attr, value in kw.iteritems():
        setattr(job, attr, value)

    job.timestamp = datetime.datetime.utcnow()

    db.commit()

def expire():
    """
        Unlock jobs that have been allocated to some worker way too long
        ago.
    """
    db = init_db('jobs')

    for job in db.query(Job).filter(Job.timestamp <= (datetime.datetime.utcnow() - datetime.timedelta(0, 120)), Job.state == b'DONE').all():
        log.info("Purging job %s" % (job.uuid))
        db.delete(job)

    db.commit()

def unlock():
    """
        Unlock jobs that have been allocated to some worker way too long
        ago.
    """
    db = init_db('jobs')

    for job in db.query(Job).filter(
                Job.timestamp <= (datetime.datetime.utcnow() - datetime.timedelta(0, 120)),
                Job.state == b'ALLOC'
            ).all():

        log.info("Unlocking %s job %s" % (job.job_type, job.uuid))
        job.state = b'PENDING'

        for worker in db.query(Worker).filter_by(job=job.id).all():
            worker.job = None

    db.commit()
