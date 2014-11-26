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

from bonnie.broker.state import init_db, Collector, Job, Worker

MAX_RETRIES = 5

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

def count_by_type_and_state(job_type, state):
    db = init_db('jobs')
    result = db.query(Job).filter_by(job_type=job_type, state=state).count()
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

def select_for_collector(identity):
    db = init_db('jobs')

    collector = db.query(Collector).filter(Collector.identity==identity, Collector.job == None, Collector.state == b'READY').first()

    if collector == None:
        return

    # This is influenced by .update(), which resets the .timestamp to
    # .utcnow(), effectively pushing all updated jobs to the back of the
    # queue.
    #
    # Practical result is a massive amount of metadata gathering
    # followed by a sudden surge of jobs getting DONE.
    #job = db.query(Job).filter_by(collector=identity, job_type='collector', state=b'PENDING').order_by(Job.timestamp).first()

    # This would result in "most recent first, work your way backwards."
    #job = db.query(Job).filter_by(collector=identity, job_type='collector', state=b'PENDING').order_by(Job.timestamp.desc()).first()

    # This results in following the storage order and is by far the
    # fastest methodology.
    job = db.query(Job).filter_by(collector=identity, job_type='collector', state=b'PENDING').first()

    if not job == None:
        job.state = b'ALLOC'
        job.timestamp = datetime.datetime.utcnow()
        db.commit()

    return job

def select_for_worker(identity):
    db = init_db('jobs')

    worker = db.query(Worker).filter(Worker.identity == identity, Worker.job == None, Worker.state == b'READY').first()

    if worker == None:
        return

    # This is influenced by .update(), which resets the .timestamp to
    # .utcnow(), effectively pushing all updated jobs to the back of the
    # queue.
    #
    # Practical result is a massive amount of metadata gathering
    # followed by a sudden surge of jobs getting DONE.
    #job = db.query(Job).filter_by(job_type='worker', state=b'PENDING').order_by(Job.timestamp).first()

    # This would result in "most recent first, work your way backwards."
    #job = db.query(Job).filter_by(job_type='worker', state=b'PENDING').order_by(Job.timestamp.desc()).first()

    # This results in following the storage order and is by far the
    # fastest methodology.
    job = db.query(Job).filter_by(job_type='worker', state=b'PENDING').first()

    if not job == None:
        job.state = b'ALLOC'
        job.timestamp = datetime.datetime.utcnow()
        db.commit()

    return job

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

    if job == None:
        return

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

    for job in db.query(Job).filter(Job.timestamp <= (datetime.datetime.utcnow() - datetime.timedelta(0, 300)), Job.state == b'DONE').all():
        log.debug("Purging job %s" % (job.uuid), level=7)
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

        log.debug("Unlocking %s job %s" % (job.job_type, job.uuid), level=7)
        job.state = b'PENDING'

        for collector in db.query(Collector).filter_by(job=job.id).all():
            collector.job = None

        for worker in db.query(Worker).filter_by(job=job.id).all():
            worker.job = None

    # process postponed jobs
    for job in db.query(Job).filter(
                Job.timestamp <= (datetime.datetime.utcnow() - datetime.timedelta(0, 30)),
                Job.state == b'POSTPONED'
            ).all():

        if job.pushbacks >= MAX_RETRIES:
            log.error("Too many pushbacks for job %s" % (job.uuid))
            job.state = b'FAILED'
        else:
            log.debug("Re-activating postponed job %s" % (job.uuid), level=7)
            job.state = b'PENDING'
            job.pushbacks += 1

    db.commit()
