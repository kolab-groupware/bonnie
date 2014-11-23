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

def add(dealer, notification, job_type='worker'):
    """
        Add a new job.
    """
    db = init_db('jobs')
    db.add(Job(dealer, notification, job_type))
    db.commit()

def count_by_state(state):
    db = init_db('jobs')
    result = db.query(Job).filter_by(state=state).count()
    return result

def count_by_type(job_type):
    db = init_db('jobs')
    result = db.query(Job).filter_by(job_type=job_type).all()
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

def set_state(uuid, state):
    db = init_db('jobs')
    for x in db.query(Job).filter_by(uuid=uuid).all():
        x.state = state
    db.commit()

def unlock():
    """
        Unlock jobs that have been allocated to some worker way too long
        ago.
    """
    db = init_db('jobs')

    for job in db.query(Job).filter(Job.timestamp <= (datetime.datetime.utcnow() - datetime.timedelta(0, 120)), Job.state == b'ALLOC').all():
        job.state = b'PENDING'

        for worker in db.query(Worker).filter_by(job=job.id).all():
            worker.job = None

    db.commit()
