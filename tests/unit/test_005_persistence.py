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

import os
import json
import bonnie
from bonnie.broker import persistence
from bonnie.broker.persistence import db, PersistentBase
from bonnie.broker.brokers.zmq_broker.job import Job
from twisted.trial import unittest

class PlistItem(PersistentBase):
    __tablename__ = 'plisttest'
    id = db.Column(db.Integer, primary_key=True)
    value = db.Column(db.String)

    def __init__(self, value, id=None):
        self.value = value
        self.id = id

    def __repr__(self):
        return '<PlistItem %s:%r>' % (self.id, self.value)


class TestBonniePersistence(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        # TODO: clear database
        # PersistentBase.metadata.drop_all(bonnie.broker.persistence.engine)
        pass

    def test_001_base_list(self):
        plist = persistence.List('base', PlistItem)
        plist.append(PlistItem("One"))
        plist.append(PlistItem("Two"))
        item3 = PlistItem("Three")
        plist.append(item3)

        self.assertEqual(len(plist), 3)
        self.assertEqual(plist[2], item3)
        self.assertEqual(2, plist.index(item3))
        self.assertTrue(item3 in plist)

        plist.append(PlistItem("Five"))
        plist.append(PlistItem("Six"))

        plist[4] = PlistItem("Five.5")
        self.assertEqual(plist[4].value, "Five.5")

        del plist[plist.index(item3)]
        plist.pop(3)
        plist.pop()
        self.assertEqual(len(plist), 2)

        i = 0
        for item in plist:
            i += 1
            self.assertTrue(isinstance(item, PlistItem))

        self.assertEqual(i, 2)


    def test_003_broker_jobs(self):
        worker_jobs = persistence.List('worker', Job)
        collector_jobs = persistence.List('collector', Job)
        one = Job(state='PENDING', notification='{"state":"pending","event":"test"}', collector_id='C.1')
        two = Job(state='PENDING', notification='{"state":"pending","event":"other"}', collector_id='C.1')
        done = Job(state='DONE', notification='{"state":"done","event":"passed"}', collector_id='C.1')
        worker_jobs.append(one)
        worker_jobs.append(two)
        worker_jobs.append(done)

        self.assertEqual(len(worker_jobs), 3)
        self.assertEqual(len(collector_jobs), 0)
        self.assertTrue(one.uuid in [x.uuid for x in worker_jobs])
        pending = [x for x in worker_jobs if x.state == 'PENDING' and x.collector_id == 'C.1']
        self.assertEqual(len(pending), 2)
        job = pending.pop()

        self.assertEqual(job, two)
        self.assertEqual(len(pending), 1)
        self.assertEqual(len(worker_jobs), 3)

        # move job to another list
        collector_jobs.append(one)
        self.assertTrue(one in collector_jobs)
        self.assertTrue(one in worker_jobs)

        worker_jobs.remove(one)
        self.assertFalse(one in worker_jobs)

        self.assertEqual(len(worker_jobs), 2)
        self.assertEqual(len(collector_jobs), 1)

        # move job to the end of the queue
        worker_jobs.remove(two)
        worker_jobs.append(two)

        self.assertEqual(worker_jobs[0], done)
        self.assertEqual(worker_jobs[-1], two)
