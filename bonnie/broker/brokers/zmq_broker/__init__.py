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

"""
    This is the broker for Bonnie.
"""

import copy
import random
import sys
import time
import zmq

import bonnie
conf = bonnie.getConf()

from job import Job
from collector import Collector
from worker import Worker

class ZMQBroker(object):
    def __init__(self):
        self.worker_jobs = []
        self.collect_jobs = []
        self.collectors = {}
        self.workers = {}

    def register(self, callback):
        callback({ '_all': self.run })

    def collector_job(self, _collector_id):
        jobs = [x for x in self.collect_jobs if x.collector_id == _collector_id and x.state == b"PENDING"]

        if len(jobs) < 1:
            return

        job = jobs.pop()
        job.set_state(b"ALLOC")
        return job

    def worker_job_add(self, _notification, client_id=None):
        """
            Add a new job.
        """
        job = Job(
                notification=_notification,
                state=b"PENDING",
                worker=None,
                client_id=client_id
            )

        if not job.uuid in [x.uuid for x in self.worker_jobs]:
            self.worker_jobs.append(job)

        print "new job: %s" % (job.uuid)

    def worker_job_allocate(self, _worker_id):
        """
            Allocate a job to a worker, should a job be available.
        """

        if len(self.worker_jobs) < 1:
            return None

        jobs = self.worker_jobs_with_status(b"PENDING")
        if len(jobs) < 1:
            return None

        job = jobs.pop()

        job.set_state(b"ALLOC")
        job.set_worker(self.workers[_worker_id])

        return job.uuid

    def worker_job_done(self, _job_uuid):
        for job in [x for x in self.worker_jobs if x.uuid == _job_uuid]:
            del self.worker_jobs[self.worker_jobs.index(job)]
        print "done job", _job_uuid

    def worker_job_free(self, _job_uuid):
        for job in [x for x in self.worker_jobs if x.uuid == _job_uuid]:
            job.set_status(b"PENDING")
            job.set_worker(None)

    def worker_job_send(self, _job_uuid, _worker_id):
        # TODO: Sanity check on job state, worker assignment, etc.
        for job in [x for x in self.worker_jobs if x.uuid == _job_uuid]:
            self.worker_router.send_multipart([_worker_id, b"JOB", _job_uuid, job.notification])

            print "sent job", _job_uuid, "to worker", _worker_id

    def worker_jobs_with_status(self, _state):
        return [x for x in self.worker_jobs if x.state == _state]

    def worker_jobs_with_worker(self, _worker_id):
        return [x for x in self.worker_jobs if x.worker == self.workers[_worker_id]]

    def worker_add(self, _worker_id, _state):
        print "adding worker", _worker_id

        worker = Worker(_worker_id, _state)
        self.workers[_worker_id] = worker

    def worker_set_status(self, _worker_id, _state):
        if not self.workers.has_key(_worker_id):
            self.worker_add(_worker_id, _state)
        else:
            self.workers[_worker_id].set_status(_state)

    def workers_expire(self):
        delete_workers = []
        for worker_id, worker in self.workers.iteritems():
            if worker.timestamp < (time.time() - 10):
                self.controller.send_multipart([worker_id, b"STATE"])

            if worker.timestamp < (time.time() - 30):
                if worker.state == b"READY":
                    self.worker_set_status(worker_id, b"STALE")
                elif worker.state == b"BUSY":
                    self.worker_set_status(worker_id, b"STALE")
                else:
                    delete_workers.append(worker_id)

            for job in self.worker_jobs_with_worker(worker_id):
                self.worker_job_free(job)

        for worker in delete_workers:
            print "deleting worker", worker
            del self.workers[worker]

    def workers_with_status(self, _state):
        return [worker_id for worker_id, worker in self.workers.iteritems() if worker.state == _state]

    def run(self):
        context = zmq.Context()

        client_router_bind_address = conf.get('broker', 'zmq_client_router_bind_address')

        if client_router_bind_address == None:
            client_router_bind_address = "tcp://*:5570"

        client_router = context.socket(zmq.ROUTER)
        client_router.bind(client_router_bind_address)

        collector_router_bind_address = conf.get('broker', 'zmq_collector_router_bind_address')

        if collector_router_bind_address == None:
            collector_router_bind_address = "tcp://*:5571"

        self.collector_router = context.socket(zmq.ROUTER)
        self.collector_router.bind(collector_router_bind_address)

        controller_bind_address = conf.get('broker', 'zmq_controller_bind_address')

        if controller_bind_address == None:
            controller_bind_address = "tcp://*:5572"

        self.controller = context.socket(zmq.ROUTER)
        self.controller.bind(controller_bind_address)

        worker_router_bind_address = conf.get('broker', 'zmq_worker_router_bind_address')

        if worker_router_bind_address == None:
            worker_router_bind_address = "tcp://*:5573"

        self.worker_router = context.socket(zmq.ROUTER)
        self.worker_router.bind(worker_router_bind_address)

        poller = zmq.Poller()
        poller.register(client_router, zmq.POLLIN)
        poller.register(self.collector_router, zmq.POLLIN)
        poller.register(self.worker_router, zmq.POLLIN)
        poller.register(self.controller, zmq.POLLIN)

        while True:
            sockets = dict(poller.poll(1000))
            self.workers_expire()

            if self.controller in sockets:
                if sockets[self.controller] == zmq.POLLIN:
                    _message = self.controller.recv_multipart()
                    print _message
                    _worker_id = _message[0]

                    if _message[1] == b"STATE":
                        _state = _message[2]
                        self.worker_set_status(_worker_id, _state)

                    if _message[1] == b"DONE":
                        self.worker_job_done(_message[2])

                    if _message[1] == b"RETRIEVE":
                        _job_uuid = _message[2]
                        self.transit_job_collect(_job_uuid)

            if client_router in sockets:
                if sockets[client_router] == zmq.POLLIN:
                    _message = client_router.recv_multipart()
                    print _message
                    _client_id = _message[0]
                    _notification = _message[1]
                    self.worker_job_add(_notification, client_id=_client_id)

                    client_router.send_multipart([_message[0], b"ACK"])

            if self.collector_router in sockets:
                if sockets[self.collector_router] == zmq.POLLIN:
                    _message = self.collector_router.recv_multipart()
                    print _message

                    if _message[1] == b"READY":
                        collector_id = _message[0]
                        job = self.collector_job(collector_id)
                        if not job == None:
                            self.collector_router.send_multipart([collector_id, job.uuid, job.notification])

                    if _message[1] == b"DONE":
                        _collector_id = _message[0]
                        _notification = _message[2]
                        self.worker_job_add(_notification, collector_id=_collector_id)

            if self.worker_router in sockets:
                if sockets[self.worker_router] == zmq.POLLIN:
                    _message = self.worker_router.recv_multipart()
                    print _message
                    _worker_id = _message[0]
                    _command = _message[1]
                    _job_uuid = _message[2]
                    self.worker_job_send(_job_uuid, _worker_id)

            ready_workers = self.workers_with_status(b"READY")
            pending_jobs = self.worker_jobs_with_status(b"PENDING")

            if len(pending_jobs) > 0 and len(ready_workers) > 0:
                _worker_id = random.choice(ready_workers)
                _job_uuid = self.worker_job_allocate(_worker_id)

                if not _job_uuid == None:
                    self.controller.send_multipart([_worker_id, b"TAKE", _job_uuid])

        client_router.close()
        self.controller.close()
        self.collector_router.close()
        self.worker_router.close()
        context.term()

    def transit_job_collect(self, _job_uuid):
        for job in [x for x in self.worker_jobs if x.uuid == _job_uuid]:
            self.collect_jobs.append(job)
            del self.worker_jobs[self.worker_jobs.index(job)]

