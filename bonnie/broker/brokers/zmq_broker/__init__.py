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
    This is the ZMQ broker implementation for Bonnie.
"""

import copy
import random
import re
import sys
import time
import zmq

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.broker.ZMQBroker')

from job import Job
from collector import Collector
from worker import Worker
from bonnie.broker import persistence

class ZMQBroker(object):
    MAX_RETRIES = 5
    running = False

    def __init__(self):
        self.worker_jobs = persistence.List('worker_jobs', Job)
        self.collect_jobs = persistence.List('collect_jobs', Job)
        self.collectors = {}
        self.workers = {}
        self.collector_interests = []

    def register(self, callback):
        callback({ '_all': self.run })

    def collector_add(self, _collector_id, _state, _interests):
        log.debug("Adding collector %s for %r" % (_collector_id, _interests), level=5)

        collector = Collector(_collector_id, _state, _interests)
        self.collectors[_collector_id] = collector

        # regisrer the reported interests
        if len(_interests) > 0:
            _interests.extend(self.collector_interests)
            self.collector_interests = list(set(_interests))

    def collect_job_allocate(self, _collector_id):
        jobs = [x for x in self.collect_jobs if x.collector_id == _collector_id and x.state == b"PENDING"]

        if len(jobs) < 1:
            return

        job = jobs.pop()
        job.set_status(b"ALLOC")
        return job

    def collect_jobs_with_status(self, _state, collector_id=None):
        return [x for x in self.collect_jobs if x.state == _state and x.collector_id == collector_id]

    def collector_set_status(self, _collector_id, _state, _interests):
        if not self.collectors.has_key(_collector_id):
            self.collector_add(_collector_id, _state, _interests)
        else:
            self.collectors[_collector_id].set_status(_state)

    def collectors_with_status(self, _state):
        return [collector_id for collector_id, collector in self.collectors.iteritems() if collector.state == _state]

    def worker_job_add(self, _notification, client_id=None, collector_id=None):
        """
            Add a new job.
        """
        job = Job(
                notification=_notification,
                state=b"PENDING",
                client_id=client_id,
                collector_id=collector_id,
            )

        if not job.uuid in [x.uuid for x in self.worker_jobs]:
            self.worker_jobs.append(job)

        log.debug("New worker job: %s; client=%s, collector=%s" % (job.uuid, client_id, collector_id), level=8)

    def worker_job_allocate(self, _worker_id):
        """
            Allocate a job to a worker, should a job be available.
        """

        if len(self.worker_jobs) < 1:
            return None

        jobs = self.worker_jobs_with_status(b"PENDING")
        if len(jobs) < 1:
            return None

        # take the first job in the queue
        job = jobs[0]

        job.set_status(b"ALLOC")
        job.set_worker(_worker_id)

        return job.uuid

    def worker_job_done(self, _job_uuid):
        for job in [x for x in self.worker_jobs if x.uuid == _job_uuid]:
            self.worker_jobs.delete(job)
        log.debug("Worker job done: %s;" % (_job_uuid), level=8)

    def worker_job_free(self, _job_uuid, pushback=False):
        for job in [x for x in self.worker_jobs if x.uuid == _job_uuid]:
            job.set_status(b"PENDING")
            job.set_worker(None)

            if pushback:
                # increment retry count on pushback
                job.retries += 1
                log.debug("Push back job %s for %d. time" % (_job_uuid, job.retries), level=8)
                if job.retries > self.MAX_RETRIES:
                    # delete job after MAX retries
                    self.worker_jobs.delete(job)
                    log.info("Delete pushed back job %s" % (_job_uuid))
                else:
                    # move it to the end of the job queue
                    self.worker_jobs.remove(job)
                    self.worker_jobs.append(job)

    def worker_job_send(self, _job_uuid, _worker_id):
        # TODO: Sanity check on job state, worker assignment, etc.
        for job in [x for x in self.worker_jobs if x.uuid == _job_uuid]:
            self.worker_router.send_multipart([_worker_id, b"JOB", _job_uuid, job.notification])

            log.debug("Sent job %s to worker %s;" % (_job_uuid, _worker_id), level=8)

    def worker_jobs_with_status(self, _state):
        return [x for x in self.worker_jobs if x.state == _state]

    def worker_jobs_with_worker(self, _worker_id):
        return [x for x in self.worker_jobs if x.worker_id == _worker_id]

    def worker_add(self, _worker_id, _state):
        log.debug("Adding worker %s (%s)" % (_worker_id, _state), level=5)

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
            log.debug("Deleting worker %s" % (worker), level=5)
            del self.workers[worker]

    def workers_with_status(self, _state):
        return [worker_id for worker_id, worker in self.workers.iteritems() if worker.state == _state]

    def run(self):
        log.info("Starting")
        self.running = True

        context = zmq.Context()

        dealer_router_bind_address = conf.get('broker', 'zmq_dealer_router_bind_address')

        if dealer_router_bind_address == None:
            dealer_router_bind_address = "tcp://*:5570"

        dealer_router = context.socket(zmq.ROUTER)
        dealer_router.bind(dealer_router_bind_address)

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
        poller.register(dealer_router, zmq.POLLIN)
        poller.register(self.collector_router, zmq.POLLIN)
        poller.register(self.worker_router, zmq.POLLIN)
        poller.register(self.controller, zmq.POLLIN)

        # reset existing jobs in self.worker_jobs and self.collect_jobs to status PENDING (?)
        # this will re-assign them to workers and collectors after a broker restart
        for job in self.worker_jobs:
            job.set_status(b"PENDING")

        for job in self.collect_jobs:
            job.set_status(b"PENDING")

        persistence.syncronize()

        while self.running:
            try:
                sockets = dict(poller.poll(1000))
            except KeyboardInterrupt, e:
                log.info("zmq.Poller KeyboardInterrupt")
                break
            except Exception, e:
                log.error("zmq.Poller error: %r", e)
                sockets = dict()

            self.workers_expire()

            if self.controller in sockets:
                if sockets[self.controller] == zmq.POLLIN:
                    _message = self.controller.recv_multipart()
                    log.debug("Controller message: %r" % (_message), level=9)

                    if _message[1] == b"STATE":
                        _worker_id = _message[0]
                        _state = _message[2]
                        self.worker_set_status(_worker_id, _state)

                    if _message[1] == b"DONE":
                        self.worker_job_done(_message[2])

                    if _message[1] == b"PUSHBACK":
                        self.worker_job_free(_message[2], True)

                    if _message[1] in self.collector_interests:
                        _job_uuid = _message[2]
                        self.transit_job_collect(_job_uuid, _message[1])

            if dealer_router in sockets:
                if sockets[dealer_router] == zmq.POLLIN:
                    _message = dealer_router.recv_multipart()
                    log.debug("Dealer message: %r" % (_message), level=9)

                    _client_id = _message[0]
                    _notification = _message[1]
                    _collector_id = _client_id.replace('Dealer', 'Collector')
                    _collector_id = re.sub(r'-[0-9]+$', '', _collector_id)
                    self.worker_job_add(_notification, client_id=_client_id, collector_id=_collector_id)

                    dealer_router.send_multipart([_message[0], b"ACK"])

            if self.collector_router in sockets:
                if sockets[self.collector_router] == zmq.POLLIN:
                    _message = self.collector_router.recv_multipart()
                    log.debug("Collector message: %r" % (_message), level=9)

                    if _message[1] == b"STATE":
                        _collector_id = _message[0]
                        _state = _message[2]
                        _interests = _message[3]
                        self.collector_set_status(_collector_id, _state, _interests.split(","))

                    if _message[1] == b"DONE":
                        _collector_id = _message[0]
                        _job_uuid = _message[2]
                        _notification = _message[3]
                        self.transit_job_worker(_job_uuid, _notification=_notification)

            if self.worker_router in sockets:
                if sockets[self.worker_router] == zmq.POLLIN:
                    _message = self.worker_router.recv_multipart()
                    log.debug("Worker message: %r" % (_message), level=9)

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

            ready_collectors = self.collectors_with_status(b"READY")

            for collector in ready_collectors:
                pending_jobs = self.collect_jobs_with_status(b"PENDING", collector_id=collector)
                if len(pending_jobs) > 0:
                    job = self.collect_job_allocate(collector)
                    self.collector_router.send_multipart([job.collector_id, job.command, job.uuid, job.notification])

            # synchronize job lists to persistent storage
            persistence.syncronize()


        log.info("Shutting down")

        persistence.syncronize()
        dealer_router.close()
        self.controller.close()
        self.collector_router.close()
        self.worker_router.close()
        context.term()

    def transit_job_collect(self, _job_uuid, _command):
        for job in [x for x in self.worker_jobs if x.uuid == _job_uuid]:
            job.set_status(b"PENDING")
            job.set_command(_command)
            self.collect_jobs.append(job)
            self.worker_jobs.remove(job)

    def transit_job_worker(self, _job_uuid, _notification):
        for job in [x for x in self.collect_jobs if x.uuid == _job_uuid]:
            job.set_status(b"PENDING")
            job.notification = _notification
            self.worker_jobs.append(job)
            self.collect_jobs.remove(job)

