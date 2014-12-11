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

"""
    This is the ZMQ broker implementation for Bonnie.
"""

import datetime
from dateutil.parser import parse
from dateutil.tz import tzutc
import json
from multiprocessing import Process
import os
import random
import re
import signal
import sys
import threading
import time
import zmq
from zmq.eventloop import ioloop, zmqstream

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.broker.ZMQBroker')

from bonnie.broker.state import init_db

import collector
import job
import worker

class ZMQBroker(object):
    running = False

    def __init__(self):
        """
            A ZMQ Broker for Bonnie.
        """
        self.context = zmq.Context(io_threads=128)
        self.poller = zmq.Poller()

        self.routers = {}
        self.router_processes = {}

        self.collector_jobs_pending = 0

    def register(self, callback):
        callback({ '_all': self.run })

    def create_router(self, name, default, **kw):
        """
            Create a regular router that remains
            available for the base ZMQBroker.
        """
        bind_address = conf.get(
                'broker',
                'zmq_%s_router_bind_address' % (name)
            )

        if bind_address == None:
            bind_address = default

        router = self.context.socket(zmq.ROUTER)
        router.bind(bind_address)

        self.routers[name] = {
                'router': router,
                'callbacks': dict(kw)
            }

        self.poller.register(router, zmq.POLLIN)

        return router

    def create_router_process(self, name, default, callback):
        """
            Create a router process (that is no
            longer available to the base ZMQBroker).
        """

        bind_address = conf.get(
                'broker',
                'zmq_%s_router_bind_address' % (name)
            )

        if bind_address == None:
            bind_address = default

        self.router_processes[name] = Process(
                target=self._run_router_process,
                args=(callback, bind_address)
            )

        self.router_processes[name].start()

    def find_router(self, socket):
        for name, attrs in self.routers.iteritems():
            if attrs['router'] == socket:
                return name, attrs['router'], attrs['callbacks']

    def run(self):
        self.create_router(
                'collector',
                'tcp://*:5571',
                recv_multipart = self._cb_cr_recv_multipart
            )

        self.create_router_process(
                'dealer',
                'tcp://*:5570',
                self._cb_dr_on_recv_stream
            )

        self.create_router(
                'worker',
                'tcp://*:5573',
                recv_multipart = self._cb_wr_recv_multipart
            )

        self.create_router(
                'worker_controller',
                'tcp://*:5572',
                recv_multipart = self._cb_wcr_recv_multipart
            )

        self.running = True
        last_expire = time.time()
        last_state = time.time()
        last_vacuum = time.time()

        try:
            db = init_db('broker')
            db.execute("VACUUM")
            db.commit()
        except Exception, errmsg:
            pass

        poller_timeout = int(conf.get('broker', 'zmq_poller_timeout', 100))

        while self.running:
            try:
                # TODO: adjust polling timout according to the number
                # of pending jobs.
                sockets = dict(self.poller.poll(poller_timeout))

            except KeyboardInterrupt, errmsg:
                log.info("zmq.Poller KeyboardInterrupt")
                break

            except Exception, errmsg:
                log.error("zmq.Poller error: %r", errmsg)
                sockets = dict()

            for socket, event in sockets.iteritems():
                if event == zmq.POLLIN:
                    name, router, callbacks = self.find_router(socket)

                    for callforward, callback in callbacks.iteritems():
                        result = getattr(router, '%s' % callforward)()
                        callback(router, result)

            # Once every 30 seconds, expire stale collectors and
            # workers, unlock jobs and expire those done.
            if last_expire < (time.time() - 30):
                collector.expire()
                worker.expire()
                job.unlock()
                job.expire()

                last_expire = time.time()

            # Report on the state of jobs.
            if last_state < (time.time() - 30):
                self._write_stats()
                last_state = time.time()

        log.info("Shutting down")

        for attrs in self.routers.values():
            attrs['router'].close()

        for proc in self.router_processes.values():
            proc.terminate()

        self.context.term()

    def _cb_cr_recv_multipart(self, router, message):
        """
            Receive a message on the Collector Router.
        """
        log.debug("Collector Router Message: %r" % (message), level=8)
        collector_identity = message[0]
        cmd = message[1]

        if not hasattr(self, '_handle_cr_%s' % (cmd)):
            log.error("Unhandled CR cmd %s" % (cmd))
            return

        handler = getattr(self, '_handle_cr_%s' % (cmd))
        handler(router, collector_identity, message[2:])

    def _cb_dr_on_recv_stream(self, stream, message):
        """
            Callback on the Dealer Router process.

            Responds as fast as possible.
        """
        log.debug("Dealer Router Message: %r" % (message), level=8)
        dealer_identity = message[0]
        notification = message[1]

        done = False
        attempts = 0
        while not done:
            attempts += 1
            try:
                _job = job.add(dealer_identity, notification)
                if not _job == None:
                    done = True
            except Exception, errmsg:
                if attempts % 10 == 0:
                    log.error("Dealer Router cannot add jobs: %r" % (errmsg))

        stream.send_multipart([dealer_identity, b'ACK'])

        if not _job == None:
            log.info("Job %s NEW by %s" % (_job.uuid, dealer_identity))

    def _cb_wr_recv_multipart(self, router, message):
        log.debug("Worker Router Message: %r" % (message), level=8)
        worker_identity = message[0]
        cmd = message[1]

        if not hasattr(self, '_handle_wr_%s' % (cmd)):
            log.error("Unhandled WR cmd %s" % (cmd))
            return

        handler = getattr(self, '_handle_wr_%s' % (cmd))
        handler(router, worker_identity, message[2:])

    def _cb_wcr_recv_multipart(self, router, message):
        log.debug("Worker Controller Router Message: %r" % (message), level=8)
        worker_identity = message[0]
        cmd = message[1]

        if not hasattr(self, '_handle_wcr_%s' % (cmd)):
            log.error("Unknown WCR cmd %s for job %s" % (cmd, message[2]))
            self._handle_wcr_UNKNOWN(router, worker_identity, message[2:])
            return

        handler = getattr(self, '_handle_wcr_%s' % (cmd))
        handler(router, worker_identity, message[2:])

    ##
    ## Collector Router command functions
    ##

    def _handle_cr_DONE(self, router, identity, message):
        """
            A collector has indicated it is done performing a job, or
            actually a command for a job.
        """
        if conf.debuglevel > 7:
            log.debug("Handling DONE for identity %s (message: %r)" % (identity, message), level=8)
        elif conf.debuglevel > 6:
            log.debug("Handling DONE for identity %s (message: %r)" % (identity, message[:-1]), level=7)

        job_uuid = message[0]
        log.info("Job %s DONE by %s" % (job_uuid, identity))

        notification = message[1]

        job.update(
                job_uuid,
                state = b'PENDING',
                job_type = 'worker',
                notification = notification,
                cmd = None
            )

        collector.update(
                identity,
                state = b'READY',
                job = None
            )

        self._send_collector_job(identity)

    def _handle_cr_STATE(self, router, identity, message):
        """
            A collector is reporting its state.
        """
        log.debug("Handling STATE for identity %s (message: %r)" % (identity, message), level=7)

        state = message[0]
        interests = message[1].split(" ")

        collector.set_state(identity, state, interests)

        if state == b'READY':
            self._send_collector_job(identity)

    ##
    ## Worker Controller Router command functions
    ##

    def _handle_wcr_DONE(self, router, identity, message):
        log.debug("Handing DONE for identity %s (message: %r)" % (identity, message), level=7)
        job_uuid = message[0]
        log.info("Job %s DONE by %s" % (job_uuid, identity))
        job.update(
                job_uuid,
                state = b'DONE'
            )

        worker.update(
                identity,
                state = b'READY',
                job = None
            )

        self._send_worker_job(identity)

    def _handle_wcr_COLLECT(self, router, identity, message):
        log.debug("Handing COLLECT for identity %s (message: %r)" % (identity, message[:-1]), level=7)
        commands = message[0]
        job_uuid = message[1]
        notification = message[2]

        log.info("Job %s COLLECT by %s" % (job_uuid, identity))

        job.update(
                job_uuid,
                cmd = commands,
                state = b'PENDING',
                job_type = 'collector',
                notification = notification
            )

        worker.update(
                identity,
                state = b'READY',
                job = None
            )

        self._send_worker_job(identity)

    def _handle_wcr_POSTPONE(self, router, identity, message):
        log.debug("Handing POSTPONE for identity %s (message: %r)" % (identity, message), level=7)
        job_uuid = message[0]
        log.info("Job %s POSTPONE by %s" % (job_uuid, identity))

        job.update(
                job_uuid,
                state = b'POSTPONED'
            )

        worker.update(
                identity,
                state = b'READY',
                job = None
            )

        self._send_worker_job(identity)

    def _handle_wcr_STATE(self, router, identity, message):
        log.debug("Handing STATE for identity %s (message: %r)" % (identity, message), level=7)
        state = message[0]

        if state == b'BUSY':
            job_uuid = message[1]
            _job = job.select(job_uuid)

            if not _job == None:
                _job_id = _job.id
            else:
                _job_id = None
                state = b'READY'

            worker.update(
                    identity,
                    state = state,
                    job = _job_id
                )
        else:
            worker.update(
                    identity,
                    state = state,
                    job = None
                )

        if state == b'READY':
            self._send_worker_job(identity)

    def _handle_wcr_UNKNOWN(self, router, identity, message):
        job_uuid = message[0]
        job.update(
                job_uuid,
                state = b'FAILED'
            )

        worker.update(
                identity,
                state = b'READY',
                job = None
            )

    ##
    ## Worker Router command functions
    ##

    def _handle_wr_GET(self, router, identity, message):
        log.debug("Handing GET for worker %s (message: %r)" % (identity, message), level=7)
        job_uuid = message[0]
        _job = job.select(job_uuid)

        router.send_multipart(
                [
                        identity,
                        b'JOB',
                        (_job.uuid).encode('ascii'),
                        (_job.notification).encode('ascii')
                    ]
            )

    def _run_router_process(self, callback, bind_address):
        """
            Run a multiprocessing.Process with this function
            as a target.
        """

        router = zmq.Context().socket(zmq.ROUTER)
        router.bind(bind_address)

        stream = zmqstream.ZMQStream(router)
        stream.on_recv_stream(callback)

        # catch sigterm and terminate the ioloop
        def _terminate(*args, **kw):
            log.info("ioloop.IOLoop shutting down")
            ioloop.IOLoop.instance().stop()

        signal.signal(signal.SIGTERM, _terminate)

        try:
            ioloop.IOLoop.instance().start()
        except KeyboardInterrupt, e:
            log.info("ioloop.IOLoop KeyboardInterrupt")
        except Exception, e:
            log.error("ioloop.IOLoop error: %r", e)
        finally:
            ioloop.IOLoop.instance().stop()

    def _send_collector_job(self, identity):
        _job = job.select_for_collector(identity)

        if _job == None:
            return

        log.debug("Sending %s to %s" % (_job.uuid, identity), level=7)

        self.routers['collector']['router'].send_multipart(
                [
                        (_job.collector).encode('ascii'),
                        (_job.cmd).encode('ascii'),
                        (_job.uuid).encode('ascii'),
                        (_job.notification).encode('ascii')
                    ]
            )

    def _send_worker_job(self, identity):
        _job = job.select_for_worker(identity)

        if _job == None:
            return

        log.debug("Sending %s to %s" % (_job.uuid, identity), level=7)

        self.routers['worker_controller']['router'].send_multipart(
                [
                        (identity).encode('ascii'),
                        b'TAKE',
                        (_job.uuid).encode('ascii')
                    ]
            )

    def _write_stats(self):
        job_retention = (int)(conf.get(
                "broker",
                "job_retention",
                300
            ))

        stats_start = time.time()

        jcp = job.count_by_type_and_state('collector', b'PENDING')
        jwp = job.count_by_type_and_state('worker', b'PENDING')
        jca = job.count_by_type_and_state('collector', b'ALLOC')
        jwa = job.count_by_type_and_state('worker', b'ALLOC')

        _job = job.first()
        _job_notification = False

        if _job == None:
            jt = 0
        else:
            while _job_notification == False:
                try:
                    _job_notification = json.loads(_job.notification)
                except Exception, errmsg:
                    job.set_state(_job.uuid, b'FAILED')
                    _job = job.first()
                    _job_notification == False

            _job_timestamp = parse(_job_notification['timestamp']).astimezone(tzutc())
            now = parse(
                    datetime.datetime.strftime(
                            datetime.datetime.utcnow(),
                            "%Y-%m-%dT%H:%M:%S.%fZ"
                        )
                ).astimezone(tzutc())

            delta = now - _job_timestamp
            if hasattr(delta, 'total_seconds'):
                seconds = delta.total_seconds()
            else:
                seconds = (delta.days * 24 * 3600) + delta.seconds

            jt = round(seconds, 0)

        stats = {
                'cb': collector.count_by_state(b'BUSY'),
                'cr': collector.count_by_state(b'READY'),
                'cs': collector.count_by_state(b'STALE'),
                'ja': sum([jca, jwa]),
                'jca': jca,
                'jcp': jcp,
                'jd': job.count_by_state(b'DONE'),
                'jf': job.count_by_state(b'FAILED'),
                'jo': job.count_by_state(b'POSTPONED'),
                'jp': sum([jcp, jwp]),
                'jr': job_retention,
                'jt': jt,
                'jwa': jwa,
                'jwp': jwp,
                'wb': worker.count_by_state(b'BUSY'),
                'wr': worker.count_by_state(b'READY'),
                'ws': worker.count_by_state(b'STALE'),
            }

        stats_end = time.time()

        stats['duration'] = "%.4f" % (stats_end - stats_start)

        self._write_stats_file(stats)
        self._write_stats_log(stats)

    def _write_stats_file(self, stats):
        fp = open("/var/lib/bonnie/state.stats", "w")
        fp.write("""# Source this file in your script
                job_retention=%(jr)d
                jobs_done=%(jd)d
                jobs_pending=%(jp)d
                jobs_alloc=%(ja)d
                jobs_postponed=%(jo)d
                jobs_failed=%(jf)d
                jobs_lag=%(jt)d
                workers_ready=%(wr)d
                workers_busy=%(wb)d
                workers_stale=%(ws)d
                collectors_ready=%(cr)d
                collectors_busy=%(cb)d
                collectors_stale=%(cs)d
                collector_jobs_pending=%(jcp)d
                collector_jobs_alloc=%(jca)d
                worker_jobs_pending=%(jwp)d
                worker_jobs_alloc=%(jwa)d
                """.replace('    ', '') % (stats))
        fp.close()

    def _write_stats_log(self, stats):
        log.info("""
            Jobs:       done=%(jd)d, pending=%(jp)d, alloc=%(ja)d,
                        postponed=%(jo)d, failed=%(jf)d.
            Workers:    ready=%(wr)d, busy=%(wb)d, stale=%(ws)d,
                        pending=%(jwp)d, alloc=%(jwa)d.
            Collectors: ready=%(cr)d, busy=%(cb)d, stale=%(cs)d,
                        pending=%(jcp)d, alloc=%(jca)d.
            Took:       seconds=%(duration)s.""" % stats)

    def _request_collector_state(self, identity):
        log.debug("Requesting state from %s" % (identity), level=7)
        self.routers['collector']['router'].send_multipart([identity.encode('ascii'), b"STATE"])
