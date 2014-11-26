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

from multiprocessing import Process
import random
import re
import sys
import threading
import time
import zmq
from zmq.eventloop import ioloop, zmqstream

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.broker.ZMQBroker')

import collector
import job
import worker

class ZMQBroker(object):
    def __init__(self):
        """
            A ZMQ Broker for Bonnie.
        """
        self.context = zmq.Context(io_threads=128)
        self.poller = zmq.Poller()

        self.routers = {}

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

        setattr(
                self,
                '%s_router' % (name),
                Process(
                        target=self._run_router_process,
                        args=(callback, bind_address)
                    )
            )

        getattr(self, '%s_router' % (name)).start()

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

        last_expire = time.time()
        last_state = time.time()

        while True:
            # TODO: adjust polling timout according to the number of pending jobs
            sockets = dict(self.poller.poll(100))

            for socket, event in sockets.iteritems():
                if event == zmq.POLLIN:
                    name, router, callbacks = self.find_router(socket)

                    for callforward, callback in callbacks.iteritems():
                        result = getattr(router, '%s' % callforward)()
                        callback(router, result)

            for _collector in collector.select_by_state(b'READY'):
                self._send_collector_job(_collector.identity)

            for _worker in worker.select_by_state(b'READY'):
                self._send_worker_job(_worker.identity)

            if last_expire < (time.time() - 30):
                collector.expire()
                worker.expire()
                job.unlock()
                job.expire()
                last_expire = time.time()

            if last_state < (time.time() - 10):
                stats_start = time.time()
                jcp = job.count_by_type_and_state('collector', b'PENDING')
                jwp = job.count_by_type_and_state('worker', b'PENDING')
                jca = job.count_by_type_and_state('collector', b'ALLOC')
                jwa = job.count_by_type_and_state('worker', b'ALLOC')

                stats = {
                        'cb': collector.count_by_state(b'BUSY'),
                        'cr': collector.count_by_state(b'READY'),
                        'cs': collector.count_by_state(b'STALE'),
                        'ja': sum([jca, jwa]),
                        'jca': jca,
                        'jcp': jcp,
                        'jd': job.count_by_state(b'DONE'),
                        'jp': sum([jcp, jwp]),
                        'jwa': jwa,
                        'jwp': jwp,
                        'jo': job.count_by_state(b'ORPHANED'),
                        'wb': worker.count_by_state(b'BUSY'),
                        'wr': worker.count_by_state(b'READY'),
                        'ws': worker.count_by_state(b'STALE'),
                    }
                stats_end = time.time()

                stats['duration'] = "%.4f" % (stats_end - stats_start)

                log.info("""
    Jobs:       done=%(jd)d, pending=%(jp)d, alloc=%(ja)d,
                orphaned=%(jo)d.
    Workers:    ready=%(wr)d, busy=%(wb)d, stale=%(ws)d,
                pending=%(jwp)d, alloc=%(jwa)d.
    Collectors: ready=%(cr)d, busy=%(cb)d, stale=%(cs)d,
                pending=%(jcp)d, alloc=%(jca)d.
    Took:       seconds=%(duration)s.""" % stats

                log.info(info)

                last_state = time.time()

    def _cb_cr_recv_multipart(self, router, message):
        """
            Receive a message on the Collector Router.
        """
        log.debug("Collector Router Message: %r" % (message), level=8)
        collector_identity = message[0]
        cmd = message[1]

        if not hasattr(self, '_handle_cr_%s' % (cmd)):
            log.error("Unhandled CR cmd %s" % (cmd))

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
        stream.send_multipart([dealer_identity, b'ACK'])

        job.add(dealer_identity, notification)

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
            log.error("Unhandled WCR cmd %s" % (cmd))
            self._handle_wcr_unknown(router, identity, message[2:])
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
        interests = message[1].split(",")

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
        log.debug("Handing COLLECT for identity %s (message: %r)" % (identity, message), level=7)
        job_uuid = message[0]
        log.info("Job %s COLLECT by %s" % (job_uuid, identity))

        updates = dict(
                cmd = message[1],
                state = b'PENDING',
                job_type = 'collector'
            )

        if len(message) > 2:
            updates['notification'] = message[2]

        job.update(
                job_uuid,
                **updates
            )

        worker.update(
                identity,
                state = b'READY',
                job = None
            )

        self._send_worker_job(identity)

    def _handle_wcr_PUSHBACK(self, router, identity, message):
        log.debug("Handing PUSHBACK for identity %s (message: %r)" % (identity, message), level=8)
        job_uuid = message[0]
        _job = job.select(job_uuid)

        if _job is not None and _job.pushbacks < 5:
            job.update(
                    job_uuid,
                    state = b'PENDING',
                    pushbacks = _job.pushbacks + 1
                )
        else:
            log.error("Job %s pushed back too many times" % (job_uuid))
            job.update(
                    job_uuid,
                    state = b'ORPHANED'
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

    def _handle_wcr_unknown(self, router, identity, message):
        job_uuid = message[0]
        job.update(
                job_uuid,
                state = b'ORPHANED'
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
        ioloop.IOLoop.instance().start()

    def _send_collector_job(self, identity):
        _job = job.select_for_collector(identity)

        if _job == None:
            return

        collector.update(
                identity,
                state = b'BUSY',
                job = _job.id
            )

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

        worker.update(
                identity,
                state = b'BUSY',
                job = _job.id
            )

        log.debug("Sending %s to %s" % (_job.uuid, identity), level=7)

        self.routers['worker_controller']['router'].send_multipart(
                [
                        (identity).encode('ascii'),
                        b'TAKE',
                        (_job.uuid).encode('ascii')
                    ]
            )
