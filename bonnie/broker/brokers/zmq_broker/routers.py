import multiprocessing
import time
import zmq
from zmq.eventloop import ioloop, zmqstream

__all__ = [
        'CollectorRouter',
        'DealerRouter',
        'WorkerRouter',
        'WorkerControllerRouter'
    ]

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.broker.ZMQBroker')

class Router(multiprocessing.Process):
    def __init__(self, *args, **kw):
        self.context = None
        self.loop = None

        self.callbacks = {}

        if kw.has_key('on_recv'):
            self.callbacks['on_recv'] = kw['on_recv']
            del kw['on_recv']

        if kw.has_key('on_send'):
            self.callbacks['on_send'] = kw['on_send']
            del kw['on_send']

        if kw.has_key('on_recv_stream'):
            self.callbacks['on_recv_stream'] = kw['on_recv_stream']
            del kw['on_recv_stream']

        if kw.has_key('on_send_stream'):
            self.callbacks['on_send_stream'] = kw['on_send_stream']
            del kw['on_send_stream']

        super(Router, self).__init__(*args, **kw)

    def run(self, *args, **kw):
        self.router = zmq.Context().socket(zmq.ROUTER)
        self.router.bind(self.bind_address)

        self.stream = zmqstream.ZMQStream(self.router)

        if self.callbacks.has_key('on_recv'):
            self.stream.on_recv(self.callbacks['on_recv'])

        if self.callbacks.has_key('on_recv_stream'):
            self.stream.on_recv_stream(self.callbacks['on_recv_stream'])

        if self.callbacks.has_key('on_send'):
            self.stream.on_send(self.callbacks['on_send'])

        if self.callbacks.has_key('on_send_stream'):
            self.stream.on_send_stream(self.callbacks['on_send_stream'])

        ioloop.IOLoop.instance().start()

    def send_multipart(self, message):
        self.router.send_multipart(message)

class CollectorRouter(Router):
    def __init__(self, *args, **kw):
        self.bind_address = conf.get('broker', 'zmq_collector_router_bind_address')
        if self.bind_address == None:
            self.bind_address = 'tcp://*:5571'

        super(CollectorRouter, self).__init__(*args, **kw)

class DealerRouter(Router):
    def __init__(self, *args, **kw):
        self.bind_address = conf.get('broker', 'zmq_dealer_router_bind_address')
        if self.bind_address == None:
            self.bind_address = 'tcp://*:5570'

        super(DealerRouter, self).__init__(*args, **kw)

class WorkerRouter(Router):
    def __init__(self, *args, **kw):
        self.bind_address = conf.get('broker', 'zmq_worker_router_bind_address')
        if self.bind_address == None:
            self.bind_address = 'tcp://*:5573'

        super(WorkerRouter, self).__init__(*args, **kw)

class WorkerControllerRouter(Router):
    def __init__(self, *args, **kw):
        self.bind_address = conf.get('broker', 'zmq_controller_bind_address')
        if self.bind_address == None:
            self.bind_address = 'tcp://*:5572'

        super(WorkerControllerRouter, self).__init__(*args, **kw)
