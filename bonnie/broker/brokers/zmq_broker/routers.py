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
        log.info("Process %s initializing..." % (multiprocessing.current_process().name))

        if not kw.has_key('callback'):
            raise ArgumentError, "No callback specified"

        if not callable(kw['callback']):
            raise ArgumentError, "Callback not callable"

        #self.zmq_callback = kw['callback']
        del kw['callback']

        self.target = self.start_router

        context = zmq.Context()

        self.zmq_router = context.socket(zmq.ROUTER)
        self.zmq_router.bind(self.bind_address)

        zmq_router_stream = zmqstream.ZMQStream(self.zmq_router, ioloop.IOLoop.instance())
        zmq_router_stream.on_recv(self.zmq_callback)
        zmq_router_stream.on_send(self.zmq_callback)
        zmq_router_stream.on_recv_stream(self.zmq_callback)
        zmq_router_stream.on_send_stream(self.zmq_callback)

        super(Router, self).__init__(*args, **kw)

        log.info("Process %s initialized" % (multiprocessing.current_process().name))

    def send_multipart(self, message):
        """
            Proxy callable for sub-classes.

            Enables the main broker thread to trigger the message bus in
            to dispatching a message.
        """
        log.info("%s sending message: %r" % (multiprocessing.current_process().name, message))

        self.zmq_router.send_multipart(message)

    def run(self, *args, **kw):
        log.info("Process %s running..." % (multiprocessing.current_process().name))
        self.zmq_callback([multiprocessing.current_process().name, "Running!", "extra"])
        super(Router, self).run(*args, **kw)

    def start(self, *args, **kw):
        log.info("Process %s starting..." % (multiprocessing.current_process().name))
        super(Router, self).start(*args, **kw)

    def start_router(self):
        log.info("Starting %s" % (multiprocessing.current_process().name))

        ioloop.IOLoop.instance().start()

    def zmq_callback(self, *args, **kw):
        log.info("Router.zmq_callback called: args: %r, kw: %r" % (args, kw))

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
