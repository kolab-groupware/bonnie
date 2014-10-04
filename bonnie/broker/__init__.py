import brokers

class BonnieBroker(object):
    broker_interests = {}
    broker_modules = {}

    def __init__(self, *args, **kw):
        for _class in brokers.list_classes():
            module = _class()
            module.register(callback=self.register_broker)
            self.broker_modules[_class] = module

    def register_broker(self, interests):
        """
            Register a broker based on interests
        """

        for interest,how in interests.iteritems():
            if not self.broker_interests.has_key(interest):
                self.broker_interests[interest] = []

            self.broker_interests[interest].append(how)

    def run(self, *args, **kw):
        for interest, hows in self.broker_interests.iteritems():
            for how in hows:
                how()

    def terminate(self, *args, **kw):
        for module in self.broker_modules.values():
            if hasattr(module, 'terminate'):
                module.terminate()
            else:
                module.running = False
