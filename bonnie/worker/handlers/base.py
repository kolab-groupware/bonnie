class HandlerBase(object):
    def __init__(self, *args, **kw):
        pass

    def register(self, callback):
        interests = {
                self.event: {
                        'callback': self.run
                    }
            }

        callback(interests)

    def run(self, notification):
        return (notification, [])
