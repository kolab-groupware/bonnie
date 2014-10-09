class HandlerBase(object):
    def __init__(self, *args, **kw):
        pass

    def register(self, callback):
        interests = {
                self.event: {
                        'callback': self.run
                    }
            }

        self.worker = callback(interests)

    def run(self, notification):
        if notification.has_key('user') and not notification.has_key('user_id'):
            notification['user_id'] = self.worker.storage.resolve_username(notification['user'])

        return (notification, [])
