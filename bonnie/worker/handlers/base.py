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
        # resolve user_id from storage
        if notification.has_key('user') and not notification.has_key('user_id'):
            user_data = notification['user_data'] if notification.has_key('user_data') else None
            notification['user_id'] = self.worker.storage.resolve_username(notification['user'], user_data, force=notification.has_key('user_data'))

        # if storage has no entry, fetch user record from collector
        if notification.has_key('user') and notification['user_id'] is None and not notification.has_key('user_data'):
            notification['user_data'] = None  # avoid endless loop if GETUSERDATA fails
            return (notification, [ b"GETUSERDATA" ])

        # don't store user data in notification
        if notification.has_key('user_data'):
            del notification['user_data']

        return (notification, [])
