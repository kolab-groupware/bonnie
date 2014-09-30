import datetime

from dateutil.parser import parse
from dateutil.tz import tzutc

import elasticsearch
import json

import bonnie
conf = bonnie.getConf()

class ElasticSearchOutput(object):
    def __init__(self, *args, **kw):
        elasticsearch_output_address = conf.get(
                'worker',
                'elasticsearch_output_address'
            )

        if elasticsearch_output_address == None:
            elasticsearch_output_address = 'localhost'

        self.es = elasticsearch.Elasticsearch(
                host=elasticsearch_output_address
            )

    def register(self, callback):
        callback({'_all': { 'callback': self.run }})

    def notification2log(self, notification):
        """
            Convert the given event notification record into a valid log entry
        """
        keymap = {
            'timestamp':    None,
            'clientIP':     'client_ip',
            'clientPort':   None,
            'serverPort':   None,
            'serverDomain': 'domain',
            'aclRights':    'acl_rights',
            'aclSubject':   'acl_subject',
            'mailboxID':    'mailbox_id',
            'messageSize':  'message_size',
            'messageHeaders': None,
            'messageContent': None,
            'bodyStructure':  None,
            'flagNames':    'flag_names',
            'diskUsed':     'disk_used',
            'vnd.cmu.oldUidset': 'olduidset',
        }
        log = { '@version': bonnie.API_VERSION }
        for key,val in notification.iteritems():
            newkey = keymap[key] if keymap.has_key(key) else key
            if newkey is not None:
                # convert NIL values into None which is more appropriate
                if isinstance(val, list):
                    val = [x for x in val if not x == "NIL"]
                elif val == "NIL":
                    val = None

                log[newkey] = val

        return log

    def run(self, notification):
        # The output should have UTC timestamps, but gets "2014-05-16T12:55:53.870+02:00"
        try:
            timestamp = parse(notification['timestamp']).astimezone(tzutc())
        except:
            timestamp = datetime.datetime.now(tzutc())

        notification['@timestamp'] = datetime.datetime.strftime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        index = 'logstash-%s' % (datetime.datetime.strftime(timestamp, "%Y-%m-%d"))

        # for notifications concerning multiple messages, create entries for each message
        if notification.has_key('messageHeaders') and isinstance(notification['messageHeaders'], dict):
            for uid,headers in notification['messageHeaders'].iteritems():
                notification['uidset']  = uid
                notification['headers'] = headers
                notification['message'] = None
                if notification.has_key('messageContent') and notification['messageContent'].has_key(uid):
                    notification['message'] = notification['messageContent'][uid]

                self.es.create(
                    index=index,
                    doc_type='logs',
                    body=self.notification2log(notification)
                )
        else:
            self.es.create(
                index=index,
                doc_type='logs',
                body=self.notification2log(notification)
            )

        return (notification, [])
