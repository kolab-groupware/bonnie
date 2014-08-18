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

    def run(self, notification):
        #print "es output for:", notification
        # The output should have UTC timestamps, but gets "2014-05-16T12:55:53.870+02:00"
        timestamp = notification['timestamp']
        notification['@timestamp'] = datetime.datetime.strftime(parse(timestamp).astimezone(tzutc()), "%Y-%m-%dT%H:%M:%S.%fZ")

        # Delete the former timestamp
        del notification['timestamp']

        self.es.create(
                index='logstash-%s' % (datetime.datetime.strftime(parse(timestamp).astimezone(tzutc()), "%Y-%m-%d")),
                doc_type='logs',
                body=notification
            )
        return (notification, [])
