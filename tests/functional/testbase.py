import os
import json
import time

from twisted.trial import unittest
from subprocess import call
from bonnie.dealer import BonnieDealer
from bonnie.worker.storage import ElasticSearchStorage

pwd = os.path.dirname(__file__)
basedir = os.path.join(pwd, '..', '..')

import bonnie
conf = bonnie.getConf()
conf.finalize_conf()

class TestBonnieFunctional(unittest.TestCase):

    def setUp(self):
        self.storage = ElasticSearchStorage()
        self.storage.es.indices.delete(index='logstash-*', ignore=[400, 404])
        self.storage.es.indices.delete(index='objects', ignore=[400, 404])

        call([os.path.join(pwd, 'start.sh'), basedir])
        time.sleep(1)

    def tearDown(self):
        call([os.path.join(pwd, 'stop.sh')])
        time.sleep(2)

    def query_log(self, query):
        attempts = 10
        while attempts > 0:
            attempts -= 1
            res = self.storage.select(query, index='logstash-*', doctype='logs', sortby='@timestamp:desc')
            if res and res['total'] > 0:
                return res['hits']

            time.sleep(1)
            # print "query retry", attempts

        return None

    def storage_get(self, key, index, doctype):
        attempts = 10
        while attempts > 0:
            attempts -= 1
            res = self.storage.get(key, index=index, doctype=doctype)
            if res is not None:
                return res
            time.sleep(1)
            # print "get retry", attempts

        return None

