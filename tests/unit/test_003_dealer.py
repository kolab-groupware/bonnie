import os
import json
import bonnie
from bonnie.dealer import BonnieDealer
from twisted.trial import unittest

class BonnieDealerOutput(object):
    """
        Mockup class to catch event notifications forwarded by BonnieDealer
    """
    output_calls = []

    def __init__(self, *args, **kw):
        pass

    def name(self):
        return 'zmq_output'

    def register(self, *args, **kw):
        pass

    def run(self, notification):
        notification = json.loads(notification)
        self.output_calls.append(notification['event'])

class TestBonnieDealer(unittest.TestCase):

    def setUp(self):
        # patch bonnie.outputs.list_classes() to register a dummy output handler
        self.patch(bonnie.dealer.outputs, 'list_classes', self._outputs_list_classes)

    def _outputs_list_classes(self):
        return [ BonnieDealerOutput ]

    def test_run(self):
        dealer = BonnieDealer()
        dealer.run(json.dumps({ 'event':'Login', 'user':'john.doe@example.org' }))
        self.assertIn('Login', BonnieDealerOutput.output_calls)

        # don't trigger blacklisted events
        BonnieDealerOutput.output_calls = []
        dealer.run(json.dumps({ 'event':'Login', 'user':'cyrus-admin' }))
        self.assertEqual(len(BonnieDealerOutput.output_calls), 0)

        dealer.run(json.dumps({ 'event':'MessageAppend', 'user':'cyrus-admin' }))
        self.assertEqual(len(BonnieDealerOutput.output_calls), 1)
        self.assertIn('MessageAppend', BonnieDealerOutput.output_calls)
        