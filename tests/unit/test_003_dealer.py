# -*- coding: utf-8 -*-
# Copyright 2010-2014 Kolab Systems AG (http://www.kolabsys.com)
#
# Jeroen van Meeuwen (Kolab Systems) <vanmeeuwen a kolabsys.com>
# Thomas Bruederli (Kolab Systems) <bruederli a kolabsys.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

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
