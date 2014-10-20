import sys

sys.path = [ '.', '../..' ] + sys.path

from testbase import TestBonnieFunctional
from test_001_login import TestBonnieLogin
from test_002_mailboxes import TestBonnieMailboxes

