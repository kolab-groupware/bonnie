import sys

sys.path = [ '.', '../..' ] + sys.path

from test_001_utils import TestBonnieUtils
from test_002_collector import TestBonnieCollector
from test_003_dealer import TestBonnieDealer
from test_004_worker import TestBonnieWorker
from test_005_persistence import TestBonniePersistence
from test_006_caching import TestBonnieCaching

