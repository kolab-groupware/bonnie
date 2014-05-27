import os
from ConfigParser import SafeConfigParser

class Conf(object):
    def __init__(self):
        self.config = SafeConfigParser()
        if os.path.exists('/etc/bonnie/bonnie.conf'):
            self.config.read('/etc/bonnie/bonnie.conf')
        elif os.path.exists(os.path.abspath(os.path.dirname(__file__) + '/../conf/bonnie.conf')):
            self.config.read(os.path.abspath(os.path.dirname(__file__) + '/../conf/bonnie.conf'))

    def get(self):
        return self.config
