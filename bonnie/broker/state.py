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

import datetime
import hashlib
import re

import sqlalchemy

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import Text

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Index

try:
    from sqlalchemy.orm import relationship
except:
    from sqlalchemy.orm import relation as relationship

try:
    from sqlalchemy.orm import sessionmaker
except:
    from sqlalchemy.orm import create_session

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.broker.state')

DeclarativeBase = declarative_base()

db = None

collector_interest_table = Table(
        'collector_interest',
        DeclarativeBase.metadata,
        Column('collector_id', Integer, ForeignKey('collector.id')),
        Column('interest_id', Integer, ForeignKey('interest.id'))
    )

##
## Classes
##

class Collector(DeclarativeBase):
    __tablename__ = 'collector'

    id = Column(Integer, primary_key=True)
    identity = Column(String(128))
    state = Column(String(16))
    job = Column(Integer, ForeignKey('job.id'))
    timestamp = Column(DateTime, default=datetime.datetime.utcnow())
    interests = relationship(
            "Interest",
            secondary=collector_interest_table
        )

    def __init__(self, identity, state = b'READY'):
        DeclarativeBase.__init__(self)
        self.identity = identity
        self.state = state

class Interest(DeclarativeBase):
    __tablename__ = 'interest'

    id = Column(Integer, primary_key=True)
    cmd = Column(String(16), nullable=False)

    def __init__(self, cmd):
        self.cmd = cmd

class Job(DeclarativeBase):
    __tablename__ = 'job'

    id = Column(Integer, primary_key=True)
    uuid = Column(String(128), nullable=False, unique=True)
    dealer = Column(String(128), nullable=False)
    collector = Column(String(128), nullable=False)
    notification = Column(Text)
    job_type = Column(String(16), default='worker')
    state = Column(String(16))
    cmd = Column(String(256))
    timestamp = Column(DateTime, default=datetime.datetime.utcnow())
    pushbacks = Column(Integer, default=0)

    def __init__(self, dealer, notification, job_type='worker'):
        DeclarativeBase.__init__(self)
        self.uuid = hashlib.sha224(notification).hexdigest()
        self.dealer = dealer
        self.collector = re.sub(r'-[0-9]+$', '', dealer).replace('Dealer', 'Collector')
        self.notification = notification
        self.state = b'PENDING'
        self.job_type = job_type
        self.timestamp = datetime.datetime.utcnow()
        self.cmd = None
        self.pushbacks = 0

Index('job_collector', 'collector')
Index('job_job_type', 'job_type')
Index('job_state', 'state')

class Worker(DeclarativeBase):
    __tablename__ = 'worker'

    id = Column(Integer, primary_key=True)
    identity = Column(String(128))
    state = Column(String(16))
    timestamp = Column(DateTime, default=datetime.datetime.utcnow())
    job = Column(Integer, ForeignKey('job.id'))

    def __init__(self, identity, state):
        DeclarativeBase.__init__(self)
        self.identity = identity
        self.state = state

def init_db(name):
    """
        Returns a SQLAlchemy Session() instance.
    """
    global db

    if not db == None:
        return db

    db_uri = conf.get('broker', 'state_sql_uri')

    if not db_uri:
        db_uri = 'sqlite:////var/lib/bonnie/state.db'

    echo = conf.debuglevel > 8

    try:
        engine = create_engine(db_uri, echo=echo)
        DeclarativeBase.metadata.create_all(engine)
    except Exception, errmsg:
        log.error("Exception occurred: %r" % (errmsg))
        return None

    Session = sessionmaker(bind=engine,autoflush=True)
    db = Session()

    return db
