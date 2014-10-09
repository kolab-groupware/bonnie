# -*- coding: utf-8 -*-
# Copyright 2010-2014 Kolab Systems AG (http://www.kolabsys.com)
#
# Thomas Bruederli <bruederli at kolabsys.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 3 or, at your option, any later
# version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Library General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307,
# USA.
#

import sqlalchemy as db
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.orm.util import has_identity
from sqlalchemy.orm.attributes import init_collection
from sqlalchemy.ext.declarative import declarative_base

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.broker.persistence')

PersistentBase = declarative_base()

# an db engine, which the Session will use for connection resources
engine = db.create_engine(conf.get('broker', 'persistence_sql_uri', 'sqlite://'))

# create a configured "Session" class
Session = sessionmaker(bind=engine)
session = Session()


class PlistCollection(list):
    """
        Extended list collection to add some handy utility methods
    """
    def delete(self, item):
        self.remove(item)
        if has_identity(item):
            session.delete(item)
        else:
            session.expunge(item)

class PersistentList(PersistentBase):
    """
        Container class representing a persistent list object
    """
    __tablename__ = 'plists'
    listname = db.Column(db.String(32), primary_key=True)
    # items = relationship('PlistItem')

    def __init__(self, name):
        self.listname = name


#### module functions

__list_classes = {}
__list_instances = {}

def List(name, _type):
    """
        Factory function to return a list-like collection with persistent storage capabilities
    """
    if __list_instances.has_key(name):
        return __list_instances[name].items

    # create new list class to handle items (relations) of the given type
    _class_name = 'PersistentList' + _type.__name__

    if __list_classes.has_key(_class_name):
        _plistclass = __list_classes[_class_name]
    else:
        # determine foreign key type
        if hasattr(_type, '__key__') and _type.__table__.columns.has_key(_type.__key__):
            reltype = _type.__table__.columns[_type.__key__].type
        elif hasattr(_type.__table__, 'primary_key'):
            for col in _type.__table__.primary_key.columns:
                _type.__key__ = col.name
                reltype = col.type
                break
        else:
            _type.__key__ = 'id'
            reltype = db.String(256)

        # we establish a many-to-many relation using an association table
        association_table = db.Table('plistitems_' + _type.__tablename__, PersistentBase.metadata,
            db.Column('listname', db.Integer, db.ForeignKey('plists.listname'), index=True),
            db.Column('ref', reltype, db.ForeignKey(_type.__tablename__ + '.' + _type.__key__), index=True)
        )

        # create a list container class with a relationship to the list item type
        _plistclass = type(_class_name, (PersistentList,), {
            'items': relationship(_type, secondary=association_table, backref='list', collection_class=PlistCollection)
        })
        __list_classes[_class_name] = _plistclass

        PersistentBase.metadata.create_all(engine)

    try:
        lst = session.query(_plistclass).filter(PersistentList.listname == name).one()
    except:
        lst = _plistclass(name)
        session.add(lst)

    # remember this instance for later calls and to avoid garbage collection
    __list_instances[name] = lst

    # return the collection og list items
    return lst.items


def syncronize():
    """
        Synchronize to persistent storage
    """
    if len(session.new) > 0 or len(session.dirty) > 0 or len(session.deleted) > 0:
        log.debug("session.commit(); new=%r; dirty=%r; deleted=%r" % (session.new, session.dirty, session.deleted), level=9)
        session.commit()
