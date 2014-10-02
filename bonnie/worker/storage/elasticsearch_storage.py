# -*- coding: utf-8 -*-
#
# Copyright 2010-2014 Kolab Systems AG (http://www.kolabsys.com)
#
# Jeroen van Meeuwen (Kolab Systems) <vanmeeuwen a kolabsys.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 3 or, at your option, any later
# version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307,
# USA.
#

"""
    Storage node writing object data into Elasticsearch
"""

import json
import hashlib
import datetime
import elasticsearch

from dateutil.tz import tzutc
from bonnie.utils import parse_imap_uri

import bonnie
conf = bonnie.getConf()

class ElasticSearchStorage(object):
    folders_index = 'objects'
    folders_doctype = 'folder'

    def __init__(self, *args, **kw):
        self.log = bonnie.getLogger('bonnie.worker.ElasticSearchStorage')

        elasticsearch_output_address = conf.get('worker', 'elasticsearch_storage_address')

        if elasticsearch_output_address == None:
            elasticsearch_output_address = 'localhost'

        self.es = elasticsearch.Elasticsearch(
            host=elasticsearch_output_address
        )

    def name(self):
        return 'elasticsearch_storage'

    def register(self, callback, **kw):
        if callback is not None:
            callback(interests={
                'uidset': { 'callback': self.resolve_folder_uri },
                'folder_uniqueid': { 'callback': self.resolve_folder_uri },
                'mailboxID': { 'callback': self.resolve_folder_uri, 'kw': { 'attrib': 'mailboxID' } }
            })

    def notificaton2folder(self, notification, attrib='uri'):
        """
            Turn the given notification record into a folder document.
            including the computation of a unique identifier which is a checksum
            of the (relevant) folder properties.
        """
        # split the uri parameter into useful parts
        uri = parse_imap_uri(notification[attrib])
        folder_uri = "imap://%(user)s@%(domain)s@%(host)s/%(path)s" % uri

        if not notification.has_key('metadata'):
            return False

        if not notification.has_key('folder_uniqueid') and notification['metadata'].has_key('/shared/vendor/cmu/cyrus-imapd/uniqueid'):
            notification['folder_uniqueid'] = notification['metadata']['/shared/vendor/cmu/cyrus-imapd/uniqueid']

        body = {
            '@version': bonnie.API_VERSION,
            '@timestamp': datetime.datetime.now(tzutc()).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'uniqueid': notification['folder_uniqueid'],
            'metadata': notification['metadata'],
            'acl': notification['acl'],
            'type': notification['metadata']['/shared/vendor/kolab/folder-type'] if notification['metadata'].has_key('/shared/vendor/kolab/folder-type') else 'mail',
            'owner': uri['user'] + '@' + uri['domain'],
            'server': uri['host'],
            'name': uri['path'],
            'uri': folder_uri,
        }

        # compute folder object signature and the unique identifier
        ignore_metadata = ['/shared/vendor/cmu/cyrus-imapd/lastupdate', '/shared/vendor/cmu/cyrus-imapd/pop3newuidl', '/shared/vendor/cmu/cyrus-imapd/size']
        signature = {
            '@version': bonnie.API_VERSION,
            'owner': body['owner'],
            'server': body['server'],
            'uniqueid': notification['folder_uniqueid'],
            'metadata': [(k,v) for k,v in sorted(notification['metadata'].iteritems()) if k not in ignore_metadata],
            'acl': [(k,v) for k,v in sorted(notification['acl'].iteritems())],
        }
        serialized = ";".join("%s:%s" % (k,v) for k,v in sorted(signature.iteritems()))
        folder_id = hashlib.md5(serialized).hexdigest()

        return dict(id=folder_id, body=body)


    def resolve_folder_uri(self, notification, attrib='uri'):
        """
            Resolve the folder uri (or folder_uniqueid) into an elasticsearch object ID
        """
        # no folder resolving required
        if not notification.has_key(attrib) or notification.has_key('folder_id'):
            return (notification, [])

        self.log.debug("Resolve folder for %r = %r" % (attrib, notification[attrib]), level=8)

        # mailbox resolving requires metadata
        if not notification.has_key('metadata'):
            self.log.debug("Adding GETMETADATA job", level=8)
            return (notification, [ b"GETMETADATA" ])

        # before creating a folder entry, we should collect folder ACLs
        if not notification.has_key('acl'):
            self.log.debug("Adding GETACL", level=8)
            return (notification, [ b"GETACL" ])

        # extract folder properties and a unique identifier from the notification
        folder = self.notificaton2folder(notification)

        # abort if notificaton2folder() failed
        if folder is False:
            return (notification, [])

        # lookup existing entry
        try:
            existing = self.es.get(
                index=self.folders_index,
                doc_type=self.folders_doctype,
                id=folder['id'],
                fields='uniqueid,name'
            )
            self.log.debug("ES search result for folder: %r" % (existing), level=8)

        except elasticsearch.exceptions.NotFoundError, e:
            self.log.debug("Folder entry not found in ES: %r", e)
            existing = None

        except Exception, e:
            self.log.warning("ES get exception: %r", e)
            existing = None

        # create an entry for the referenced imap folder
        if existing is None:
            self.log.debug("Create folder object for: %r" % (folder['body']['uri']), level=8)

            try:
                ret = self.es.create(
                    index=self.folders_index,
                    doc_type=self.folders_doctype,
                    id=folder['id'],
                    body=folder['body'],
                    consistency='one',
                    replication='async'
                )
                self.log.debug("Created folder object: %r" % (ret), level=8)

            except Exception, e:
                self.log.warning("ES create exception: %r", e)
                folder = None

        # update entry if name changed
        elif folder['body']['uniqueid'] in existing['fields']['uniqueid'] and \
            not folder['body']['name'] in existing['fields']['name']:
            try:
                ret = self.es.update(
                    index=self.folders_index,
                    doc_type=self.folders_doctype,
                    id=folder['id'],
                    body={ 'doc': {
                            'name': folder['body']['name'],
                            'uri': folder['body']['uri']
                        }
                    },
                    consistency='one',
                    replication='async'
                )
                self.log.debug("Updated folder object: %r" % (ret), level=8)

            except Exception, e:
                self.log.warning("ES update exception: %r", e)


        # add reference to internal folder_id
        if folder is not None:
            notification['folder_id'] = folder['id']

        return (notification, [])
