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
        self.log = bonnie.getLogger('worker.ElasticSearchStorage')

        elasticsearch_output_address = conf.get('worker', 'elasticsearch_storage_address')

        if elasticsearch_output_address == None:
            elasticsearch_output_address = 'localhost'

        self.es = elasticsearch.Elasticsearch(
            host=elasticsearch_output_address
        )

    def register(self, callback, **kw):
        if callback is not None:
            callback(interests={
                'uidset': { 'callback': self.resolve_folder_uri },
                'uniqueid': { 'callback': self.resolve_folder_uri }
            })

    def resolve_folder_uri(self, notification):
        """
            Resolve the folder uri (or uniqueid) into an elasticsearch object ID
        """
        # no folder resolving required
        if not notification.has_key('uri'):
            (notification, [])

        # split the uri parameter into useful parts
        uri = parse_imap_uri(notification['uri'])

        self.log.debug("Resolve folder uri %r: %r" % (notification['uri'], uri), level=8)

        # mailbox resolving requires 'uniqueid' coming from metadata
        if not notification.has_key('uniqueid') and not notification.has_key('metadata'):
            self.log.debug("Adding GETMETADATA job", level=8)
            return (notification, [ b"GETMETADATA" ])

        if not notification.has_key('uniqueid') and notification['metadata'].has_key('/shared/vendor/cmu/cyrus-imapd/uniqueid'):
            notification['uniqueid'] = notification['metadata']['/shared/vendor/cmu/cyrus-imapd/uniqueid']

        # lookup existing entry
        if notification.has_key('uniqueid'):
            try:
                result = self.es.search(
                    index=self.folders_index,
                    doc_type=self.folders_doctype,
                    q='uniqueid:"%s"' % (notification['uniqueid']),
                    search_type='query_then_fetch'
                )
                self.log.debug("ES search result for folder: %r" % (result), level=8)

            except Exception, e:
                self.log.warning("ES search exception: %r", e)
                result = None

            # replace uniqueid with the internal folder_id after successful lookup
            # TODO: check if the folder was updated recently (ACL changes?, /shared/vendor/cmu/cyrus-imapd/lastupdate?)
            #       and insert a new folder record
            if result is not None and len(result['hits']['hits']) > 0:
                hit = result['hits']['hits'][0]
                notification['folder_id'] = hit['_id']
                notification.pop('uniqueid', None)


            # create an entry for the referenced imap folder
            if not notification.has_key('folder_id'):
                # before creating a folder entry, we should collect folder ACLs
                if not notification.has_key('acl'):
                    self.log.debug("Adding GETACL", level=8)
                    return (notification, [ b"GETACL" ])

                self.log.debug("Create folder object for: %r" % (notification['uniqueid']), level=8)

                try:
                    ret = self.es.create(
                        index=self.folders_index,
                        doc_type=self.folders_doctype,
                        body={
                            '@version': bonnie.API_VERSION,
                            'uniqueid': notification['uniqueid'],
                            'metadata': notification['metadata'],
                            'acl': notification['acl'],
                            'owner': uri['user'] + '@' + uri['domain'],
                            'server': uri['host'],
                            'name': uri['path'],
                            'uri': "imap://%(user)s@%(domain)s@%(host)s/%(path)s" % uri,
                        }
                    )
                    self.log.debug("Created folder object: %r" % (ret), level=8)

                    # replace uniqueid with the internal folder_id
                    notification['folder_id'] = ret['_id']
                    notification.pop('uniqueid', None)

                except Exception, e:
                    self.log.warning("ES create exception: %r", e)

        return (notification, [])
