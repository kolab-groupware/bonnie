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

import re
import json
import urllib
import hashlib
import datetime
import elasticsearch

from dateutil.tz import tzutc
from bonnie.utils import parse_imap_uri

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.worker.ElasticSearchStorage')

class ElasticSearchStorage(object):
    """
        Storage node writing object data into Elasticsearch
    """
    default_index = 'objects'
    default_doctype = 'object'
    folders_index = 'objects'
    folders_doctype = 'folder'

    def __init__(self, *args, **kw):
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
            self.worker = callback(interests={
                'uidset': { 'callback': self.resolve_folder_uri },
                'folder_uniqueid': { 'callback': self.resolve_folder_uri },
                'mailboxID': { 'callback': self.resolve_folder_uri, 'kw': { 'attrib': 'mailboxID' } }
            })

    def get(self, key, index=None, doctype=None, fields=None, **kw):
        """
            Standard API for accessing key/value storage
        """
        _index = index or self.default_index
        _doctype = doctype or self.default_doctype
        try:
            res = self.es.get(
                index=_index,
                doc_type=_doctype,
                id=key,
                _source_include=fields or '*'
            )
            log.debug("ES get result for %s/%s/%s: %r" % (_index, _doctype, key, res), level=8)

            if res['found']:
                result = self._transform_result(res)
            else:
                result = None

        except elasticsearch.exceptions.NotFoundError, e:
            log.debug("ES entry not found for %s/%s/%s: %r" % (_index, _doctype, key, e))
            result = None

        except Exception, e:
            log.warning("ES get exception: %r", e)
            result = None

        return result


    def set(self, key, value, index=None, doctype=None, **kw):
        """
            Standard API for writing to key/value storage
        """
        _index = index or self.default_index
        _doctype = doctype or self.default_doctype
        try:
            existing = self.es.get(
                index=_index,
                doc_type=_doctype,
                id=key,
                fields=None
            )
            log.debug("ES get result for %s/%s/%s: %r" % (_index, _doctype, key, existing), level=8)

        except elasticsearch.exceptions.NotFoundError, e:
            existing = None

        except Exception, e:
            log.warning("ES get exception: %r", e)
            existing = None

        if existing is None:
            try:
                ret = self.es.create(
                    index=_index,
                    doc_type=_doctype,
                    id=key,
                    body=value,
                    consistency='one',
                    replication='async'
                )
                log.debug("Created ES object for %s/%s/%s: %r" % (_index, _doctype, key, ret), level=8)

            except Exception, e:
                log.warning("ES create exception: %r", e)
                ret = None
        else:
            try:
                ret = self.es.update(
                    index=_index,
                    doc_type=_doctype,
                    id=key,
                    body={ 'doc': value },
                    consistency='one',
                    replication='async'
                )
                log.debug("Updated ES object for %s/%s/%s: %r" % (_index, _doctype, key, ret), level=8)

            except Exception, e:
                log.warning("ES update exception: %r", e)

        return ret



    def select(self, query, index=None, doctype=None, fields=None, sortby=None, limit=None, **kw):
        """
            Standard API for querying storage

            @param query:   List of query parameters, each represented as a triplet of (<field> <op> <value>).
                            combined to an AND list of search criterias. <value> can either be
                             - a string for direct comparison
                             - a list for "in" comparisons
                             - a tuple with two values for range queries
            @param index:   Index name (i.e. database name)
            @param doctype: Document type (i.e. table name)
            @param fields:  List of fields to retrieve (string, comma-separated)
            @param sortby:  Fields to be used fort sorting the results (string, comma-separated)
            @param limit:   Number of records to return
        """
        result = None
        args = dict(index=index or self.default_index, doc_type=doctype or self.default_doctype, _source_include=fields or '*')

        if isinstance(query, dict):
            args['body'] = query
        elif isinstance(query, list):
            args['q'] = self._build_query(query)
        else:
            args['q'] = query

        if sortby is not None:
            args['sort'] = sortby
        if limit is not None:
            args['size'] = int(limit)

        try:
            res = self.es.search(**args)
            log.debug("ES select result for %r: %r" % (args['q'] or args['body'], res), level=8)

        except elasticsearch.exceptions.NotFoundError, e:
            log.debug("ES entry not found for key %s: %r", key, e)
            res = None

        except Exception, e:
            log.warning("ES get exception: %r", e)
            res = None

        if res is not None and res.has_key('hits'):
            result = dict(total=res['hits']['total'])
            result['hits'] = [self._transform_result(x) for x in res['hits']['hits']]
        else:
            result = None

        return result

    def _build_query(self, params, boolean='AND'):
        """
            Convert the given list of query parameters into a Lucene query string
        """
        query = []
        for p in params:
            if isinstance(p, str):
                # direct query string
                query.append(p)

            elif isinstance(p, tuple) and len(p) == 3:
                # <field> <op> <value> triplet
                (field, op, value) = p
                op_ = '-' if op == '!=' else ''

                if isinstance(value, list):
                    value_ = '("' + '","'.join(value) + '")'
                elif isinstance(value, tuple):
                    value_ = '[%s TO %s]' % value
                else:
                    quote = '"' if not '*' in str(value) else ''
                    value_ = quote + str(value) + quote

                query.append('%s%s:%s' % (op_, field, value_))

            elif isinstance(p, tuple) and len(p) == 2:
                # group/subquery with boolean operator
                (op, subquery) = p
                query.append('(' + self._build_query(subquery, op) + ')')

        return (' '+boolean+' ').join(query)

    def _transform_result(self, res):
        """
            Turn an elasticsearch result item into a simple dict
        """
        result = res['_source'] if res.has_key('_source') else dict()
        result['_id'] = res['_id']
        result['_index'] = res['_index']
        result['_doctype'] = res['_type']
        result['_score'] = res['_score']

        return result

    def resolve_username(self, user):
        """
            Resovle the given username to the corresponding nsuniqueid from LDAP
        """
        if not '@' in user:
            return user

        # TODO: resolve with storage data
        # return md5 sum of the username to make usernames work as fields/keys in elasticsearch
        return hashlib.md5(user).hexdigest()


    def notificaton2folder(self, notification, attrib='uri'):
        """
            Turn the given notification record into a folder document.
            including the computation of a unique identifier which is a checksum
            of the (relevant) folder properties.
        """
        # split the uri parameter into useful parts
        uri = parse_imap_uri(notification[attrib])

        # re-compose folder uri
        templ = "imap://%(user)s@%(domain)s@%(host)s/"
        if uri['user'] is None:
            templ = "imap://%(host)s/"
        folder_uri = templ % uri + urllib.quote(uri['path'])

        if not notification.has_key('metadata'):
            return False

        if not notification.has_key('folder_uniqueid') and notification['metadata'].has_key('/shared/vendor/cmu/cyrus-imapd/uniqueid'):
            notification['folder_uniqueid'] = notification['metadata']['/shared/vendor/cmu/cyrus-imapd/uniqueid']

        if not notification.has_key('folder_uniqueid'):
            notification['folder_uniqueid'] = hashlib.md5(notification[attrib]).hexdigest()

        body = {
            '@version': bonnie.API_VERSION,
            '@timestamp': datetime.datetime.now(tzutc()).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'uniqueid': notification['folder_uniqueid'],
            'metadata': notification['metadata'],
            'acl': dict((self.resolve_username(k),v) for k,v in notification['acl'].iteritems()),
            'type': notification['metadata']['/shared/vendor/kolab/folder-type'] if notification['metadata'].has_key('/shared/vendor/kolab/folder-type') else 'mail',
            'owner': uri['user'] + '@' + uri['domain'] if uri['user'] is not None else 'nobody',
            'server': uri['host'],
            'name': re.sub('@.+$', '', uri['path']),
            'uri': folder_uri,
        }

        # compute folder object signature and the unique identifier
        ignore_metadata = ['/shared/vendor/cmu/cyrus-imapd/lastupdate', '/shared/vendor/cmu/cyrus-imapd/pop3newuidl', '/shared/vendor/cmu/cyrus-imapd/size']
        signature = {
            '@version': bonnie.API_VERSION,
            'owner': body['owner'],
            'server': body['server'],
            'uniqueid': notification['folder_uniqueid'],
            'metadata': [(k,v) for k,v in sorted(body['metadata'].iteritems()) if k not in ignore_metadata],
            'acl': [(k,v) for k,v in sorted(body['acl'].iteritems())],
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

        log.debug("Resolve folder for %r = %r" % (attrib, notification[attrib]), level=8)

        # mailbox resolving requires metadata
        if not notification.has_key('metadata'):
            log.debug("Adding GETMETADATA job", level=8)
            return (notification, [ b"GETMETADATA" ])

        # before creating a folder entry, we should collect folder ACLs
        if not notification.has_key('acl'):
            log.debug("Adding GETACL", level=8)
            return (notification, [ b"GETACL" ])

        # extract folder properties and a unique identifier from the notification
        folder = self.notificaton2folder(notification)

        # abort if notificaton2folder() failed
        if folder is False:
            return (notification, [])

        # lookup existing entry
        existing = self.get(
            index=self.folders_index,
            doctype=self.folders_doctype,
            key=folder['id'],
            fields='uniqueid,name'
        )

        # create an entry for the referenced imap folder
        if existing is None:
            log.debug("Create folder object for: %r" % (folder['body']['uri']), level=8)

            ret = self.set(
                index=self.folders_index,
                doctype=self.folders_doctype,
                key=folder['id'],
                value=folder['body']
            )
            if ret is None:
                folder = None

        # update entry if name changed
        elif folder['body']['uniqueid'] == existing['uniqueid'] and \
            not folder['body']['name'] == existing['name']:
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
                log.debug("Updated folder object: %r" % (ret), level=8)

            except Exception, e:
                log.warning("ES update exception: %r", e)


        # add reference to internal folder_id
        if folder is not None:
            notification['folder_id'] = folder['id']

        return (notification, [])
