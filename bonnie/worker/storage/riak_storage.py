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
import json
import re
import urllib
import random
import riak
import time

from dateutil.tz import tzutc
from bonnie.utils import parse_imap_uri
from bonnie.worker.storage import CachedDict

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.worker.RiakStorage')

class RiakStorage(object):
    """
        Storage node writing object data into Riak
    """
    default_index = 'objects'
    default_doctype = 'object'
    folders_index = 'objects'
    folders_doctype = 'folder'
    users_index = 'objects'
    users_doctype = 'user'

    def __init__(self, *args, **kw):
        riak_output_address = conf.get(
                'worker',
                'riak_storage_address'
            )

        if riak_output_address == None:
            riak_output_address = 'localhost'

        self.es = riak.Elasticsearch(
                host=riak_output_address
            )

        # use dicts with automatic expiration for caching user/folder
        # lookups
        self.user_id_cache = CachedDict(300)
        self.folder_id_cache = CachedDict(120)

    def name(self):
        return 'riak_storage'

    def register(self, callback, **kw):
        if callback is not None:
            self.worker = callback(
                    interests = {
                            'uidset': {
                                    'callback': self.resolve_folder_uri
                                },
                            'folder_uniqueid': {
                                    'callback': self.resolve_folder_uri
                                },
                            'mailboxID': {
                                    'callback': self.resolve_folder_uri,
                                    'kw': { 'attrib': 'mailboxID' }
                                }
                        }
                )

    def get(self, key, index=None, doctype=None, fields=None, **kw):
        """
            Standard API for accessing key/value storage
        """
        _index = index or self.default_index
        _doctype = doctype or self.default_doctype
        try:
            res = self.riak.get(
                    index = _index,
                    doc_type = _doctype,
                    id = key,
                    _source_include = fields or '*'
                )

            log.debug(
                    "Riak get result for %s/%s/%s: %r" % (
                            _index,
                            _doctype,
                            key,
                            res
                        ),
                    level = 8
                )

            if res['found']:
                result = self._transform_result(res)
            else:
                result = None

        except riak.exceptions.NotFoundError, errmsg:
            log.debug(
                    "Riak entry not found for %s/%s/%s: %r" % (
                            _index,
                            _doctype,
                            key,
                            errmsg
                        )
                )

            result = None

        except Exception, errmsg:
            log.warning("Riak get exception: %r" % (errmsg))
            result = None

        return result

    def set(self, key, value, index=None, doctype=None, **kw):
        """
            Standard API for writing to key/value storage
        """
        _index = index or self.default_index
        _doctype = doctype or self.default_doctype

        try:
            existing = self.riak.get(
                    index = _index,
                    doc_type = _doctype,
                    id = key,
                    fields = None
                )

            log.debug(
                    "Riak get result for %s/%s/%s: %r" % (
                            _index,
                            _doctype,
                            key,
                            existing
                        ),
                    level = 8
                )

        except riak.exceptions.NotFoundError, errmsg:
            existing = None

        except Exception, errmsg:
            log.warning("Riak get exception: %r" % (errmsg))
            existing = None

        if existing is None:
            try:
                ret = self.riak.create(
                        index = _index,
                        doc_type = _doctype,
                        id = key,
                        body = value,
                        consistency = 'one',
                        replication = 'async'
                    )

                log.debug(
                        "Created ES object for %s/%s/%s: %r" % (
                                _index,
                                _doctype,
                                key,
                                ret
                            ),
                        level = 8
                    )

            except Exception, errmsg:
                log.warning("Riak create exception: %r" % (errmsg))
                ret = None

        else:
            try:
                ret = self.riak.update(
                        index = _index,
                        doc_type = _doctype,
                        id = key,
                        body = { 'doc': value },
                        consistency = 'one',
                        replication = 'async'
                    )

                log.debug(
                        "Updated ES object for %s/%s/%s: %r" % (
                                _index,
                                _doctype,
                                key,
                                ret
                            ),
                        level = 8
                    )

            except Exception, errmsg:
                log.warning("Riak update exception: %r" % (errmsg))

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
        args = dict(
                index = index or self.default_index,
                doc_type = doctype or self.default_doctype,
                _source_include = fields or '*'
            )

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
            res = self.riak.search(**args)
            log.debug(
                    "Riak select result for %r: %r" % (
                            args['q'] or args['body'],
                            res
                        ),
                    level = 8
                )

        except riak.exceptions.NotFoundError, errmsg:
            log.debug(
                    "Riak entry not found for %r: %r" % (
                            args['q'] or args['body'],
                            errmsg
                        ),
                    level = 8
                )

            res = None

        except Exception, errmsg:
            log.warning("Riak get exception: %r" % (errmsg))
            res = None

        if res is not None and res.has_key('hits'):
            result = dict(total=res['hits']['total'])
            result['hits'] = [self._transform_result(x) for x in res['hits']['hits']]
        else:
            result = None

        return result

    def _build_query(self, params, boolean='AND'):
        """
            Convert the given list of query parameters into a Lucene
            query string.
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
            Turn an riak result item into a simple dict
        """
        result = res['_source'] if res.has_key('_source') else dict()
        result['_id'] = res['_id']
        result['_index'] = res['_index']
        result['_doctype'] = res['_type']

        if res.has_key('_score'):
            result['_score'] = res['_score']

        return result

    def resolve_username(self, user, user_data=None, force=False):
        """
            Resolve the given username to the corresponding nsuniqueid
            from LDAP
        """
        if not '@' in user:
            return user

        # return id cached in memory
        if self.user_id_cache.has_key(user):
            return self.user_id_cache[user]

        user_id = None

        # find existing entry in our storage backend
        result = self.select(
            [ ('user', '=', user) ],
            index=self.users_index,
            doctype=self.users_doctype,
            sortby='@timestamp:desc',
            limit=1
        )

        if result and result['total'] > 0:
            user_id = result['hits'][0]['_id']

        elif user_data and user_data.has_key('id'):
            # user data (from LDAP) is provided
            user_id = user_data['id']

            # insert a user record into our database
            del user_data['id']
            user_data['user'] = user
            user_data['@timestamp'] = datetime.datetime.now(
                    tzutc()
                ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            self.set(
                    user_id,
                    user_data,
                    index=self.users_index,
                    doctype=self.users_doctype
                )

        elif force:
            user_id = hashlib.md5(user).hexdigest()

        # cache this for 5 minutes
        if user_id is not None:
            self.user_id_cache[user] = user_id

        return user_id


    def notificaton2folder(self, notification, attrib='uri'):
        """
            Turn the given notification record into a folder document.
            including the computation of a unique identifier which is a
            checksum of the (relevant) folder properties.
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

        if not notification.has_key('folder_uniqueid') and \
                notification['metadata'].has_key(
                        '/shared/vendor/cmu/cyrus-imapd/uniqueid'
                    ):

            notification['folder_uniqueid'] = notification['metadata']['/shared/vendor/cmu/cyrus-imapd/uniqueid']

        if not notification.has_key('folder_uniqueid'):
            notification['folder_uniqueid'] = hashlib.md5(
                    notification[attrib]
                ).hexdigest()

        body = {
                '@version': bonnie.API_VERSION,
                '@timestamp': datetime.datetime.now(
                        tzutc()
                    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),

                'uniqueid': notification['folder_uniqueid'],
                'metadata': notification['metadata'],
                'acl': dict(
                        (self.resolve_username(k, force=True),v) for k,v in notification['acl'].iteritems()
                    ),

                'type': notification['metadata']['/shared/vendor/kolab/folder-type'] if notification['metadata'].has_key('/shared/vendor/kolab/folder-type') else 'mail',

                'owner': uri['user'] + '@' + uri['domain'] if uri['user'] is not None else 'nobody',
                'server': uri['host'],
                'name': re.sub('@.+$', '', uri['path']),
                'uri': folder_uri,
            }

        # compute folder object signature and the unique identifier
        ignore_metadata = [
                '/shared/vendor/cmu/cyrus-imapd/lastupdate',
                '/shared/vendor/cmu/cyrus-imapd/pop3newuidl',
                '/shared/vendor/cmu/cyrus-imapd/size'
            ]

        signature = {
                '@version': bonnie.API_VERSION,
                'owner': body['owner'],
                'server': body['server'],
                'uniqueid': notification['folder_uniqueid'],
                'metadata': [
                        (k,v) for k,v in sorted(body['metadata'].iteritems()) if k not in ignore_metadata
                    ],

                'acl': [
                        (k,v) for k,v in sorted(body['acl'].iteritems())
                    ],
            }

        serialized = ";".join(
                "%s:%s" % (k,v) for k,v in sorted(signature.iteritems())
            )

        folder_id = hashlib.md5(serialized).hexdigest()

        return dict(id=folder_id, body=body)

    def resolve_folder_uri(self, notification, attrib='uri'):
        """
            Resolve the folder uri (or folder_uniqueid) into an
            riak object ID.
        """
        # no folder resolving required
        if not notification.has_key(attrib) or \
                notification.has_key('folder_id'):

            return (notification, [])

        now = int(time.time())
        base_uri = re.sub(';.+$', '', notification[attrib])
        jobs = []

        log.debug(
                "Resolve folder for %r = %r" % (attrib, base_uri),
                level = 8
            )

        # return id cached in memory
        if not notification.has_key('metadata') and \
                self.folder_id_cache.has_key(base_uri):

            notification['folder_id'] = self.folder_id_cache[base_uri]
            return (notification, [])

        # mailbox resolving requires metadata
        if not notification.has_key('metadata'):
            log.debug("Adding GETMETADATA job", level=8)
            jobs.append(b"GETMETADATA")

        # before creating a folder entry, we should collect folder ACLs
        if not notification.has_key('acl'):
            log.debug("Adding GETACL", level=8)
            jobs.append(b"GETACL")

        # reject notification with additional collector jobs
        if len(jobs) > 0:
            return (notification, jobs)

        # extract folder properties and a unique identifier from the
        # notification
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
            log.debug(
                    "Create folder object for: %r" % (
                            folder['body']['uri']
                        ),
                    level = 8
                )

            ret = self.set(
                    index = self.folders_index,
                    doctype = self.folders_doctype,
                    key = folder['id'],
                    value = folder['body']
                )

            if ret is None:
                folder = None

        # update entry if name changed
        elif folder['body']['uniqueid'] == existing['uniqueid'] and \
                not folder['body']['name'] == existing['name']:

            try:
                ret = self.riak.update(
                        index = self.folders_index,
                        doc_type = self.folders_doctype,
                        id = folder['id'],
                        body = {
                                'doc': {
                                        'name': folder['body']['name'],
                                        'uri': folder['body']['uri']
                                    }
                            },
                        consistency = 'one',
                        replication = 'async'
                    )

                log.debug("Updated folder object: %r" % (ret), level=8)

            except Exception, errmsg:
                log.warning("Riak update exception: %r" % (errmsg))

        # add reference to internal folder_id
        if folder is not None:
            self.folder_id_cache[base_uri] = folder['id']
            notification['folder_id'] = folder['id']

        return (notification, [])

    def report(self):
        """
            Callback from the worker main loop to trigger periodic jobs
        """
        # clean-up in-memory caches from time to time
        self.user_id_cache.expunge()
        self.folder_id_cache.expunge()
