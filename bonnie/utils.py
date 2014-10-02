import os
import urllib
import urlparse
import datetime
import subprocess

from dateutil.parser import parse as parse_date
from dateutil.tz import tzutc

from email import message_from_string
from email.header import decode_header
from email.utils import getaddresses

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('bonnie.utils')


def expand_uidset(uidset):
    """
        Expand the given UID set string into a complete set of values
        Examples: 1,2,5 => [1,2,5] or 1:4 => [1,2,3,4]
    """
    _uids = []
    for _uid in uidset.split(','):
        if len(_uid.split(':')) > 1:
            for __uid in range((int)(_uid.split(':')[0]), (int)(_uid.split(':')[1])+1):
                _uids.append("%d" % (__uid))
        else:
            _uids.append(str(_uid))

    return _uids


def parse_imap_uri(uri):
    """
        Split the given URI string into its components
    """
    split_uri = urlparse.urlsplit(uri)

    if len(split_uri.netloc.split('@')) == 3:
        (username, domain, server) = split_uri.netloc.split('@')
    elif len(split_uri.netloc.split('@')) == 2:
        (username, server) = split_uri.netloc.split('@')
        domain = None
    elif len(split_uri.netloc.split('@')) == 1:
        username = None
        domain = None
        server = split_uri.netloc

    result = dict(user=username, domain=domain, host=server)

    # First, .path == '/Calendar/Personal%20Calendar;UIDVALIDITY=$x[/;UID=$y]
    # Take everything after the first slash, and omit any INBOX/ stuff.
    path_str = '/'.join([x for x in split_uri.path.split('/') if not x == 'INBOX'][1:])
    path_arr = path_str.split(';')
    result['path'] = urllib.unquote(path_arr[0])

    # parse the path/query parameters into a dict
    param = dict()
    for p in path_arr[1:]:
        if '=' in p:
            (key,val) = p.split('=', 2)
            result[key] = urllib.unquote(val)

    return result


def decode_message_headers(message):
    """
        Copy headers from the given message object into a dict
        structure and normalize the values into UTF-8 strings
    """
    headers = dict(message.items())

    # split recipient headers into lists
    for h in ['From','To','Cc','Bcc']:
        if headers.has_key(h):
            headers[h] = ['%s <%s>' % (name,address) for name,address in getaddresses(message.get_all(h))]

    # decode header values into UTF-8
    for k,value in headers.iteritems():
        if isinstance(value, list):
            headers[k] = [_decode_mime_header(x) for x in value]
        else:
            headers[k] = _decode_mime_header(value)

    # replace content-type with the normalized value (good for searching)
    headers['Content-Type'] = message.get_content_type()

    # convert Date into UTC
    if headers.has_key('Date'):
        try:
            date = parse_date(headers['Date']).astimezone(tzutc())
            headers['Date'] = datetime.datetime.strftime(date, "%Y-%m-%dT%H:%M:%SZ")
        except:
            pass

    return headers

def _decode_mime_header(value):
    """
        Helper method to decode a single header string
    """
    return ' '.join(unicode(raw, charset) if charset is not None else raw for raw,charset in decode_header(value))


def mail_message2dict(data):
    """
        Parse the given MIME message and return its contents as a dict
    """
    try:
        message = message_from_string(data)
    except Exception, e:
        log.warning("Failed to parse MIME message: %r", e)
        return dict(_data=data)

    result = decode_message_headers(message)

    if message.is_multipart():
        for mime_part in message.walk():
            # skip top level part
            if not result.has_key('@parts'):
                result['@parts'] = []
                continue

            part = dict(mime_part.items())
            part['@body'] = mime_part.get_payload()

            # convert payload into actual string data
            if isinstance(part['@body'], list):
                part['@body'] = (str(x) for x in part['@body'])

            result['@parts'].append(part)
    else:
        result['@body'] = message.get_payload()

    return result


def imap_folder_path(uri):
    """
        Translate the folder name into a fully qualified folder path such as it
        would be used by a cyrus administrator.
    """
    if isinstance(uri, str):
        uri = parse_imap_uri(uri)

    username = uri['user']
    domain = uri['domain']
    folder_name = uri['path']

    # FIXME: prefix for shared folders; should be read from IMAP NAMESPACE command
    shared_prefix = conf.get('imap', 'sharedprefix')

    # Through filesystem
    # To get the mailbox path, use:
    # TODO: Assumption #1 is we are using virtual domains, and this domain does
    # TODO: Assumption #2 is the mailbox in question is a user mailbox
    # TODO: Assumption #3 is we use the unix hierarchy separator

    # Translate the folder name in to a fully qualified folder path such as it
    # would be used by a cyrus administrator.
    #
    # Other Users (covered, the netloc has the username the suffix is the
    # original folder name).
    if not username == None:
        if folder_name == "INBOX":
            folder_path = os.path.join('user', '%s@%s' % (username, domain))
        else:
            folder_path = os.path.join('user', username, '%s@%s' % (folder_name, domain))
    # Shared Folders
    else:
        if folder_name.startswith(shared_prefix):
            folder_path = folder_name[len(shared_prefix):]
        else:
            folder_path = folder_name

    return folder_path


def imap_mailbox_fs_path(uri):
    """
        Translate the given folder URI into a filesystem path where this mailbox is stored.
    """
    if isinstance(uri, str):
        uri = parse_imap_uri(uri)

    folder_name = uri['path']

    # Translate the folder name in to a fully qualified folder path such as it
    # would be used by a cyrus administrator.
    folder_path = imap_folder_path(uri)

    # Through filesystem
    # To get the mailbox path, use:
    # TODO: Assumption #1 is we are using virtual domains, and this domain does
    # TODO: Assumption #2 is the mailbox in question is a user mailbox
    # TODO: Assumption #3 is we use the unix hierarchy separator

    # TODO: Check if this file exists and is actually executable
    # New in Python 2.7:
    if hasattr(subprocess, 'check_output'):
        mailbox_path = subprocess.check_output(
                ["/usr/lib/cyrus-imapd/mbpath", folder_path]
            ).strip()
    else:
        # Do it the old-fashioned way
        p1 = subprocess.Popen(
                ["/usr/lib/cyrus-imapd/mbpath", folder_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

        (stdout, stderr) = p1.communicate()
        mailbox_path = stdout.strip()

    # TODO: Assumption #4 is we use altnamespace
    if not folder_name == "INBOX":
        if not len(folder_name.split('@')) > 0:
            mailbox_path = os.path.join(mailbox_path, folder_name)

    return mailbox_path
    