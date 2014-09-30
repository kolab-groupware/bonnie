import urllib
import urlparse
from email import message_from_string

import bonnie
conf = bonnie.getConf()
log = bonnie.getLogger('utils')


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


def mail_message2dict(data):
    """
        Parse the given MIME message and return its contents as a dict
    """
    try:
        message = message_from_string(data)
    except Exception, e:
        log.warning("Failed to parse MIME message: %r", e)
        return dict(_data=data)

    result = dict(message.items())

    if message.is_multipart():
        for mime_part in message.walk():
            # skip top level part
            if not result.has_key('_parts'):
                result['_parts'] = []
                continue

            part = dict(mime_part.items())
            part['_data'] = mime_part.get_payload()

            # convert payload into actual string data
            if isinstance(part['_data'], list):
                part['_data'] = (str(x) for x in part['_data'])

            result['_parts'].append(part)
    else:
        result['_data'] = message.get_payload()

    return result
    