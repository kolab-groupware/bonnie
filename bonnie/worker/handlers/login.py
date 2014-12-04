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

"""
    Base handler for an event notification of type 'Login'
"""

from bonnie.worker.handlers import HandlerBase

class LoginHandler(HandlerBase):
    """
        A *Login* event notification handler.

        The event notification is a JSON object with the following
        layout::

            {
                    "event":"Login",
                    "timestamp":"2014-11-27T10:45:38.201+01:00",
                    "service":"imap",
                    "serverDomain":"10.8.14.13",
                    "serverPort":143,
                    "clientIP":"10.8.13.10",
                    "clientPort":45735,
                    "uri":"imap://imapb13.example.org",
                    "pid":14210,
                    "user":"alexander.aachen@example.org",
                    "vnd.cmu.sessionId":"cyrus-imapd-14210-1417081537-1-10541965771286434889"
                }

    """

    event = 'Login'

    def __init__(self, *args, **kw):
        super(LoginHandler, self).__init__(*args, **kw)
