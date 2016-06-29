# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
from spyne.model.primitive import Mandatory, String, Unicode
from spyne.error import Fault
from spyne.model.complex import ComplexModel
from spyne.decorator import srpc, rpc
from hydra_complexmodels import LoginResponse
import logging
from HydraServer.util.hdb import login_user
from HydraLib.HydraException import HydraError
from spyne.protocol.json import JsonDocument

from spyne.service import ServiceBase

log = logging.getLogger(__name__)
_session_db = dict()

def get_session_db():
    return _session_db

def set_session_db(session_db):
    global _session_db
    _session_db = session_db

class HydraDocument(JsonDocument):
    """An implementation of the json protocol
       with request headers working"""

    def create_in_document(self, ctx, in_string_encoding=None):
        super(HydraDocument, self).create_in_document(ctx, in_string_encoding)

    def decompose_incoming_envelope(self, ctx, message=JsonDocument.REQUEST):
        super(HydraDocument, self).decompose_incoming_envelope(ctx, message)
        req = ctx.transport.req

        ctx.in_header_doc = {}
        for r in req:
            if r.find('HTTP') == 0:
                key = r[5:].lower()
                val = [req[r].lower()]
                ctx.in_header_doc[key] = val

    def deserialize(self, ctx, message):
        super(HydraDocument, self).deserialize(ctx, message)
        if ctx.descriptor.in_header:
            in_header_class = ctx.descriptor.in_header[0]
            ctx.in_header = in_header_class()
            for k, v in ctx.in_header_doc.items():
                setattr(ctx.in_header, k, v[0])

class RequestHeader(ComplexModel):
    __namespace__ = 'hydra.base'
    session_id    = Unicode
    username      = Unicode
    user_id       = Unicode
    app_name      = Unicode

    def __init__(self):
        self.app_name = None
        self.user_id  = None
        self.username = None

class HydraService(ServiceBase):
    __tns__ = 'hydra.base'
    __in_header__ = RequestHeader

class AuthenticationError(Fault):
    __namespace__ = 'hydra.base'

    def __init__(self, user_name):
        Fault.__init__(self,
                faultcode='Client.AuthenticationError',
                faultstring='Invalid authentication request for %r' % user_name
            )

class AuthorizationError(Fault):
    __namespace__ = 'hydra.authentication'

    def __init__(self):

        Fault.__init__(self,
                faultcode='Client.AuthorizationError',
                faultstring='You are not authozied to access this resource.'
            )

class HydraServiceError(Fault):
    __namespace__ = 'hydra.error'

    def __init__(self, message, code="HydraError"):

        Fault.__init__(self,
                faultcode=code,
                faultstring=message
        )

class ObjectNotFoundError(HydraServiceError):
    __namespace__ = 'hydra.error'

    def __init__(self, message):

        Fault.__init__(self,
                faultcode='NoObjectFoundError',
                faultstring=message
        )

class LogoutService(HydraService):
    __tns__      = 'hydra.authentication'

    @rpc(Mandatory.String, _returns=String,
                                                    _throws=AuthenticationError)
    def logout(ctx, username):
        del(_session_db[ctx.in_header.session_id])
        return "OK"

class AuthenticationService(ServiceBase):
    __tns__      = 'hydra.base'

    @srpc(Mandatory.Unicode, Unicode, _returns=LoginResponse,
                                                   _throws=AuthenticationError)
    def login(username, password):
        try:

            if username is None:
                raise HydraError("No Username specified.")
            username = username.encode('utf-8')

            if password is None:
                raise HydraError("No password specified")
            password = password.encode('utf-8')

            user_id, session_id = login_user(username, password)
        except HydraError, e:
            raise AuthenticationError(e)

        _session_db[session_id] = (user_id, username)
        loginresponse = LoginResponse()
        loginresponse.session_id = session_id
        loginresponse.user_id    = user_id

        return loginresponse
