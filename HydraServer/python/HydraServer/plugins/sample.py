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
from spyne.decorator import rpc
from spyne.model.primitive import Unicode
from HydraServer.soap_server.hydra_base import HydraService
import logging
log = logging.getLogger(__name__)

class Service(HydraService):
    """
        An example of a server-side plug-in
    """

    @rpc(Unicode, _returns=Unicode)
    def echo(ctx, message):
        return message
