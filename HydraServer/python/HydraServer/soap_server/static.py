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
from spyne.model.primitive import Unicode, Integer32
from spyne.model.binary import ByteArray
from spyne.decorator import rpc
from hydra_base import HydraService
from HydraServer.lib import static

class ImageService(HydraService):
    """
        The network SOAP service.
    """

    @rpc(Unicode, ByteArray, _returns=Unicode)
    def add_image(ctx, name, file):
        static.add_image(name,
                         file,
                         **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Unicode, _returns=ByteArray)
    def get_image(ctx, name):

        encoded_file = static.get_image(name,
                                       **ctx.in_header.__dict__)
        return encoded_file

    @rpc(Unicode, _returns=Unicode)
    def remove_image(ctx, name):
        static.remove_image(name,
                            **ctx.in_header.__dict__)
        return 'OK'


class FileService(HydraService):
    """
        The network SOAP service.
    """

    @rpc(Unicode, Integer32, Unicode, ByteArray, _returns=Unicode)
    def add_file(ctx, resource_type, resource_id, name, file):
        static.add_file(resource_type,
                        resource_id,
                        name,
                        file,
                        **ctx.in_header.__dict__)

        return 'OK'

    @rpc(Unicode,Integer32,Unicode, _returns=ByteArray)
    def get_file(ctx, resource_type, resource_id, name):
        encoded_file = static.get_file(resource_type,
                                       resource_id,
                                       name,
                                       **ctx.in_header.__dict__)

        return encoded_file

    @rpc(Unicode,Integer32, Unicode, _returns=Unicode)
    def remove_file(ctx, resource_type, resource_id, name):
        static.remove_file(resource_type,
                           resource_id,
                           name,
                           **ctx.in_header.__dict__)
        return 'OK'

