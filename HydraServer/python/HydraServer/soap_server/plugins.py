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
from spyne.model.complex import Array as SpyneArray
from spyne.model.primitive import Unicode, Integer32
from hydra_complexmodels import Plugin
from spyne.decorator import rpc
from hydra_base import HydraService

from HydraServer.lib import plugins

class PluginService(HydraService):
    """
        Plugin SOAP service
    """

    @rpc(_returns=SpyneArray(Unicode))
    def get_plugins(ctx):
        """
            Get all available plugins
        """
        plug_ins = plugins.get_plugins(**ctx.in_header.__dict__)

        return plug_ins

    @rpc(Plugin, _returns=Unicode)
    def run_plugin(ctx, plugin):
        """
            Run a plugin
        """
      
        pid = plugins.run_plugin(plugin,
                                 **ctx.in_header.__dict__)

        return pid

       
    @rpc(Unicode, Integer32, _returns=Unicode)
    def check_plugin_status(ctx, plugin_name, pid):
        status = plugins.check_plugin_status(plugin_name,
                                             pid,
                                             **ctx.in_header.__dict__)
        return status
