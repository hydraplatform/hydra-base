
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
import logging
log = logging.getLogger(__name__)
import os
import glob
import importlib
modules = glob.glob(os.path.dirname(__file__)+"/*.py")
mods = []
for f in modules:
    if not os.path.basename(f).startswith('_'):
        mods.append(os.path.basename(f)[:-3])

__all__ = mods

services = []
for x in __all__:
    y = importlib.import_module("HydraServer.plugins.%s"%x)
    services.append(y.Service)

