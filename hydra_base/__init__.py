#!/usr/bin/env python
# -*- coding: utf-8 -*-

# (c) Copyright 2013 to 2017 University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
#!/usr/bin/env python
# -*- coding: utf-8 -*-

__version__="0.1.7"

import logging
from . import config
if config.CONFIG is None:
    config.load_config()


from . import hydra_logging
hydra_logging.init()

from .db import connect, commit_transaction, rollback_transaction

log = logging.getLogger(__name__)

log.debug(" \n ")

log.debug("CONFIG localfiles %s found in %s", len(config.localfiles), config.localfile)

log.debug("CONFIG repofiles %s found in %s", len(config.repofiles), config.repofile)

log.debug("CONFIG userfiles %s found in %s", len(config.userfiles), config.userfile)

log.debug("CONFIG sysfiles %s found in %s", len(config.sysfiles), config.sysfile)

if len(config.sysfiles) + len(config.repofiles) + len(config.userfiles) + len(config.sysfiles) == 0:
    log.critical("No config found. Please put your ini file into one of the files listed beside CONFIG above.")

log.debug(" \n ")

from .lib.attributes import *
from .lib.data import *
from .lib.groups import *
from .lib.network import *
from .lib.notes import *
from .lib.objects import *
from .lib.plugins import *
from .lib.project import *
from .lib.rules import *
from .lib.scenario import *
from .lib.sharing import *
from .lib.static import *
from .lib.template import *
from .lib.units import *
from .lib.users import *
from .lib.service import *
