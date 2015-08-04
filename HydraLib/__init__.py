# (c) Copyright 2013, 2014, University of Manchester
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
import logging
import config
if config.CONFIG is None:
    config.load_config()

import hydra_logging
hydra_logging.init()


log = logging.getLogger(__name__)

log.info(" \n\n\n ")

log.info("CONFIG localfiles %s found in %s", len(config.localfiles), config.localfile)

log.info("CONFIG repofiles %s found in %s", len(config.repofiles), config.repofile)

log.info("CONFIG userfiles %s found in %s", len(config.userfiles), config.userfile)

log.info("CONFIG sysfiles %s found in %s", len(config.sysfiles), config.sysfile)

if len(config.sysfiles) + len(config.repofiles) + len(config.userfiles) + len(config.sysfiles):
    log.critical("No config found. Please put your ini file into one of the files listed beside CONFIG above.")

log.info(" \n\n\n ")
