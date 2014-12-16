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
import os
import glob
import ConfigParser
import sys

global CONFIG
CONFIG = None

def load_config():
    """Load a config file. This function looks for a config (*.ini) file in the
    following order::

        (1) ./*.ini
        (2) ~/.config/hydra/
        (3) /etc/hydra
        (4) [...]/HYDRA/HydraLib/trunk/../../config/

    (1) will override (2) will override (3) will override (4). Parameters not
    defined in (1) will be taken from (2). Parameters not defined in (2) will
    be taken from (3).  (3) is the config folder that will be checked out from
    the svn repository.  (2) Will be be provided as soon as an installable
    distribution is available. (1) will usually be written individually by
    every user."""

    global CONFIG

    #TODO: Check for the operating system we are running, provide search paths
    #      for Windows machines.
    modulepath = os.path.dirname(os.path.abspath(__file__))

    localfiles = glob.glob(os.getcwd() + '/*.ini')
    userfiles = glob.glob(os.path.expanduser('~') + '/.config/hydra/*.ini')
    sysfiles = glob.glob('/etc/hydra/*.ini')
    repofiles = glob.glob(modulepath + '/../config/*.ini')

    if os.name == 'nt':
        import winpaths
        userfiles = glob.glob(os.path.expanduser('~') + '/AppData/Local/hydra.ini')
        sysfiles = glob.glob(winpaths.get_common_documents() + '/Hydra/*.ini')
    
    config = ConfigParser.ConfigParser(allow_no_value=True)

    for ini_file in repofiles:
        config.read(ini_file)
    for ini_file in sysfiles:
        config.read(ini_file)
    for ini_file in userfiles:
        config.read(ini_file)
    for ini_file in localfiles:
        config.read(ini_file)

    if os.name == 'nt':
        set_windows_env_variables(config)

    try:
        home_dir = config.get('DEFAULT', 'home_dir')
    except:
        home_dir = os.environ.get('HYDRA_HOME_DIR', '~')
    config.set('DEFAULT', 'home_dir', os.path.expanduser(home_dir))

    try:
        hydra_base = config.get('DEFAULT', 'hydra_base_dir')
    except:
        hydra_base = os.environ.get('HYDRA_BASE_DIR', '~/svn/HYDRA')
    config.set('DEFAULT', 'hydra_base_dir', os.path.expanduser(hydra_base))


    CONFIG = config

    return config

def set_windows_env_variables(config):
    import winpaths
    config.set('DEFAULT', 'common_app_data_folder', winpaths.get_common_appdata())
    config.set('DEFAULT', 'win_local_appdata', winpaths.get_local_appdata())
    config.set('DEFAULT', 'win_appdata', winpaths.get_appdata())
    config.set('DEFAULT', 'win_desktop', winpaths.get_desktop())
    config.set('DEFAULT', 'win_programs', winpaths.get_programs())
    config.set('DEFAULT', 'win_common_admin_tools', winpaths.get_common_admin_tools())
    config.set('DEFAULT', 'win_common_documents', winpaths.get_common_documents())
    config.set('DEFAULT', 'win_cookies', winpaths.get_cookies())
    config.set('DEFAULT', 'win_history', winpaths.get_history())
    config.set('DEFAULT', 'win_internet_cache', winpaths.get_internet_cache())
    config.set('DEFAULT', 'win_my_pictures', winpaths.get_my_pictures())
    config.set('DEFAULT', 'win_personal', winpaths.get_personal())
    config.set('DEFAULT', 'win_my_documents', winpaths.get_my_documents())
    config.set('DEFAULT', 'win_program_files', winpaths.get_program_files())
    config.set('DEFAULT', 'win_program_files_common', winpaths.get_program_files_common())
    config.set('DEFAULT', 'win_system', winpaths.get_system())
    config.set('DEFAULT', 'win_windows', winpaths.get_windows())
    config.set('DEFAULT', 'win_startup', winpaths.get_startup())
    config.set('DEFAULT', 'win_recent', winpaths.get_recent())

def get(section, option, default=None):

    if CONFIG is None:
        load_config()

    try:
        return CONFIG.get(section, option)
    except:
        return default

def getint(section, option, default=None):

    if CONFIG is None:
        load_config()

    try:
        return CONFIG.getint(section, option)
    except:
        return default
    
