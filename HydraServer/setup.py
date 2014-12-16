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
import sys
from cx_Freeze import setup, Executable

includes = ["encodings.utf_8",\
            "encodings.ascii",\
            "lxml._elementpath",\
            "ConfigParser",\
            "mysql.connector",\
            "suds",\
	    "pytz",\
            "spyne",\
            "spyne.service",
            "sqlite3"]

setup(
    name = "On Dijkstra's Algorithm",
    version = "3.1",
    description = "A Dijkstra's Algorithm help tool.",
    executables = [Executable("server.py"), Executable("test.py")],
    options     = {
                        "build_exe": 
                        {
                            "includes": includes,
                        }
                }
)
