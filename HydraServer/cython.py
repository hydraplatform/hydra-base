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
#!/usr/bin/env python

#
#   Cython -- Main Program, generic
#

if __name__ == '__main__':

    import os
    import sys

    # Make sure we import the right Cython
    cythonpath, _ = os.path.split(os.path.realpath(__file__))
    sys.path.insert(0, cythonpath)

    from Cython.Compiler.Main import main
    main(command_line = 1)

else:
    # Void cython.* directives.
    from Cython.Shadow import *
