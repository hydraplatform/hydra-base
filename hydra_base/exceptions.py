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

class HydraError(Exception):
    def __init__(self, message="A hydra error has occurred", code=0000):
        # Call the base class constructor with the parameters it needs
        self.code = code
        self.message = message
        Exception.__init__(self)

class HydraDBError(HydraError):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        code = error_codes.get('HYDRADB', '0000')
        HydraError.__init__(self, message, code)

class HydraPluginError(HydraError):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        code = error_codes.get('HYDRAPLUGIN', '0000')
        HydraError.__init__(self, message, code)

class ResourceNotFoundError(HydraError):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        code = error_codes.get('HYDRARESOURCE', '0000')
        HydraError.__init__(self, message, code)

class HydraAttributeError(HydraError):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        code = error_codes.get('HYDRAATTR', '0000')
        HydraError.__init__(self, message, code)

class PermissionError(HydraError):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        code = error_codes.get('HYDRAPERM', '0000')
        HydraError.__init__(self, message, code)

class OwnershipError(HydraError):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        code = error_codes.get('HYDRAOWNER', '0000')
        HydraError.__init__(self, message, code)

class DataError(HydraError):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        code = error_codes.get('HYDRADATA', '0000')
        HydraError.__init__(self, message, code)

class ValidationError(HydraError):
    pass

#
#ERROR CODES FOR HYDRA
#Categories are:
#DB Errors:         100 - 199
#Plugin Errors:     200 - 299
#ResourceErrors:    300 - 399
#Attribute Errors:  400 - 499
#Permission Errors: 500 - 599
#Data Errors        600 - 699
#Ownership Errors   700 - 799
#
error_codes = {
    'HYDRADB'      : "100",
    'HYDRAPLUGIN'  : "200",
    'HYDRARESOURCE': "300",
    'HYDRAATTR'    : "400",
    'HYDRAPERM'    : "500",
    'HYDRADATA'    : "600",
    'HYDRAOWNER'   : "700",
}
