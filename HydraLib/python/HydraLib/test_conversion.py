#!/usr/python
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
# a

import datetime
from util import timestamp_to_ordinal, ordinal_to_timestamp
import unittest
class ConversionTest(unittest.TestCase):
    def test_conversion(self):
        for i in range(100):
            x = datetime.datetime.now()
            ordinal_x = timestamp_to_ordinal(x)
            y = ordinal_to_timestamp(ordinal_x)
            ordinal_y = timestamp_to_ordinal(y)
            assert x == y
            assert ordinal_x == ordinal_y

def run():
   # hydra_logging.init(level='DEBUG')
   # HydraIface.init(hdb.connect())
    unittest.main()
   # hdb.disconnect()
   # hydra_logging.shutdown()

if __name__ == "__main__":
    run() # run all tests
