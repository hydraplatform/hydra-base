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
# -*- coding: utf-8 -*-

import server
import timeit
import logging
log = logging.getLogger(__name__)
import cProfile, pstats, StringIO

class NetworkTest(server.SoapServerTest):

    def create_large_network(self):
        self.create_network_with_data(num_nodes=1000, ret_full_net=False)

    def test_add_large_network(self):
        time = timeit.Timer(self.create_large_network).timeit(number=1)
        log.debug(time)
        assert time < 50

    #def test_get_network(self):
    #    n = self.client.service.get_network(1000)
    #    log.info(n)

if __name__ == '__main__':
  #  pr = cProfile.Profile()
  #  pr.enable()
  #  server.run()
  #  pr.disable()
  #  s = StringIO.StrinigIO()
  #  sortby = 'cumulative'
  #  ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
  #  ps.print_stats()
  #  print s.getvalue()
    server.run()
