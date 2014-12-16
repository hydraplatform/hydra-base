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
import os
from daemon import Daemon

from server import HydraServer


class HydraDaemon(Daemon):

    def run(self):
        server = HydraServer()
        server.run_server()


if __name__ == '__main__':

    HOMEDIR = os.path.expanduser('~')
    pidfile = HOMEDIR + '/.hydra/hydra_server.pid'
    outfile = HOMEDIR + '/.hydra/hydra.out'
    errfile = HOMEDIR + '/.hydra/hydra.err'

    daemon = HydraDaemon(pidfile, stdout=outfile, stderr=errfile)

    if len(sys.argv) == 2:
            if 'start' == sys.argv[1]:
                    daemon.start()
            elif 'stop' == sys.argv[1]:
                    daemon.stop()
            elif 'restart' == sys.argv[1]:
                    daemon.restart()
            else:
                    print "Unknown command"
                    sys.exit(2)
            sys.exit(0)
    else:
            print "usage: %s start|stop|restart" % sys.argv[0]
            sys.exit(2)
