#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run performance tests for different sizes of networks and different numbers
of attributes."""

import subprocess
import time

from ImportCSV import ImportCSV
from generate_networks import create_network

nodenumbers = [256]
#nodenumbers = [2, 4, 8, 16]
#attrnumbers = [2, 4, 8, 16, 32, 64, 128, 256]
attrnumbers = [256]


if __name__ == '__main__':

    logfile = open('performance.log', 'a')
    try:
        for n in nodenumbers:
            for m in attrnumbers:
                res = subprocess.call(['./clear_db.sh'])
                if res == 0:
                    print "MySQL database cleared successfully..."

                res = subprocess.call(['python', '../../../../hydra_daemon.py',
                                       'start'])
                if res == 0:
                    time.sleep(2)
                    print "Started Hydra server successfully..."

                testcase = ImportCSV()
                testdata = create_network(n, m)
                testcase.node_data = testdata['nodes']
                testcase.link_data = testdata['links']
                testcase.create_project(ID=1000)
                testcase.create_scenario(name='Performance test scenario')

                startime = time.time()
                testcase.create_network()
                endtime = time.time()
                exectime = endtime - startime
                print "create_network took %s seconds." % exectime

                startime = time.time()
                testcase.create_nodes()
                endtime = time.time()
                exectime = endtime - startime
                print "create_nodes took %s seconds." % exectime

                startime = time.time()
                testcase.create_links()
                
                endtime = time.time()
                exectime = endtime - startime
                print "create_links took %s seconds." % exectime

                # Create a complete network structure
                testcase.Network.scenarios.Scenario.append(testcase.Scenario)
                for node in testcase.Nodes.values():
                    testcase.Network.nodes.Node.append(node)
                for link in testcase.Links.values():
                    testcase.Network.links.Link.append(link)

                # Commit the network and measure time
                startime = time.time()

                net = testcase.cli.service.add_network(testcase.Network)

                endtime = time.time()
                exectime = endtime - startime

                logfile.write("%s, %s, %s\n" % (n, m, exectime))
                print n, m, exectime
                del testcase
                res = subprocess.call(['python', '../../../../hydra_daemon.py',
                                       'stop'])
                if res == 0:
                    print "Stopped server successfully..."

        logfile.close()
    except:
        res = subprocess.call(['python', '../../../../hydra_daemon.py',
                               'stop'])
        if res == 0:
            print "Stopped server successfully after exception..."
        raise
