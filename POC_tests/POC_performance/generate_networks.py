#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This little script generates a network of a specified size and writes it to
CSV files. The CSV files can then be imported using the ImportCSV plugin.

Basic usage::

    python generate_networks.py -n N

"""

import argparse as ap
import numpy.random as nr


def create_network(nnodes, nattr):
    nodes = []
    data = []
    links = []

    # create nodes
    node_header = 'name, x, y, description'
    units = ', , ,'
    for n in range(nnodes):
        node = ('node%04d' % n, 0, 0, 'Node number %d' % n)
        nodes.append(node)

        tmpdata = ()
        for m in range(nattr):
            tmpdata += (nr.random() * 100,)
        data.append(tmpdata)

    # create links
    link_header = 'name, from, to, description'
    for n in range(nnodes - 1):
        link = ('link%04d' % n, nodes[n][0], nodes[n + 1][0],
                'Link number %d' % n)
        links.append(link)

    datatemplate = ''
    for n in range(nattr):
        attr = ', attr%04d' % n
        node_header += attr
        link_header += attr
        units += ' ,'
        datatemplate += ', %s'

    node_header += '\n'
    link_header += '\n'
    units += '\n'
    datatemplate += '\n'

    nodefile = node_header + units
    for n in range(nnodes):
        nodefile += '%s, %s, %s, %s' % nodes[n]
        nodefile += datatemplate % data[n]

    linkfile = link_header + units
    for n in range(nnodes - 1):
        linkfile += '%s, %s, %s, %s' % links[n]
        linkfile += datatemplate % data[n]

    return {'nodes': nodefile, 'links': linkfile}


def commandline_parser():
    parser = ap.ArgumentParser()
    parser.add_argument('-n', '--nnodes', help='Number of nodes.')
    parser.add_argument('-a', '--nattr', help='Number of attributes.')
    parser.add_argument('-o', '--output', nargs='+', help='Output file names.')
    return parser


if __name__ == '__main__':
    parser = commandline_parser()
    args = parser.parse_args()
    try:
        files = create_network(int(args.nnodes), int(args.nattr))
    except ValueError:
        print "Input values must be integers."

    if args.output is None:
        print files['nodes']
        print files['links']
    else:
        with open(args.output[0], 'w') as nodefile:
            nodefile.write(files['nodes'])
        with open(args.output[1], 'w') as linkfile:
            linkfile.write(files['links'])
