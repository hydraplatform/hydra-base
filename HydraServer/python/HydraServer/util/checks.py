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
"""This module implements different checks for the validity of networks and
scenarios.
"""

from HydraServer.db import HydraIface


def check_scenario(scenario_id):
    """Implement different checks for the validity of a scenario.
    """

    messages = []

    scenario = HydraIface.Scenario(scenario_id=scenario_id)
    scenario.load_all()

    for res_scenario in scenario.resourcescenarios:
        consistent, message = dimension_consistency(res_scenario)
        messages.append(message)

    return messages


def check_network(network_id):
    """Implement different checks for the validity of a network.
    """


def dimension_consistency(resource_scenario):
    """Check if the dimensions of attributes and assigned datasets are
    consistent. Dimensions of datasets are assumed to be consistent with the
    dimension of an attribute if:

        1. No dimensions are specified for both, attribute and dataset;

        2. No dimension is specified for one of them;

        3. The dimensions of both, dataset and attribute, match.
    """
    res_attr = resource_scenario.get_resource_attr()
    scen_data = resource_scenario.get_dataset()
    attr = HydraIface.Attr(attr_id=res_attr.db.attr_id)
    attr_dim = attr.db.attr_dimen
    if attr_dim == '':
        attr_dim = None
    data_dim = scen_data.db.data_dimen
    if data_dim == '':
        data_dim = None

    resource = res_attr.get_resource()
    resource_type = res_attr.db.ref_key.lower()

    message = "Dimensions are consistent for %s '%s.%s' (ID: %s.%s)" + \
        "and dataset '%s' (ID: %s)."
    info = (resource_type,
            eval('resource.db.' + resource_type + '_name'),
            attr.db.attr_name,
            eval('resource.db.' + resource_type + '_id'),
            attr.db.attr_id,
            scen_data.db.data_name,
            scen_data.db.dataset_id)

    if attr_dim is None or data_dim is None:
        return True, message % info
    elif attr_dim == data_dim:
        return True, message % info
    else:
        message = "WARNING: inconsistent dimensions for %s '%s.%s'" + \
            "(ID: %s.%s) and dataset '%s' (ID: %s)."
        return False, message % info


if __name__ == '__main__':
    from HydraLib import hdb
    connection = hdb.connect()
    HydraIface.init(connection)
    messages = check_scenario(2)
    for i in messages:
        print i
