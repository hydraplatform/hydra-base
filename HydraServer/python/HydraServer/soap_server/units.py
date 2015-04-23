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

from spyne.model.primitive import Unicode, Boolean, Decimal, Integer32
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from spyne.util.dictdoc import get_object_as_dict
from hydra_base import HydraService
from hydra_complexmodels import Unit, Dimension

from HydraServer.lib import units

class UnitService(HydraService):
    """
    """

    @rpc(Unicode, _returns=Boolean)
    def add_dimension(ctx, dimension):
        """Add a physical dimensions (such as ``Volume`` or ``Speed``) to the
        servers list of dimensions. If the dimension already exists, nothing is
        done.
        """
        result = units.add_dimension(dimension, **ctx.in_header.__dict__)
        return result

    @rpc(Unicode, _returns=Boolean)
    def delete_dimension(ctx, dimension):
        """Delete a physical dimension from the list of dimensions. Please note
        that deleting works only for dimensions listed in the custom file.
        """
        result = units.delete_dimension(dimension, **ctx.in_header.__dict__)
        return result

    @rpc(Unit, _returns=Boolean)
    def add_unit(ctx, unit):
        """Add a physical unit to the servers list of units. The Hydra server
        provides a complex model ``Unit`` which should be used to add a unit.

        A minimal example:

        .. code-block:: python

            from HydraLib import PluginLib

            cli = PluginLib.connect()

            new_unit = cli.factory.create('hyd:Unit')
            new_unit.name = 'Teaspoons per second'
            new_unit.abbr = 'tsp s^-1'
            new_unit.cf = 0               # Constant conversion factor
            new_unit.lf = 1.47867648e-05  # Linear conversion factor
            new_unit.dimension = 'Volumetric flow rate'
            new_unit.info = 'A flow of one teaspoon per second.'

            cli.service.add_unit(new_unit)
        """
        # Convert the complex model into a dict
        unitdict = get_object_as_dict(unit, Unit)
        units.add_unit(unitdict, **ctx.in_header.__dict__)
        return True

    @rpc(Unit, _returns=Boolean)
    def update_unit(ctx, unit):
        """Update an existing unit added to the custom unit collection. Please
        not that units built in to the library can not be updated.
        """
        unitdict = get_object_as_dict(unit, Unit)
        result = units.update_unit(unitdict, **ctx.in_header.__dict__)
        return result

    @rpc(Unit, _returns=Boolean)
    def delete_unit(ctx, unit):
        """Delete a unit from the custom unit collection.
        """
        unitdict = get_object_as_dict(unit, Unit)
        result = units.delete_unit(unitdict, **ctx.in_header.__dict__)
        return result

    @rpc(Decimal(min_occurs=1, max_occurs="unbounded"),
         Unicode, Unicode,
         _returns=Decimal(min_occurs="1", max_occurs="unbounded"))
    def convert_units(ctx, values, unit1, unit2):
        """Convert a value from one unit to another one.

        Example::

            >>> cli = PluginLib.connect()
            >>> cli.service.convert_units(20.0, 'm', 'km')
            0.02
        """
        return units.convert_units(values, unit1, unit2, **ctx.in_header.__dict__)

    @rpc(Integer32, Unicode, _returns=Integer32)
    def convert_dataset(ctx, dataset_id, to_unit):
        """Convert a whole dataset (specified by 'dataset_id' to new unit
        ('to_unit').
        """
        return units.convert_dataset(dataset_id, to_unit, **ctx.in_header.__dict__)

    @rpc(Unicode, _returns=Unicode)
    def get_dimension(ctx, unit1):
        """Get the corresponding physical dimension for a given unit.

        Example::

            >>> cli = PluginLib.connect()
            >>> cli.service.get_dimension('m')
            Length
        """
        dim = units.get_dimension(unit1, **ctx.in_header.__dict__)

        return dim

    @rpc(_returns=SpyneArray(Unicode))
    def get_dimensions(ctx):
        """Get a list of all physical dimensions available on the server.
        """
        dim_list = units.get_dimensions(**ctx.in_header.__dict__)
        return dim_list

    @rpc(_returns=SpyneArray(Dimension))
    def get_all_dimensions(ctx):
        """Get a list of all physical dimensions available on the server.
        """
        dimdict = units.get_all_dimensions(**ctx.in_header.__dict__)
        dimens = []
        for dim_name, unit_list in dimdict.items():
            dimens.append(Dimension(dim_name, unit_list))
        return dimens

    @rpc(Unicode, _returns=SpyneArray(Unit))
    def get_units(ctx, dimension):
        """Get a list of all units corresponding to a physical dimension.
        """
        unit_list = units.get_units(dimension, **ctx.in_header.__dict__)
        return unit_list

    @rpc(Unicode, Unicode, _returns=Boolean)
    def check_consistency(ctx, unit, dimension):
        """Check if a given units corresponds to a physical dimension.
        """
        return units.check_consistency(unit, dimension, **ctx.in_header.__dict__)
