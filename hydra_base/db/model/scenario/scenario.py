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
import time
from ..base import *

from ..dataset import Dataset, Metadata
from ..network import ResourceAttr
from ..attributes import Attr
from ..ownership import DatasetOwner
from .resourcegroupitem import ResourceGroupItem

__all__ = ['Scenario', 'ResourceScenario']

class ResourceScenario(Base, Inspect):
    """
    """

    __tablename__='tResourceScenario'

    dataset_id = Column(Integer(), ForeignKey('tDataset.id'), nullable=False)
    scenario_id = Column(Integer(), ForeignKey('tScenario.id'), primary_key=True, nullable=False, index=True)
    resource_attr_id = Column(Integer(), ForeignKey('tResourceAttr.id'), primary_key=True, nullable=False, index=True)
    source           = Column(String(60))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    dataset      = relationship('Dataset', backref=backref("resourcescenarios", order_by=dataset_id))
    scenario     = relationship('Scenario', backref=backref("resourcescenarios", order_by=scenario_id, cascade="all, delete-orphan"))
    resourceattr = relationship('ResourceAttr', backref=backref("resourcescenarios", cascade="all, delete-orphan"), uselist=False)

    _parents  = ['tScenario', 'tResourceAttr']
    _children = ['tDataset']

    def get_dataset(self, user_id):
        dataset = get_session().query(Dataset.id,
                Dataset.type,
                Dataset.unit_id,
                Dataset.name,
                Dataset.hidden,
                case([(and_(Dataset.hidden=='Y', DatasetOwner.user_id is not None), None)],
                        else_=Dataset.value).label('value'),
                case([(and_(Dataset.hidden=='Y', DatasetOwner.user_id is not None), None)],
                        else_=Dataset.value).label('value')).filter(
                Dataset.id==self.id).outerjoin(DatasetOwner,
                                    and_(Dataset.id==DatasetOwner.dataset_id,
                                    DatasetOwner.user_id==user_id)).one()

        return dataset

    @property
    def value(self):
        return self.dataset

class Scenario(Base, Inspect):
    """
    """

    __tablename__='tScenario'
    __table_args__ = (
        UniqueConstraint('network_id', 'name', name="unique scenario name"),
    )

    id = Column(Integer(), primary_key=True, index=True, nullable=False)
    name = Column(String(200),  nullable=False)
    description = Column(String(1000))
    layout  = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'),  nullable=True)
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    network_id = Column(Integer(), ForeignKey('tNetwork.id'), index=True)
    start_time = Column(String(60))
    end_time = Column(String(60))
    locked = Column(String(1),  nullable=False, server_default=text(u"'N'"))
    time_step = Column(String(60))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    created_by = Column(Integer(), ForeignKey('tUser.id'), nullable=False)
    parent_id = Column(Integer(), ForeignKey('tScenario.id'), nullable=True)

    network = relationship('Network', backref=backref("scenarios", order_by=id))
    parent = relationship('Scenario', remote_side=[id],
        backref=backref("children", order_by=id))

    _parents  = ['tNetwork']
    _children = ['tResourceScenario']

    def add_resource_scenario(self, resource_attr, dataset=None, source=None):
        rs_i = ResourceScenario()
        if resource_attr.id is None:
            rs_i.resourceattr = resource_attr
        else:
            rs_i.resource_attr_id = resource_attr.id

        if dataset.id is None:
            rs_i.dataset = dataset
        else:
            rs_i.dataset_id = dataset.id
        rs_i.source = source
        self.resourcescenarios.append(rs_i)

    def add_resourcegroup_item(self, ref_key, resource, group_id):
        group_item_i = ResourceGroupItem()
        group_item_i.group_id = group_id
        group_item_i.ref_key  = ref_key
        if ref_key == 'GROUP':
            group_item_i.subgroup = resource
        elif ref_key == 'NODE':
            group_item_i.node     = resource
        elif ref_key == 'LINK':
            group_item_i.link     = resource
        self.resourcegroupitems.append(group_item_i)

    def get_data(self, user_id,
        child_data=None,
        get_parent_data=False,
        ra_ids=None,
        include_results=True,
        include_only_results=False,
        include_data_types=None,
        exclude_data_types=None,
        include_values=True,
        include_data_type_values=None,
        exclude_data_type_values=None,
        include_metadata=True):
        """
            Return all the resourcescenarios relevant to this scenario.
            If this scenario inherits from another, look up the tree to compile
            an exhaustive list of resourcescnearios, removing any duplicates, prioritising
            the ones closest to this scenario (my immediate parent's values are used instead
            of its parents)

            If an explicit list of RAs is provided, only return data for these. This is used
            when requesting data for a specific resource, for example.
        """

        #This avoids python's mutable keyword argumets causing child_data to keep its values beween
        #function calls
        if child_data is None:
            child_data = []

        #Idenify all existing resource attr ids, which take priority over anything in here
        childrens_ras = []
        for child_rs in child_data:
            childrens_ras.append(child_rs.resource_attr_id)
        t = time.time()
        resourcescenarios = self.get_all_resourcescenarios(
            user_id,
            ra_ids=ra_ids,
            include_results=include_results,
            include_only_results=include_only_results,
            include_metadata=include_metadata,
            include_data_types=include_data_types,
            exclude_data_types=exclude_data_types,
            include_values=include_values,
            include_data_type_values=include_data_type_values,
            exclude_data_type_values=exclude_data_type_values
            )
        log.info(f"get_all_resourcescenarios took {time.time() - t:.2f} seconds")
        for this_rs in resourcescenarios:
            if this_rs.resource_attr_id not in childrens_ras:
                child_data.append(this_rs)

        if self.parent is not None and get_parent_data is True:
            return self.parent.get_data(user_id, child_data=child_data,
                                        get_parent_data=get_parent_data,
                                       ra_ids=ra_ids)

        return child_data

    def get_all_resourcescenarios(self, user_id,
        ra_ids=None,
        include_results=True,
        include_only_results=False,
        include_data_types=None,
        exclude_data_types=None,
        include_values=True,
        include_data_type_values=None,
        exclude_data_type_values=None,
        include_metadata=True):
        """
            Get all the resource scenarios in a network, across all scenarios
            returns a dictionary of dict objects, keyed on scenario_id
        """
        log.debug("Starting dataset query")

        rs_qry = get_session().query(
                    Dataset.type,
                    Dataset.unit_id,
                    Dataset.name,
                    Dataset.hash,
                    Dataset.cr_date,
                    Dataset.created_by,
                    Dataset.hidden,
                    ResourceScenario.dataset_id,
                    ResourceScenario.scenario_id,
                    ResourceScenario.resource_attr_id,
                    ResourceScenario.source,
                    ResourceAttr.attr_id,
                    ResourceAttr.attr_is_var,
                    ResourceAttr.ref_key,
                    ResourceAttr.node_id,
                    ResourceAttr.network_id,
                    ResourceAttr.link_id,
                    ResourceAttr.group_id,
                    Attr.name.label('attr_name'),
                    Attr.description.label('attr_description')
        ).outerjoin(DatasetOwner, and_(DatasetOwner.dataset_id==Dataset.id, DatasetOwner.user_id==user_id)).filter(
                    or_(Dataset.hidden=='N', Dataset.created_by==user_id, DatasetOwner.user_id != None),
                    ResourceAttr.id == ResourceScenario.resource_attr_id,
                    Scenario.id==ResourceScenario.scenario_id,
                    Scenario.id==self.id,
                    Dataset.id==ResourceScenario.dataset_id,
                    Attr.id==ResourceAttr.attr_id)

        if include_results is False:
            rs_qry = rs_qry.filter(ResourceAttr.attr_is_var=='N')

        if include_only_results is True:
            rs_qry = rs_qry.filter(ResourceAttr.attr_is_var=='Y')

        if ra_ids is not None:
            rs_qry = rs_qry.filter(ResourceScenario.resource_attr_id.in_(ra_ids))
        
        if include_data_types is not None:
            rs_qry = rs_qry.filter(func.upper(Dataset.type).in_([t.upper() for t in include_data_types]))
        
        if exclude_data_types is not None:
            rs_qry = rs_qry.filter(func.upper(Dataset.type).not_in([t.upper() for t in exclude_data_types]))

        excl_values_rs = []
        if include_values is True:
            if include_data_type_values is not None:
                rs_qry = rs_qry.filter(func.upper(Dataset.type).in_([t.upper() for t in include_data_type_values]))

            if exclude_data_type_values is not None:
                excl_values_qry = rs_qry.filter(func.upper(Dataset.type).in_([t.upper() for t in exclude_data_type_values]))
                rs_qry = rs_qry.filter(func.upper(Dataset.type).not_in([t.upper() for t in exclude_data_type_values]))
                excl_values_rs = excl_values_qry.all()

            rs_qry = rs_qry.add_columns(Dataset.value)

        non_dataframe_rs = rs_qry.all()
        
        all_rs = non_dataframe_rs + excl_values_rs

        log.info(f"Ending dataset query -- {len(all_rs)} results")
        t = time.time()
        processed_rs = []
        for rs in all_rs:
            rs_obj = JSONObject({
                'resource_attr_id': rs.resource_attr_id,
                'scenario_id':rs.scenario_id,
                'dataset_id':rs.dataset_id,
                'resourceattr': {
                    'id': rs.resource_attr_id,
                    'attr_id':rs.attr_id,
                    'attr_is_var': rs.attr_is_var,
                    'ref_key': rs.ref_key,
                    'node_id': rs.node_id,
                    'link_id': rs.link_id,
                    'network_id': rs.network_id,
                    'group_id': rs.group_id,
                    'attr':{
                        'name': rs.attr_name,
                        'description':rs.attr_description,
                        'id': rs.attr_id
                    }
                },
                'dataset': {
                    'id':rs.dataset_id,
                    'type' : rs.type,
                    'unit_id' : rs.unit_id,
                    'name' : rs.name,
                    'hash' : rs.hash,
                    'cr_date':rs.cr_date,
                    'created_by':rs.created_by,
                    'hidden':rs.hidden,
                    'value': getattr(rs, 'value', None),
                    'metadata':{},
                }
            }, normalize=False)

            processed_rs.append(rs_obj)
        log.info(f"Datasets processed in {time.time() - t:.2f} seconds")

        ## If metadata is requested, use a dedicated query to extract metadata
        ## from the scenario's datasets,
        ## and enter them into a lookup table, keyed by dataset_id so they can
        ## be extracted later.
        metadata_lookup = {}
        if include_metadata is True:
            dataset_ids = [rs.dataset.id for rs in processed_rs]
            metadata = get_session().query(Metadata)\
                        .join(Dataset)\
                        .join(ResourceScenario)\
                        .filter(ResourceScenario.scenario_id == self.id).all()
            for m in metadata:
                if metadata_lookup.get(m.dataset_id):
                    metadata_lookup[m.dataset_id][m.key] = m.value
                else:
                    metadata_lookup[m.dataset_id] = {m.key:m.value}
            for rs in processed_rs:
                if rs.dataset.value is not None:
                    rs.dataset.metadata = metadata_lookup.get(rs.dataset.id, {})
        return processed_rs

    def get_group_items(self, child_items=None, get_parent_items=False):
        """
            Return all the resource group items relevant to this scenario.
            If this scenario inherits from another, look up the tree to compile
            an exhaustive list of resource group items, removing any duplicates, prioritising
            the ones closest to this scenario (my immediate parent's values are used instead
            of its parents)
        """

        #This avoids python's mutable keyword argumets causing child_items to keep its values beween
        #function calls
        if child_items == None:
            child_items = []

        #Idenify all existing resource attr ids, which take priority over anything in here
        childrens_groups = []
        for child_rgi in child_items:
            childrens_groups.append(child_rgi.group_id)

        #Add resource attributes which are not defined already
        for this_rgi in self.resourcegroupitems:
            if this_rgi.group_id not in childrens_groups:
                child_items.append(this_rgi)

        if self.parent is not None and get_parent_items is True:
            return self.parent.get_group_items(child_items=child_items,
                                               get_parent_items=get_parent_items)

        return child_items




