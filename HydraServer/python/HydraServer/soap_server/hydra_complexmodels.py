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
from spyne.model.complex import Array as SpyneArray, ComplexModel
from spyne.model.primitive import Unicode
from spyne.model.primitive import Integer
from spyne.model.primitive import Decimal
from spyne.model.primitive import AnyDict
from spyne.model.primitive import Double
from decimal import Decimal as Dec
from HydraLib.hydra_dateutil import get_datetime,\
        timestamp_to_ordinal,\
        ordinal_to_timestamp
import pandas as pd
import logging
from HydraServer.util import generate_data_hash
import json
import zlib
from HydraLib import config
from HydraLib.HydraException import HydraError

NS = "soap_server.hydra_complexmodels"
log = logging.getLogger(__name__)

def get_timestamp(ordinal):
    if ordinal is None:
        return None

    if type(ordinal) in (str, unicode):
        ordinal = Dec(ordinal)
    timestamp = str(ordinal_to_timestamp(ordinal))
    return timestamp


ref_id_map = {
    'NODE'     : 'node_id',
    'LINK'     : 'link_id',
    'GROUP'    : 'group_id',
    'NETWORK'  : 'network_id',
    'SCENARIO' : 'scenario_id',
    'PROJECT'  : 'project_id'
}


class HydraComplexModel(ComplexModel):
    __namespace__ = 'soap_server.hydra_complexmodels'

    def get_outgoing_layout(self, resource_layout):
        layout = {}
        if resource_layout not in (None, ""):
            db_layout    = eval(resource_layout)
            for k, v in db_layout.items():
                layout[k] = v

        return layout

class LoginResponse(HydraComplexModel):
    """
    """
    __namespace__ = 'soap_server.hydra_complexmodels'
    _type_info = [
        ('session_id', Unicode(min_occurs=1)),
        ('user_id',    Integer(min_occurs=1)),
    ]

class ResourceData(HydraComplexModel):
    """
        An object which represents a resource attr, resource scenario and dataset
        all in one.


        * **attr_id:** The ID of the attribute to which this data belongs
        * **scenario_id:** The ID of the scenario in which this data has been assigned
        * **resource_attr_id:** The unique ID representing the attribute and resource combination
        * **ref_key:** Indentifies the type of resource to which this dataset is attached. Can be 'NODE', 'LINK', 'GROUP', 'NETWORK' or 'PROJECT'
        * **ref_id:** The ID of the node, link, group, network, or project in question
        * **attr_is_var:** Flag to indicate whether this resource's attribute is a variable and hence should be filled in by a model
        * **dataset_id:** The ID of the dataset which has been assigned to the resource attribute
        * **dataset_type:** The type of the dataset -- can be scalar, descriptor, array or timeseries
        * **dataset_dimension:** The dimension of the dataset (This MUST match the dimension of the attribute)
        * **dataset_unit:** The unit of the dataset.
        * **dataset_name:** The name of the dataset. Most likely used for distinguishing similar datasets or searching for datasets
        * **dataset_frequency:** The frequency of the timesteps in a timeseries. Only applicable if the dataset has a type 'timeseries'
        * **dataset_hidden:** Indicates whether the dataset is hidden, in which case only authorised users can use the dataset.
        * **dataset_metadata:**: A dictionary of the metadata associated with the dataset. For example: {'created_by': "User 1", "source":"Import from CSV"}
        * **dataset_value:**
            Depending on what the dataset_type is, this can be a decimal value, a freeform
            string or a JSON string.
            For a timeseries for example, the datasset_value looks like:
            ::

             '{
                 "H1" : {\n
                         "2014/09/04 16:46:12:00":1,\n
                         "2014/09/05 16:46:12:00":2,\n
                         "2014/09/06 16:46:12:00":3,\n
                         "2014/09/07 16:46:12:00":4},\n

                 "H2" : {\n
                         "2014/09/04 16:46:12:00":10,\n
                         "2014/09/05 16:46:12:00":20,\n
                         "2014/09/06 16:46:12:00":30,\n
                         "2014/09/07 16:46:12:00":40},\n

                 "H3" :  {\n
                         "2014/09/04 16:46:12:00":100,\n
                         "2014/09/05 16:46:12:00":200,\n
                         "2014/09/06 16:46:12:00":300,\n
                         "2014/09/07 16:46:12:00":400}\n
             }'

    """
    _type_info = [
        ('attr_id',            Unicode(default=None)),
        ('scenario_id',        Unicode(default=None)),
        ('resource_attr_id',   Unicode(default=None)),
        ('ref_id',             Unicode(default=None)),
        ('ref_key',            Unicode(default=None)),
        ('attr_is_var',        Unicode(default=None)),
        ('dataset_id',         Unicode(default=None)),
        ('dataset_type',       Unicode(default=None)),
        ('dataset_dimension',  Unicode(default=None)),
        ('dataset_unit',       Unicode(default=None)),
        ('dataset_name',       Unicode(default=None)),
        ('dataset_value',      Unicode(default=None)),
        ('dataset_frequency',  Unicode(default=None)),
        ('dataset_hidden',     Unicode(default=None)),
        ('dataset_metadata',   Unicode(default=None)),
    ]

    def __init__(self, resourceattr=None):

        super(ResourceData, self).__init__()
        if  resourceattr is None:
            return

        ra = resourceattr

        self.attr_id = str(ra.attr_id)
        self.resource_attr_id = str(ra.resource_attr_id)
        self.ref_key = str(ra.ref_key)
        self.ref_id  = str(getattr(ra, ref_id_map[self.ref_key]))

        self.source = ra.source
        self.scenario_id = str(ra.scenario_id)

        self.dataset_hidden    = ra.hidden
        self.dataset_id        = str(ra.dataset_id)
        self.dataset_type      = ra.data_type
        self.dataset_name      = ra.data_name

        self.dataset_dimension = ra.data_dimen
        self.dataset_unit      = ra.data_units
        self.dataset_frequency = ra.frequency
        self.dataset_value     = ra.value

        self.metadata = {}
        for m in ra.metadata:
            self.metadata[m.metadata_name] = m.metadata_val

        self.dataset_metadata = json.dumps(self.metadata)

class Dataset(HydraComplexModel):
    """
    """
    _type_info = [
        ('id',               Integer(min_occurs=0, default=None)),
        ('type',             Unicode),
        ('dimension',        Unicode(min_occurs=1, default='dimensionless')),
        ('unit',             Unicode(min_occurs=1, default=None)),
        ('name',             Unicode(min_occurs=1, default=None)),
        ('value',            Unicode(min_occurs=1, default=None)),
        ('hidden',           Unicode(min_occurs=0, default='N', pattern="[YN]")),
        ('created_by',       Integer(min_occurs=0, default=None)),
        ('cr_date',          Unicode(min_occurs=0, default=None)),
        ('metadata',         Unicode(min_occurs=0, default='{}')),
    ]

    def __init__(self, parent=None, include_metadata=True):
        super(Dataset, self).__init__()
        if  parent is None:
            return

        self.hidden    = parent.hidden
        self.id        = parent.dataset_id
        self.type      = parent.data_type
        self.name      = parent.data_name
        self.created_by = parent.created_by
        self.cr_date    = str(parent.cr_date)

        self.dimension = parent.data_dimen
        self.unit      = parent.data_units
        self.value = None

        if parent.value is not None:
            try:
                self.value = zlib.decompress(parent.value)
            except:
                self.value = parent.value

        if include_metadata is True:
            metadata = {}
            for m in parent.metadata:
                metadata[m.metadata_name] = m.metadata_val
            self.metadata = json.dumps(metadata)

    def parse_value(self):
        """
            Turn the value of an incoming dataset into a hydra-friendly value.
        """
        try:
            #attr_data.value is a dictionary,
            #but the keys have namespaces which must be stripped.


            if self.value is None:
                log.warn("Cannot parse dataset. No value specified.")
                return None
            
            data = str(self.value)

            if len(data) > 100:
                log.debug("Parsing %s", data[0:100])
            else:
                log.debug("Parsing %s", data)

            if self.type == 'descriptor':
                return data 
            elif self.type == 'scalar':
                return data
            elif self.type == 'timeseries':
                timeseries_pd = pd.read_json(data)

                #Epoch doesn't work here because dates before 1970 are not
                # supported in read_json. Ridiculous.
                ts = timeseries_pd.to_json(date_format='iso', date_unit='ns')
                if len(data) > config.get('DATA', 'compression_threshold', 1000):
                    return zlib.compress(ts)
                else:
                    return ts
            elif self.type == 'array':
                #check to make sure this is valid json
                json.loads(data)
                if len(data) > config.get('DATA', 'compression_threshold', 1000):
                    return zlib.compress(data)
                else:
                    return data
        except Exception, e:
            log.exception(e)
            raise HydraError("Error parsing value %s: %s"%(self.value, e))

    def get_metadata_as_dict(self, user_id=None, source=None):

        if self.metadata is None:
            return {}

        metadata_dict = {}

        if type(self.metadata) == dict:
            metadata_dict = self.metadata
        else:
            metadata_dict = json.loads(self.metadata)

        #These should be set on all datasests by default, but we don't
        #want to enforce this rigidly
        metadata_keys = [m.lower() for m in metadata_dict]
        if user_id is not None and 'user_id' not in metadata_keys:
            metadata_dict['user_id'] = str(user_id)

        if source is not None and 'source' not in metadata_keys:
            metadata_dict['source'] = str(source)

        return metadata_dict

    def get_hash(self, val, metadata):

        if metadata is None:
            metadata = self.get_metadata()

        if val is None:
            value = self.parse_value()
        else:
            value = val

        dataset_dict = {'data_name' : self.name,
                    'data_units': self.unit,
                    'data_dimen': self.dimension,
                    'data_type' : self.type.lower(),
                    'value'     : value,
                    'metadata'  : metadata,}

        data_hash = generate_data_hash(dataset_dict)

        return data_hash

class DatasetCollection(HydraComplexModel):
    _type_info = [
        ('name', Unicode(default=None)),
        ('id'  , Integer(default=None)),
        ('dataset_ids',   SpyneArray(Integer)),
        ('cr_date',       Unicode(default=None)),
    ]

    def __init__(self, parent=None):
        super(DatasetCollection, self).__init__()
        if  parent is None:
            return
        self.name = parent.collection_name
        self.id   = parent.collection_id
        self.dataset_ids = [d.dataset_id for d in parent.items]
        self.cr_date = str(parent.cr_date)

class Attr(HydraComplexModel):
    """
    """
    _type_info = [
        ('id', Integer(default=None)),
        ('name', Unicode(default=None)),
        ('dimen', Unicode(default=None)),
        ('description', Unicode(default=None)),
        ('cr_date', Unicode(default=None)),
    ]

    def __init__(self, parent=None):
        super(Attr, self).__init__()
        if  parent is None:
            return
        self.id = parent.attr_id
        self.name = parent.attr_name
        self.dimen = parent.attr_dimen
        self.description = parent.attr_description
        self.cr_date = str(parent.cr_date)

class ResourceScenario(HydraComplexModel):
    """
    """
    _type_info = [
        ('resource_attr_id', Integer(default=None)),
        ('attr_id',          Integer(default=None)),
        ('value',            Dataset),
        ('source',           Unicode),
        ('cr_date',       Unicode(default=None)),
    ]

    def __init__(self, parent=None, attr_id=None):
        super(ResourceScenario, self).__init__()
        if parent is None:
            return
        self.resource_attr_id = parent.resource_attr_id
        self.attr_id          = attr_id if attr_id is not None else parent.resourceattr.attr_id

        self.value = Dataset(parent.dataset)
        self.source = parent.source
        self.cr_date = str(parent.cr_date)

class ResourceAttr(HydraComplexModel):
    """
    """
    _type_info = [
        ('id',      Integer(min_occurs=0, default=None)),
        ('attr_id', Integer(default=None)),
        ('ref_id',  Integer(min_occurs=0, default=None)),
        ('ref_key', Unicode(min_occurs=0, default=None)),
        ('attr_is_var', Unicode(min_occurs=0, default='N')),
        ('resourcescenario', ResourceScenario),
        ('cr_date', Unicode(default=None)),
    ]

    def __init__(self, parent=None):
        super(ResourceAttr, self).__init__()
        if  parent is None:
            return
        self.id = parent.resource_attr_id
        self.attr_id = parent.attr_id
        self.ref_key  = parent.ref_key
        self.cr_date = str(parent.cr_date)
        if parent.ref_key == 'NETWORK':
            self.ref_id = parent.network_id
        elif parent.ref_key  == 'NODE':
            self.ref_id = parent.node_id
        elif parent.ref_key == 'LINK':
            self.ref_id = parent.link_id
        elif parent.ref_key == 'GROUP':
            self.ref_id = parent.group_id

        self.attr_is_var = parent.attr_is_var
        #This should be set externally as it is not related to its parent.
        self.resourcescenario = None

class ResourceAttrMap(HydraComplexModel):
    _type_info = [
        ('resource_attr_id_a', Integer(default=None)),
        ('resource_attr_id_b', Integer(default=None)),
        ('attr_a_name'       , Unicode(default=None)),
        ('attr_b_name'       , Unicode(default=None)),
        ('ref_key_a'         , Unicode(default=None)),
        ('ref_key_b'         , Unicode(default=None)),
        ('ref_id_a'          , Integer(default=None)),
        ('ref_id_b'          , Integer(default=None)),
        ('resource_a_name'   , Unicode(default=None)),
        ('resource_b_name'   , Unicode(default=None)),
        ('network_a_id'      , Integer(default=None)),
        ('network_b_id'      , Integer(default=None)),
    ]

    def __init__(self, parent=None):
        super(ResourceAttrMap, self).__init__()
        if  parent is None:
            return

        self.resource_attr_id_a = parent.resource_attr_id_a
        self.resource_attr_id_b = parent.resource_attr_id_b

        self.ref_key_a = parent.resourceattr_a.ref_key
        self.ref_id_a  = parent.resourceattr_a.get_resource_id()
        self.attr_a_name = parent.resourceattr_a.attr.attr_name
        self.resource_a_name = parent.resourceattr_a.get_resource().get_name()

        self.ref_key_b = parent.resourceattr_b.ref_key
        self.ref_id_b  = parent.resourceattr_b.get_resource_id()
        self.attr_b_name = parent.resourceattr_b.attr.attr_name
        self.resource_b_name = parent.resourceattr_b.get_resource().get_name()

        self.network_a_id = parent.network_a_id
        self.network_b_id = parent.network_b_id


class ResourceTypeDef(HydraComplexModel):
    """
    """
    _type_info = [
        ('ref_key', Unicode(default=None)),
        ('ref_id',  Integer(default=None)),
        ('type_id', Integer(default=None)),
    ]

class TypeAttr(HydraComplexModel):
    """
    """
    _type_info = [
        ('attr_id',            Integer(min_occurs=1, max_occurs=1)),
        ('attr_name',          Unicode(default=None)),
        ('type_id',            Integer(default=None)),
        ('data_type',          Unicode(default=None)),
        ('dimension',          Unicode(default=None)),
        ('unit',               Unicode(default=None)),
        ('default_dataset_id', Integer(default=None)),
        ('data_restriction',   AnyDict(default=None)),
        ('is_var',             Unicode(default=None)),
        ('description',        Unicode(default=None)),
        ('cr_date',            Unicode(default=None)),
    ]

    def __init__(self, parent=None):
        super(TypeAttr, self).__init__()
        if  parent is None:
            return

        self.attr_id   = parent.attr_id
        attr = parent.get_attr()
        if attr is not None:
            self.attr_name = attr.attr_name
            self.dimension = attr.attr_dimen
        else:
            self.attr_name = None
            self.dimension = None

        self.type_id   = parent.type_id
        self.data_type = parent.data_type
        self.unit      = parent.unit
        self.default_dataset_id = self.default_dataset_id
        self.description = parent.description
        self.cr_date = str(parent.cr_date)
        if parent.data_restriction is not None:
            self.data_restriction = eval(parent.data_restriction)
            for k, v in self.data_restriction.items():
                self.data_restriction[k] = [v]
        else:
            self.data_restriction = {}
        self.is_var = parent.attr_is_var

class TemplateType(HydraComplexModel):
    """
    """
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('resource_type', Unicode(values=['GROUP', 'NODE', 'LINK', 'NETWORK'], default=None)),
        ('alias',       Unicode(default=None)),
        ('layout',      AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('template_id', Integer(min_occurs=1, default=None)),
        ('typeattrs',   SpyneArray(TypeAttr)),
        ('cr_date',     Unicode(default=None)),
    ]

    def __init__(self, parent=None):
        super(TemplateType, self).__init__()
        if parent is None:
            return

        self.id        = parent.type_id
        self.name      = parent.type_name
        self.alias     = parent.alias
        self.resource_type = parent.resource_type
        self.cr_date = str(parent.cr_date)
        self.layout = self.get_outgoing_layout(parent.layout)
        self.template_id  = parent.template_id

        typeattrs = []
        for typeattr in parent.typeattrs:
            typeattrs.append(TypeAttr(typeattr))

        self.typeattrs = typeattrs

class Template(HydraComplexModel):
    """
    """
    _type_info = [
        ('id',        Integer(default=None)),
        ('name',      Unicode(default=None)),
        ('layout',    AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('types',     SpyneArray(TemplateType)),
        ('cr_date',   Unicode(default=None)),
    ]

    def __init__(self, parent=None):
        super(Template, self).__init__()
        if parent is None:
            return

        self.name   = parent.template_name
        self.id     = parent.template_id
        self.cr_date = str(parent.cr_date)
        self.layout = self.get_outgoing_layout(parent.layout)

        types = []
        for templatetype in parent.templatetypes:
            types.append(TemplateType(templatetype))
        self.types = types

class TypeSummary(HydraComplexModel):
    """
    """
    _type_info = [
        ('name',    Unicode),
        ('id',      Integer),
        ('template_name', Unicode),
        ('template_id', Integer),
    ]

    def __init__(self, parent=None):
        super(TypeSummary, self).__init__()

        if parent is None:
            return

        self.name          = parent.type_name
        self.id            = parent.type_id
        self.template_name = parent.template.template_name
        self.template_id   = parent.template_id

class Resource(HydraComplexModel):
    """
    """

    def get_layout(self):
        if hasattr(self, 'layout') and self.layout is not None:
            return str(self.layout).replace('{%s}'%NS, '')
        else:
            return None


class ResourceSummary(HydraComplexModel):
    """
    """
    _type_info = [
        ('ref_key', Unicode(default=None)),
        ('id',  Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(min_occurs=1, default="")),
        ('attributes',  SpyneArray(ResourceAttr)),
        ('types',       SpyneArray(TypeSummary)),
    ]

    def __init__(self, parent=None):
        super(ResourceSummary, self).__init__()

        if parent is None:
            parent
        if hasattr(parent, 'node_id'):
            self.ref_key = 'NODE'
            self.id   = parent.node_id
            self.name = parent.node_name
            self.description = parent.node_description
        elif hasattr(parent, 'link_id'):
            self.ref_key = 'LINK'
            self.id   = parent.link_id
            self.name = parent.link_name
            self.description = parent.link_description
        elif hasattr(parent, 'group_id'):
            self.ref_key = 'GROUP'
            self.id   = parent.group_id
            self.name = parent.group_name
            self.description = parent.group_description

        self.attributes = [ResourceAttr(ra) for ra in parent.attributes]
        self.types = [TypeSummary(t.templatetype) for t in parent.types]

class Node(Resource):
    """
    """
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(min_occurs=1, default="")),
        ('layout',      AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('x',           Decimal(min_occurs=1, default=0)),
        ('y',           Decimal(min_occurs=1, default=0)),
        ('status',      Unicode(default='A', pattern="[AX]")),
        ('attributes',  SpyneArray(ResourceAttr)),
        ('types',       SpyneArray(TypeSummary)),
        ('cr_date',     Unicode(default=None)),
    ]

    def __init__(self, parent=None, summary=False):
        super(Node, self).__init__()

        if parent is None:
            return


        self.id = parent.node_id
        self.name = parent.node_name
        self.x = parent.node_x
        self.y = parent.node_y
        self.description = parent.node_description
        self.cr_date = str(parent.cr_date)
        self.layout = self.get_outgoing_layout(parent.layout)
        self.status = parent.status
        if summary is False:
            self.attributes = [ResourceAttr(a) for a in parent.attributes]
        self.types = [TypeSummary(t.templatetype) for t in parent.types]



class Link(Resource):
    """
    """
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(min_occurs=1, default="")),
        ('layout',      AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('node_1_id',   Integer(default=None)),
        ('node_2_id',   Integer(default=None)),
        ('status',      Unicode(default='A', pattern="[AX]")),
        ('attributes',  SpyneArray(ResourceAttr)),
        ('types',       SpyneArray(TypeSummary)),
        ('cr_date',     Unicode(default=None)),
    ]

    def __init__(self, parent=None, summary=False):
        super(Link, self).__init__()

        if parent is None:
            return


        self.id = parent.link_id
        self.name = parent.link_name
        self.node_1_id = parent.node_1_id
        self.node_2_id = parent.node_2_id
        self.description = parent.link_description
        self.cr_date = str(parent.cr_date)
        self.layout = self.get_outgoing_layout(parent.layout)
        self.status    = parent.status
        if summary is False:


            self.attributes = [ResourceAttr(a) for a in parent.attributes]
        self.types = [TypeSummary(t.templatetype) for t in parent.types]


class AttributeData(HydraComplexModel):
    """
        A class which is returned by the server when a request is made
        for the data associated with an attribute.
    """
    _type_info = [
        ('resourceattrs', SpyneArray(ResourceAttr)),
        ('resourcescenarios', SpyneArray(ResourceScenario)),
    ]

class ResourceGroupItem(HydraComplexModel):
    """
    """
    _type_info = [
        ('id',       Integer(default=None)),
        ('ref_id',   Integer(default=None)),
        ('ref_key',  Unicode(default=None)),
        ('group_id', Integer(default=None)),
        ('cr_date',     Unicode(default=None)),
    ]

    def __init__(self, parent=None):
        super(ResourceGroupItem, self).__init__()
        if parent is None:
            return
        self.id       = parent.item_id
        self.group_id = parent.group_id
        self.ref_key  = parent.ref_key
        self.cr_date  = str(parent.cr_date)
        if self.ref_key == 'NODE':
            self.ref_id = parent.node_id
        elif self.ref_key == 'LINK':
            self.ref_id = parent.link_id
        elif self.ref_key == 'GROUP':
            self.ref_id = parent.subgroup_id

class ResourceGroup(HydraComplexModel):
    """
    """
    _type_info = [
        ('id',          Integer(default=None)),
        ('network_id',  Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(min_occurs=1, default="")),
        ('status',      Unicode(default='A', pattern="[AX]")),
        ('attributes',  SpyneArray(ResourceAttr)),
        ('types',       SpyneArray(TypeSummary)),
        ('cr_date',     Unicode(default=None)),
    ]

    def __init__(self, parent=None, summary=False):
        super(ResourceGroup, self).__init__()

        if parent is None:
            return

        self.name        = parent.group_name
        self.id          = parent.group_id
        self.description = parent.group_description
        self.status      = parent.status
        self.network_id  = parent.network_id
        self.cr_date     = str(parent.cr_date)

        self.types       = [TypeSummary(t.templatetype) for t in parent.types]

        if summary is False:
            self.attributes  = [ResourceAttr(a) for a in parent.attributes]

class Scenario(Resource):
    """
    """
    _type_info = [
        ('id',                   Integer(default=None)),
        ('name',                 Unicode(default=None)),
        ('description',          Unicode(min_occurs=1, default="")),
        ('network_id',           Integer(default=None)),
        ('layout',               AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('status',               Unicode(default='A', pattern="[AX]")),
        ('locked',               Unicode(default='N', pattern="[YN]")),
        ('start_time',           Unicode(default=None)),
        ('end_time',             Unicode(default=None)),
        ('created_by',           Integer(default=None)),
        ('cr_date',              Unicode(default=None)),
        ('time_step',            Unicode(default=None)),
        ('resourcescenarios',    SpyneArray(ResourceScenario, default=None)),
        ('resourcegroupitems',   SpyneArray(ResourceGroupItem, default=None)),
    ]

    def __init__(self, parent=None, summary=False):
        super(Scenario, self).__init__()

        if parent is None:
            return
        self.id = parent.scenario_id
        self.name = parent.scenario_name
        self.description = parent.scenario_description
        self.layout = self.get_outgoing_layout(parent.layout)
        self.network_id = parent.network_id
        self.status = parent.status
        self.locked = parent.locked
        self.start_time = get_timestamp(parent.start_time)
        self.end_time = get_timestamp(parent.end_time)
        self.time_step = parent.time_step
        self.created_by = parent.created_by
        self.cr_date    = str(parent.cr_date)
        if summary is False:
            self.resourcescenarios = [ResourceScenario(rs) for rs in parent.resourcescenarios]
            self.resourcegroupitems = [ResourceGroupItem(rgi) for rgi in parent.resourcegroupitems]
        else:
            self.resourcescenarios = []
            self.resourcegroupitems = []

class Rule(HydraComplexModel):
    """
    """
    _type_info = [
        ('id', Integer),
        ('name', Unicode),
        ('description', Unicode),
        ('scenario_id', Integer),
        ('ref_key', Unicode),
        ('ref_id', Integer),
        ('text', Unicode),
        ('cr_date', Unicode(default=None)),
    ]

    def __init__(self, parent=None):
        super(Rule, self).__init__()
        if parent is None:
            return

        self.id = parent.rule_id
        self.name = parent.rule_name
        self.description = parent.rule_description
        self.ref_key = parent.ref_key
        if self.ref_key == 'NETWORK':
            self.ref_id = parent.network_id
        elif self.ref_key == 'NODE':
            self.ref_id = parent.node_id
        elif self.ref_key == 'LINK':
            self.ref_id = parent.link_id
        elif self.ref_key == 'GROUP':
            self.ref_id = parent.group_id

        self.scenario_id = parent.scenario_id
        self.text        = parent.rule_text
        self.cr_date    = str(parent.cr_date)

class Note(HydraComplexModel):
    """
    """
    _type_info = [
        ('id', Integer),
        ('ref_key', Unicode),
        ('ref_id', Integer),
        ('text', Unicode),
        ('created_by', Integer),
        ('cr_date', Unicode),
    ]

    def __init__(self, parent=None):
        super(Note, self).__init__()
        if parent is None:
            return

        self.id = parent.note_id
        self.ref_key = parent.ref_key
        if self.ref_key == 'NETWORK':
            self.ref_id = parent.network_id
        elif self.ref_key == 'NODE':
            self.ref_id = parent.node_id
        elif self.ref_key == 'LINK':
            self.ref_id = parent.link_id
        elif self.ref_key == 'GROUP':
            self.ref_id = parent.group_id
        elif self.ref_key == 'SCENARIO':
            self.ref_id = parent.scenario_id
        elif self.ref_key == 'PROJECT':
            self.ref_id = parent.project_id

        self.text        = parent.note_text
        self.created_by  = parent.created_by
        self.cr_date     = str(parent.cr_date)


class ResourceGroupDiff(HydraComplexModel):
    """
    """
    _type_info = [
       ('scenario_1_items', SpyneArray(ResourceGroupItem)),
       ('scenario_2_items', SpyneArray(ResourceGroupItem))
    ]

    def __init__(self, parent=None):
        super(ResourceGroupDiff, self).__init__()

        if parent is None:
            return

        self.scenario_1_items = [ResourceGroupItem(rs) for rs in parent['scenario_1_items']]
        self.scenario_2_items = [ResourceGroupItem(rs) for rs in parent['scenario_2_items']]

class ResourceScenarioDiff(HydraComplexModel):
    """
    """
    _type_info = [
        ('resource_attr_id',     Integer(default=None)),
        ('scenario_1_dataset',   Dataset),
        ('scenario_2_dataset',   Dataset),
    ]

    def __init__(self, parent=None):
        super(ResourceScenarioDiff, self).__init__()

        if parent is None:
            return

        self.resource_attr_id   = parent['resource_attr_id']

        self.scenario_1_dataset = Dataset(parent['scenario_1_dataset'])
        self.scenario_2_dataset = Dataset(parent['scenario_2_dataset'])

class ScenarioDiff(HydraComplexModel):
    """
    """
    _type_info = [
        ('resourcescenarios',    SpyneArray(ResourceScenarioDiff)),
        ('groups',               ResourceGroupDiff),
    ]

    def __init__(self, parent=None):
        super(ScenarioDiff, self).__init__()

        if parent is None:
            return

        self.resourcescenarios = [ResourceScenarioDiff(rd) for rd in parent['resourcescenarios']]
        self.groups = ResourceGroupDiff(parent['groups'])

class Network(Resource):
    """
    """
    _type_info = [
        ('project_id',          Integer(default=None)),
        ('id',                  Integer(default=None)),
        ('name',                Unicode(default=None)),
        ('description',         Unicode(min_occurs=1, default=None)),
        ('created_by',          Integer(default=None)),
        ('cr_date',             Unicode(default=None)),
        ('layout',              AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('status',              Unicode(default='A', pattern="[AX]")),
        ('attributes',          SpyneArray(ResourceAttr)),
        ('scenarios',           SpyneArray(Scenario)),
        ('nodes',               SpyneArray(Node)),
        ('links',               SpyneArray(Link)),
        ('resourcegroups',      SpyneArray(ResourceGroup)),
        ('types',               SpyneArray(TypeSummary)),
        ('projection',          Unicode(default=None)),
    ]

    def __init__(self, parent=None, summary=False):
        super(Network, self).__init__()

        if parent is None:
            return
        self.project_id = parent.project_id
        self.id         = parent.network_id
        self.name       = parent.network_name
        self.description = parent.network_description
        self.created_by  = parent.created_by
        self.cr_date     = str(parent.cr_date)
        self.layout = self.get_outgoing_layout(parent.layout)
        self.status      = parent.status
        self.scenarios   = [Scenario(s, summary) for s in parent.scenarios]
        self.nodes       = [Node(n, summary) for n in parent.nodes]
        self.links       = [Link(l, summary) for l in parent.links]
        self.resourcegroups = [ResourceGroup(rg, summary) for rg in parent.resourcegroups]
        self.types          = [TypeSummary(t.templatetype) for t in parent.types]
        self.projection  = parent.projection

        if summary is False:
            self.attributes  = [ResourceAttr(ra) for ra in parent.attributes]

class NetworkExtents(HydraComplexModel):
    """
    """
    _type_info = [
        ('network_id', Integer(default=None)),
        ('min_x',      Decimal(default=0)),
        ('min_y',      Decimal(default=0)),
        ('max_x',      Decimal(default=0)),
        ('max_y',      Decimal(default=0)),
    ]

    def __init__(self, parent=None):
        super(NetworkExtents, self).__init__()

        if parent is None:
            return

        self.network_id = parent.network_id
        self.min_x = parent.min_x
        self.min_y = parent.min_y
        self.max_x = parent.max_x
        self.max_y = parent.max_y

class Project(Resource):
    """
    """
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(default=None)),
        ('status',      Unicode(default='A', pattern="[AX]")),
        ('cr_date',     Unicode(default=None)),
        ('created_by',  Integer(default=None)),
        ('attributes',  SpyneArray(ResourceAttr)),
        ('attribute_data', SpyneArray(ResourceScenario)),
    ]

    def __init__(self, parent=None):
        super(Project, self).__init__()

        if parent is None:
            return

        self.id = parent.project_id
        self.name = parent.project_name
        self.description = parent.project_description
        self.status      = parent.status
        self.cr_date     = str(parent.cr_date)
        self.created_by  = parent.created_by
        self.attributes  = [ResourceAttr(ra) for ra in parent.attributes]
        self.attribute_data  = [ResourceScenario(rs) for rs in parent.attribute_data]

class ProjectSummary(Resource):
    """
    """
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(default=None)),
        ('status',      Unicode(default=None)),
        ('cr_date',     Unicode(default=None)),
        ('created_by',  Integer(default=None)),
    ]

    def __init__(self, parent=None):
        super(ProjectSummary, self).__init__()

        if parent is None:
            return
        self.id = parent.project_id
        self.name = parent.project_name
        self.description = parent.project_description
        self.cr_date = str(parent.cr_date)
        self.created_by = parent.created_by
        self.summary    = parent.summary

class User(HydraComplexModel):
    """
    """
    _type_info = [
        ('id',  Integer),
        ('username', Unicode(default=None)),
        ('display_name', Unicode(default=None)),
        ('password', Unicode(default=None)),
    ]

    def __init__(self, parent=None):
        super(User, self).__init__()

        if parent is None:
            return

        self.id = parent.user_id
        self.username = parent.username
        self.display_name = parent.display_name
        self.password     = parent.password

class Perm(HydraComplexModel):
    """
    """
    _type_info = [
        ('id',   Integer),
        ('name', Unicode),
        ('code', Unicode),
    ]

    def __init__(self, parent=None):
        super(Perm, self).__init__()

        if parent is None:
            return

        self.id   = parent.perm_id
        self.name = parent.perm_name
        self.code = parent.perm_code

class RoleUser(HydraComplexModel):
    """
    """
    _type_info = [
        ('user_id',  Integer),
    ]
    def __init__(self, parent=None):
        super(RoleUser, self).__init__()

        if parent is None:
            return

        self.user_id = parent.user.user_id

class RolePerm(HydraComplexModel):
    """
    """
    _type_info = [
        ('perm_id',   Integer),
    ]

    def __init__(self, parent=None):
        super(RolePerm, self).__init__()

        if parent is None:
            return

        self.perm_id = parent.perm_id

class Role(HydraComplexModel):
    """
    """
    _type_info = [
        ('id',     Integer),
        ('name',   Unicode),
        ('code',   Unicode),
        ('roleperms', SpyneArray(RolePerm)),
        ('roleusers', SpyneArray(RoleUser)),
    ]

    def __init__(self, parent=None):
        super(Role, self).__init__()

        if parent is None:
            return

        self.id = parent.role_id
        self.name = parent.role_name
        self.code = parent.role_code
        self.roleperms = [RolePerm(rp) for rp in parent.roleperms]
        self.roleusers = [RoleUser(ru) for ru in parent.roleusers]

class PluginParam(HydraComplexModel):
    """
    """
    _type_info = [
        ('name',        Unicode),
        ('value',       Unicode),
    ]

    def __init__(self, parent=None):
        super(PluginParam, self).__init__()

        if parent is None:
            return

        self.name = parent.name
        self.value = parent.value


class Plugin(HydraComplexModel):
    """
    """
    _type_info = [
        ('name',        Unicode),
        ('location',    Unicode),
        ('params',      SpyneArray(PluginParam)),
    ]

    def __init__(self, parent=None):
        super(Plugin, self).__init__()

        if parent is None:
            return

        self.name = parent.name
        self.location = parent.location
        self.params = [PluginParam(pp) for pp in parent.params]


class ProjectOwner(HydraComplexModel):
    """
    """
    _type_info = [
        ('project_id',   Integer),
        ('user_id',  Integer),
        ('edit',     Unicode),
        ('view',     Unicode)
    ]
    def __init__(self, parent=None):
        super(ProjectOwner, self).__init__()

        if parent is None:
            return
        self.project_id = parent.project_id
        self.user_id    = parent.user_id
        self.edit       = parent.edit
        self.view       = parent.view

class DatasetOwner(HydraComplexModel):
    """
    """
    _type_info = [
        ('dataset_id',   Integer),
        ('user_id',  Integer),
        ('edit',     Unicode),
        ('view',     Unicode)
    ]
    def __init__(self, parent=None):
        super(DatasetOwner, self).__init__()

        if parent is None:
            return
        self.dataset_id = parent.dataset_id
        self.user_id    = parent.user_id
        self.edit       = parent.edit
        self.view       = parent.view

class NetworkOwner(HydraComplexModel):
    """
    """
    _type_info = [
        ('network_id',   Integer),
        ('user_id',  Integer),
        ('edit',     Unicode),
        ('view',     Unicode)
    ]
    def __init__(self, parent=None):
        super(NetworkOwner, self).__init__()

        if parent is None:
            return
        self.network_id = parent.network_id
        self.user_id    = parent.user_id
        self.edit       = parent.edit
        self.view       = parent.view


class Unit(HydraComplexModel):
    """
    """
    _type_info = [
        ('name', Unicode),
        ('abbr', Unicode),
        ('cf', Double),
        ('lf', Double),
        ('info', Unicode),
        ('dimension', Unicode),
    ]

    def __init__(self, parent=None):
        super(Unit, self).__init__()

        if parent is None:
            return
        self.name = parent.name
        self.abbr = parent.abbr
        self.cf   = parent.cf
        self.lf   = parent.lf
        self.info = parent.info
        self.dimension = parent.dimension

class Dimension(HydraComplexModel):
    """
        A dimension, with name and units
    """
    _type_info = [
        ('name', Unicode),
        ('units', SpyneArray(Unicode)),
    ]

    def __init__(self, name, units):

        self.name = name
        self.units = units

