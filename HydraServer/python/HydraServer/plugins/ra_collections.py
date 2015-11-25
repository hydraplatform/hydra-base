
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

from sqlalchemy import Column,\
ForeignKey,\
text,\
Integer as SAInteger,\
Numeric,\
String,\
LargeBinary,\
BigInteger,\
DateTime,\
TIMESTAMP,\
BIGINT,\
Float,\
UniqueConstraint,\
and_

from sqlalchemy.orm import relationship, backref

from HydraLib.HydraException import HydraError

from spyne.decorator import rpc
from spyne.model.primitive import Unicode
from spyne.model.primitive import Integer as SpyneInteger
from spyne.model.complex import Array as SpyneArray
from HydraServer.soap_server.hydra_base import HydraService
import logging
log = logging.getLogger(__name__)


from HydraServer.db import DeclarativeBase as Base, DBSession
from HydraServer.soap_server.hydra_complexmodels import HydraComplexModel

class ResourceAttrCollection(Base):
    """
    """

    __tablename__='tResourceAttrCollection'

    collection_id    = Column(SAInteger(), primary_key=True, nullable=False)
    collection_name  = Column(SAInteger(), nullable=False)
    layout           = Column(LargeBinary(), nullable=True)
    cr_date          = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))


class ResourceAttrCollectionItem(Base):
    """
    """

    __tablename__='tResourceAttrCollectionItem'

    collection_id    = Column(SAInteger(), ForeignKey('tResourceAttrCollection.collection_id'), primary_key=True, nullable=False)
    resource_attr_id = Column(SAInteger(), ForeignKey('tResourceAttr.resource_attr_id'), primary_key=True, nullable=False)

    collection = relationship('ResourceAttrCollection', backref=backref('items', uselist=True, cascade="all, delete-orphan"), lazy='joined')
    resourceattr = relationship('ResourceAttr')

class HydraResourceAttrCollection(HydraComplexModel):
    __namespace__ = 'soap_server.hydra_complexmodels'
    _type_info = [
        ('id',     SpyneInteger(default=None)),
        ('name',   Unicode(min_occurs=1)),
        ('layout', Unicode),
        ('resource_attr_ids',  SpyneArray(SpyneInteger)),
    ]

    def __init__(self, parent=None):
        super(HydraResourceAttrCollection, self).__init__()
        if  parent is None:
            return
        self.id = parent.collection_id
        self.name = parent.collection_name
        self.layout = parent.layout
        self.resource_attr_ids = [i.resource_attr_id for i in parent.items]


from HydraServer.db import engine
Base.metadata.create_all(engine)

class Service(HydraService):
    """
        An example of a server-side plug-in
    """
    
    __service_name__ = "SampleService"
    
    
    @rpc(HydraResourceAttrCollection, _returns=HydraResourceAttrCollection)
    def add_resource_attr_collection(ctx, ra_collection):
        """
            Add a new resource attribute collection
        """
        ra_i = ResourceAttrCollection()
        ra_i.collection_name = ra_collection.name
        ra_i.layout          = ra_collection.layout

        for ra_id in ra_collection.resource_attr_ids:
            item_i = ResourceAttrCollectionItem()
            item_i.resource_attr_id = ra_id
            ra_i.items.append(item_i)

        DBSession.add(ra_i)
        DBSession.flush()

        return HydraResourceAttrCollection(ra_i)

    @rpc(SpyneInteger, SpyneArray(SpyneInteger), _returns=HydraResourceAttrCollection)
    def add_items_to_attr_collection(ctx, collection_id, resource_attr_ids):
        """
            Add new items to a resource attribute collection
        """
        collection_i = DBSession.query(ResourceAttrCollection).filter(ResourceAttrCollection.collection_id==collection_id).first()

        if collection_i is None:
            raise HydraError("No collection with ID %s", collection_id)

        for ra_id in resource_attr_ids:
            item_i = ResourceAttrCollectionItem()
            item_i.resource_attr_id = ra_id
            collection_i.items.append(item_i)

        DBSession.add(collection_i)
        DBSession.flush()

        return HydraResourceAttrCollection(collection_i)

    @rpc(SpyneInteger, SpyneArray(SpyneInteger), _returns=Unicode)
    def remove_items_from_attr_collection(ctx, collection_id, resource_attr_ids):
        """
            Add new items to a resource attribute collection
        """
        collection_i = DBSession.query(ResourceAttrCollection).filter(ResourceAttrCollection.collection_id==collection_id).first()

        if collection_i is None:
            raise HydraError("No collection with ID %s", collection_id)

        for item in collection_i.items:
            if item.resource_attr_id in resource_attr_ids:
                DBSession.delete(item)

        DBSession.flush()
        
        return 'OK'


    @rpc(SpyneInteger, _returns=Unicode)
    def delete_resource_attr_collection(ctx, collection_id):
        """
            Delete a resource attribute collection
        """
        collection_i = DBSession.query(ResourceAttrCollection).filter(ResourceAttrCollection.collection_id==collection_id).first()

        if collection_i is None:
            raise HydraError("No collection with ID %s", collection_id)

        DBSession.delete(collection_i)

        return 'OK'

    @rpc(SpyneInteger, _returns=HydraResourceAttrCollection)
    def get_resource_attr_collection(ctx, collection_id):
        """
            Delete a resource attribute collection
        """
        collection_i = DBSession.query(ResourceAttrCollection).filter(ResourceAttrCollection.collection_id==collection_id).first()

        if collection_i is None:
            raise HydraError("No collection with ID %s", collection_id)

        return HydraResourceAttrCollection(collection_i) 
