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
from spyne.model.primitive import Integer, Integer32, Boolean, Unicode, AnyDict, Decimal
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Dataset,\
        DatasetCollection

from HydraServer.lib import data

import json

from hydra_base import HydraService

class DataService(HydraService):

    """
        The data SOAP service
    """

    @rpc(Dataset, _returns=Dataset)
    def add_dataset(ctx, dataset):
        """
           Add a single dataset. Return the new dataset with a dataset ID.
                .. code-block:: python

                    (Dataset){
                        value     = 123,
                        unit      = 'm^3', 
                        dimension = 'Volume', 
                        name      = 'Storage Capacity',
                        type      = 'scalar', #(others are 'descriptor', 'array' and 'timeseries')
                        metadata  = "{'measured_by':'John Doe'}", #Json encoded python dictionary
                    }

           Args:
               dataset (Dataset): The dataset complex model (see above)

           Returns:
               Dataset: The new dataset object, complete with ID

        """
        value = dataset.parse_value()
        metadata = dataset.get_metadata_as_dict(user_id=ctx.in_header.user_id)
        dataset_i = data.add_dataset(dataset.type,
                                     value,
                                     dataset.unit,
                                     dataset.dimension,
                                     metadata,
                                     dataset.name,
                                     ctx.in_header.user_id,
                                    flush=True)
        
        return Dataset(dataset_i)

    @rpc(SpyneArray(Integer32), _returns=SpyneArray(Dataset))
    def get_datasets(ctx, dataset_ids):
        """
        Get a list of datasets, by ID
        
        Args:
            dataset_ids (List(int)): A list of dataset IDs

        Returns:
            List(Dataset): The corresponding list of datasets. A subset will be returned if not all datasets are available.

        Raises:
            ResourceNotFoundError: If none of the requested datasets were found.
        """
        datasets = data.get_datasets(dataset_ids, **ctx.in_header.__dict__)
        ret_datasets = [Dataset(d) for d in datasets]
        return ret_datasets

    @rpc(Integer, _returns=Dataset)
    def get_dataset(ctx, dataset_id):
        """
        Get a single dataset, by ID

        Args:
            dataset_id (int): THe ID of the requested dataset

        Returns:
            Dataset: The dataset complex model

        Raises:
            ResourceNotFoundError: If the dataset does not exist.
        """

        dataset_i = data.get_dataset(dataset_id, **ctx.in_header.__dict__)
        
        return Dataset(dataset_i)

    @rpc(Integer, _returns=Dataset)
    def clone_dataset(ctx, dataset_id):
        """
        Clone a single dataset, by ID
    
        Args:
            dataset_id (int): THe ID of the dataset to be cloned

        Returns:
            Dataset: The newly cloned dataset complex model

        Raises:
            ResourceNotFoundError: If the dataset does not exist.

        """

        dataset_i = data.clone_dataset(dataset_id, **ctx.in_header.__dict__)

        return Dataset(dataset_i)


    @rpc(Integer, Unicode, Unicode, Unicode, Unicode, Unicode,
         Integer, Unicode, Unicode,
         Integer, Integer, Unicode,
         Unicode(pattern='[YN]', default='N'), #include metadata flag
         Unicode(pattern='[YN]', default='N'), # include value flag
         Integer(default=0),Integer(default=2000), #start, size page flags
         _returns=SpyneArray(Dataset))
    def search_datasets(ctx, dataset_id,
                name,
                collection_name,
                data_type,
                dimension,
                unit,
                scenario_id,
                metadata_name,
                metadata_val,
                attr_id,
                type_id,
                unconnected,
                inc_metadata,
                inc_val,
                page_start,
                page_size):
        """
        Search for datadets that satisfy the criteria specified.
        By default, returns a max of 2000 datasets. To return datasets from 2001 onwards,
        set page_start to 2001. 
    
        Args:
            dataset_id      (int)    : The ID of the dataset
            name            (string) : The name of the dataset
            collection_name (string) : Search for datsets in a collection with this name
            data_type       (string) : 'scalar', 'descriptor', 'array', 'timeseries'
            dimension       (string) : Datasets with this dimension
            unit            (string) : Datasets with this unit.
            scenario_id     (int)    : Datasets in this scenraio
            metadata_name   (string) : Datasets that have this metadata
            metadata_val    (string) : Datasets that have this metadata value
            attr_id         (int)    : Datasts that are associated with this attribute via resource scenario & resource attribute
            type_id         (int)    : Datasets that are associated with this type via resource scenario -> resource attribute -> attribute -> type
            unconnected     (char)   : Datasets that are not in any scenarios
            inc_metadata    (char) (default 'N')   : Return metadata with retrieved datasets. 'Y' gives a performance hit.
            inc_val         (char) (default 'N')  : Include the value with the dataset. 'Y' gives a performance hit
            page_start      (int)    : Return datasets from this point (ex: from index 2001 of 10,000)
            page_size       (int)    : Return this number of datasets in one go. default is 2000.

        Returns:
            List(Dataset): The datasets matching all the specified criteria.

        """
        datasets = data.search_datasets(dataset_id,
                                     name,
                                     collection_name,
                                     data_type,
                                     dimension,
                                     unit,
                                     scenario_id,
                                     metadata_name,
                                     metadata_val,
                                     attr_id,
                                     type_id,
                                     unconnected,
                                     inc_metadata,
                                     inc_val,
                                     page_start,
                                     page_size,
                                     **ctx.in_header.__dict__)

        cm_datasets = []
        for d in datasets:
            cm_datasets.append(Dataset(d))

        return cm_datasets

    @rpc(Integer(max_occurs="unbounded"), _returns=Unicode)
    def get_metadata(ctx, dataset_ids):
        """
        Get the metadata for a dataset or list of datasets

        Args:
            dataset_ids (List(int)): The list of dataset IDS that you want metadata for

        Returns:
            (string): A dictionary keyed on metadata name, dumped as a json string.
        """

        if type(dataset_ids) == int:
            dataset_ids = [dataset_ids]
        
        metadata = data.get_metadata(dataset_ids)
        metadata_dict = {}
        for m in metadata:
            metadata_dict[m.metadata_name] = m.metadata_val

        return json.dumps(metadata_dict)

    @rpc(SpyneArray(Dataset), _returns=SpyneArray(Integer))
    def bulk_insert_data(ctx, bulk_data):
        """
            Insert sereral pieces of data at once.

            Args:
                bulk_data (List(Dataset)): A list of Dataset complex models

            Returns:
                List(int): A list of new dataset IDS
        """
        datasets = data.bulk_insert_data(bulk_data, **ctx.in_header.__dict__)

        return [d.dataset_id for d in datasets]

    @rpc(_returns=SpyneArray(DatasetCollection))
    def get_all_dataset_collections(ctx):
        """
        Get all the dataset collections available.

        Args:
            None

        Returns:
            List(DatasetCollection): A list of dataset collection objects, each containing references to all the datasets inside them.

        """
        dataset_colns = data.get_all_dataset_collections(**ctx.in_header.__dict__)
        all_colns = []
        for d_g in dataset_colns:
            all_colns.append(DatasetCollection(d_g))
        return all_colns


    @rpc(Integer, Integer, _returns=Unicode)
    def add_dataset_to_collection(ctx, dataset_id, collection_id):
        """
        Add a single dataset to a dataset collection.

        Args:
            dataset_id (int): The dataset to add to the collection
            collection_id (int): The collection to receive the new dataset

        Returns:
            string: 'OK'

        Raises:
            ResourceNotFoundError: If the dataset or collection do not exist
        """

        data.add_dataset_to_collection(dataset_id,
                                       collection_id,
                                       **ctx.in_header.__dict__)
        return 'OK'

    @rpc(SpyneArray(Integer32), Integer, _returns=Unicode)
    def add_datasets_to_collection(ctx, dataset_ids, collection_id):
        """
        Add multiple datasets to a dataset collection.

        Args:
            dataset_ids (Lsit(int)): The IDs of the datasets to add to the collection
            collection_id (int): The collection to receive the new dataset

        Returns:
            string: 'OK'

        Raises:
            ResourceNotFoundError: If the collection does not exist
        """
        data.add_datasets_to_collection(dataset_ids,
                                        collection_id,
                                        **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, Integer, _returns=Unicode)
    def remove_dataset_from_collection(ctx, dataset_id, collection_id):
        """
        Remove a single dataset to a dataset collection.

        Args:
            dataset_id (int): The dataset to remove from the collection
            collection_id (int): The collection to lose the dataset

        Returns:
            string: 'OK'

        Raises:
            ResourceNotFoundError: If the dataset or collection do not exist

        """
        data.remove_dataset_from_collection(dataset_id,
                                             collection_id,
                                             **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, Integer, _returns=Unicode(pattern='[YN]'))
    def check_dataset_in_collection(ctx, dataset_id, collection_id):
        """
        Check whether a dataset is contained inside a collection
   
        Args:
            dataset_id (int): The dataset being checked
            collection_id (int): The collection to check in

        Returns:
            char: 'Y' or 'N'

        Raises:
            ResourceNotFoundError: If the collection does not exist
        """
        
        result = data.check_dataset_in_collection(dataset_id,
                                         collection_id,
                                         **ctx.in_header.__dict__)
        return result

    @rpc(Integer, _returns=DatasetCollection)
    def get_dataset_collection(ctx, collection_id):
        """
        Get a single dataset collection, by ID.
        
        Args:
            collection_id (int): The collection to retrieve

        Returns:
            DatasetCollection: A dataset collection complex model

        Raises:
            ResourceNotFoundError: If the collection does not exist

        """

        dataset_coln_i = data.get_dataset_collection(collection_id,
                                                     **ctx.in_header.__dict__)
        return DatasetCollection(dataset_coln_i)

    @rpc(Integer, _returns=Unicode)
    def delete_dataset_collection(ctx, collection_id):
        """
        Delete a single dataset collection, by ID.
   
        Args:
            collection_id (int): The collection to delete

        Returns:
            string: 'OK' 

        Raises:
            ResourceNotFoundError: If the collection does not exist


        """

        data.delete_dataset_collection(collection_id,
                                                     **ctx.in_header.__dict__)
        return "OK"

    @rpc(Unicode, _returns=DatasetCollection)
    def get_dataset_collection_by_name(ctx, collection_name):
        """
        Get all the dataset collections with the provided name.

        Args:
            collection_name (string): The name of the collection to retrieve

        Returns:
            DatasetCollection: A dataset collection complex model, containing a list of DatasetCollectionItem complex models.

        Raises:
            ResourceNotFoundError: If the collection does not exist

        """
        dataset_coln_i = data.get_dataset_collection_by_name(collection_name,
                                                             **ctx.in_header.__dict__)
        return DatasetCollection(dataset_coln_i)

    @rpc(DatasetCollection, _returns=DatasetCollection)
    def add_dataset_collection(ctx, collection):
        """
        Add a dataset collection:
        The name of the collection does NOT need to be unique, so be careful
        with the naming to ensure the collection is searchable later.

        Args:
            collection (DatasetCollection): A DatasetCollection complex model containing a list of DatasetCollectionItem objects

        Returns:
            DatasetCollection: The same collection as was sent in, but with an ID

        """

        dataset_coln_i = data.add_dataset_collection(collection, **ctx.in_header.__dict__)

        new_coln = DatasetCollection(dataset_coln_i)
        return new_coln

    @rpc(Unicode, _returns=SpyneArray(DatasetCollection))
    def get_collections_like_name(ctx, collection_name):
        """
        Get all the dataset collections with a name like the specified name

        Args:
            collection_name (string): The collection name to search.

        Returns:
            List(DatasetCollection): All the collections with names similar to the specified name
        """
        collections = data.get_collections_like_name(collection_name, **ctx.in_header.__dict__)
        ret_collections = [DatasetCollection(g) for g in collections]
        return ret_collections

    @rpc(Integer, _returns=SpyneArray(Dataset))
    def get_collection_datasets(ctx, collection_id):
        """
            Get all the datasets from the collection with the specified name

            Args:
                collection_id (int): The collection whose dastasets we want to retrieve

            Returns:
                List(Dataset): A list of dastaset complex models, all of them in the collection specified
        """
        collection_datasets = data.get_collection_datasets(collection_id,
                                                 **ctx.in_header.__dict__)
        ret_data = [Dataset(d) for d in collection_datasets]

        return ret_data

    @rpc(Dataset, _returns=Dataset)
    def update_dataset(ctx, dataset):
        """
            Update a piece of data directly, rather than through a resource
            scenario.

            Args:
                dataset (Dataset): A complex model representing an existing dataset which is to be updsated (must have an id)

            Returns:
                Dataset: The updated dataset
        """
        val = dataset.parse_value()

        metadata = dataset.get_metadata_as_dict()

        updated_dataset = data.update_dataset(dataset.id,
                                        dataset.name,
                                        dataset.type,
                                        val,
                                        dataset.unit,
                                        dataset.dimension,
                                        metadata,
                                        **ctx.in_header.__dict__)

        return Dataset(updated_dataset)


    @rpc(Integer, _returns=Unicode)
    def delete_dataset(ctx, dataset_id):
        """
            Removes a piece of data from the DB.
            CAUTION! Use with care, as this cannot be undone easily.

            Args:
                dataset_id (int): The ID of the dataset to be deleted.

            Returns:
                string: 'OK'
        """
        data.delete_dataset(dataset_id, **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, Unicode(min_occurs=0, max_occurs='unbounded'), _returns=AnyDict)
    def get_val_at_time(ctx, dataset_id, timestamps):
        """
        Get the value of the dataset at a specified time (s).
        
        - If the dataset is not a timeseries, just return the value
        
        - If the dataset is a timeseries, return the value within the timeseries
        that is closest to the requested time(s). 
        
        - If the time specified occurs before the start of the timeseries, return None. 
        
        - If the time specified occurs after the end of the timeseries, return the last value in the timeseries.

        Args:
            dataset_id (int): The ID of the dataset being searched
            timestamps (List(timestamps)): A list of timestamps to get values for.

        Returns:
            dict: A dictionary, keyed on the timestamps requested

        """
        return data.get_val_at_time(dataset_id, 
                                    timestamps,
                                    **ctx.in_header.__dict__)
    
    @rpc(Integer32(min_occurs=0, max_occurs='unbounded'), Unicode(min_occurs=0, max_occurs='unbounded'), _returns=AnyDict)
    def get_multiple_vals_at_time(ctx, dataset_ids, timestamps):
        """
        Similar to get_val_at_time, but perform the action on multiple datasets at once
        
        - If the dataset is not a timeseries, just return the value
        
        - If the dataset is a timeseries, return the value within the timeseries
        that is closest to the requested time(s). 
        
        - If the time specified occurs before the start of the timeseries, return None. 
        
        - If the time specified occurs after the end of the timeseries, return the last value in the timeseries.

        Args:
            dataset_ids (List(int)): The IDs of the datasets being searched
            timestamps (List(timestamps)): A list of timestamps to get values for.

        Returns:
            dict: A dictionary, keyed on the dataset_id, then by the timestamps requested

        """

        result = data.get_multiple_vals_at_time(dataset_ids, 
                                    timestamps,
                                    **ctx.in_header.__dict__)
        return result

    @rpc(Integer,Unicode,Unicode,Unicode(values=['seconds', 'minutes', 'hours', 'days', 'months']), Decimal(default=1),_returns=AnyDict)
    def get_vals_between_times(ctx, dataset_id, start_time, end_time, timestep, increment):
        """
        Retrive data between two specified times within a timeseries. The times
        need not be specified in the timeseries. This function will 'fill in the blanks'.

        Two types of data retrieval can be done.

        If the timeseries is timestamp-based, then start_time and end_time
        must be datetimes and timestep must be specified (minutes, seconds etc).
        'increment' reflects the size of the timestep -- timestep = 'minutes' and increment = 2
        means 'every 2 minutes'.

        If the timeseries is float-based (relative), then start_time and end_time
        must be decimal values. timestep is ignored and 'increment' represents the increment
        to be used between the start and end.
        Ex: start_time = 1, end_time = 5, increment = 1 will get times at 1, 2, 3, 4, 5

        Args:
            dataset_id (int): The dataset being queried
            start_time (string): The date or value from which to start the query
            end_time   (string): The date or value that ends the query
            timestep   Enum(string): 'seconds', 'minutes', 'hours', 'days', 'months':
                The increment in time that the result will be in
            increment  (decimal): The increment that the result will be in if the timeseries is not timestamp-based.

        Returns:
            (AnyDict): A dictionary, keyed on the newly created timestamps, which have been
                        created from the start time and timesteps.

        """
        return data.get_vals_between_times(dataset_id, 
                                           start_time, 
                                           end_time, 
                                           timestep,
                                           increment,
                                           **ctx.in_header.__dict__)

    @rpc(Unicode, _returns=Unicode)
    def check_json(ctx, json_string):
        """
        Check that an incoming data string is json serialisable.
        Used for testing.

        Args:
            json_string (string): A json string to be tested for validity

        Returns:
            'OK' or '"Unable to process JSON string. error was:..."
        """
        try:
            data.check_json(json_string)
        except Exception, e:
            return "Unable to process JSON string. error was: %s"%e

        return 'OK'


