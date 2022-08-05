.. _datasets:

Working with datasets
=====================

In Hydra, a dataset is not directly connected to a network or scenario. It lives
on its own, separately from everything else and can be managed independently.
While datasets can be added or removed `through` the use of scenarios, that is
merely for convenience. Datasets are separate and as such can be added, 
updated, deleted individually. In this section, we explain how this is done.

Hydra supports 4 types of values: Scalars (numbers), Descriptors (Strings of text),
Time series (a series of time-value pairs) and Arrays (a list of numbers or strings or
a combination of both).

These values are contained in Dataset objects, which require the following:

- **id**: The unique ID of the dataset with Hydra
- **type**: The type of the dataset: 'scalar', 'descriptor', 'array', 'timeseries'
- **dimension**: For example, 'length', 'volume'. For more info see TODO
- **unit**: For example 'm' or 'm^3' based on the above dimensions.
- **name**: A human-readable name of the datast
- **value**: The actual value (see below)
- **hidden**: A flag ('Y' or 'N') to indicate whether this has been hidden by the user who uploaded it, meaning other hydra users cannot view it.
- **metadata**: A list of key-value pairs representing the metadata for the dataset.

Two un-editable properties of an existing dataset are:

- **created_by**: THe user who inserted this dataset
- **cr_date**: The time this datast was created

These are created automatically by Hydra and cannot be changed.

A new dataset in the client, might look like this:

.. code-block:: python

    dataset = dict(
        id=None,
        type = 'descriptor',
        name = 'Flow speed',
        unit = 'm s^-1',
        dimension = 'Speed',
        hidden = 'N',
        value = {'desc_val':'hello'},
    )

Note the 'value' entry is itself a dict, with 'desc_val' as the key.
This is a secondary check to ensure that the type and value match.

For a given `val`, the values for the 4 data types should look like:

- **descriptor** : `{desc_val: val}`
- **scalar**     : `{param_value: val}`
- **array**      : `{arr_data: }`
- **timeseries** : `{ts_values: val}`


Adding the dataset is as simple as calling ``add_dataset``:

.. code-block:: python

    self.client.add_dataset(dataset)

Searching for Datasets
----------------------
As datasets can live in Hydra indepenently of networks, we provide a facility
to search for them, through the ``search_datasets`` function.

For example, if I know what some time ago, I added a timeseries with units of
of litres, I put these criteria into the search:

.. code-block:: python 

    self.client.service.search_datasets(units='l', data_type='timeseries')

This search will return the first 2000 timeseries matching this criteria. If there
are more and you still can't find your dataset, either change the ``page_start``
or ``page_size`` or both:

.. code-block:: python

    self.client.service.search_datasets(units='l', data_type='timeseries',
                                         page_size=3000, page_start=1999)

This search will return the 3000 results after result 1999.

.. _scalarsanddescriptors:

Scalars and descriptors
-----------------------

Scalars and descriptors are the most basic data types in Hydra.
A scalar is stored as a decimal value in Hydra, so a 1 is stored as 1.0 etc.

A scalar would look like;

.. code-block:: python

        value = {'param_value:':123}

Whereas a descriptor looks like (as above):

.. code-block:: python

        value = {'desc_val':'hello'}


.. _arrays:

Arrays
------

Hydra support n-dimensional arrays. There is no restriction on the type or shape of array you use, so long as it is parseable by the python library `numpy <http://www.numpy.org/>`_.

Some examples are:

[1, 2, 3]

[[1, 2, 3], [4, 5, 6]]

and so on...

.. _array_format:

Array format
""""""""""""
**deprecated**
In order to deal with arrays using XML, rather than using literal strings, Hydra uses a custom array format encoded as XML. THis format involves using
`<array>` and `<item>` tags, for example, from above:

.. code-block:: xml

 <array>
    <item>1</item>
    <item>2</item>
    <item>3</item>
 </array>

and for multi-dimensional arrays:

.. code-block:: xml

 <array>
  <array>
    <item>1</item>
    <item>2</item>
    <item>3</item>
  </array>
  <array>
     <item>4</item>
     <item>5</item>
     <item>6</item>
  </array>
 </array>


.. _timeseries:

Time series
-----------
**deprecated**
Hydra uses the python library `pandas <http://pandas.pydata.org/>`_ to 
support timeseries. It converts the timeseries sent to it into a pandas-compatible timeseries and then back to the expected format during a request.

In the soap interface, a timeseries is formatted as a list of dictionaries, with each dictionary having a ts_time and ts_value. For example:

.. code-block:: python

        value = {'ts_values' :
        [
            {'ts_time' : datetime.datetime.now(),
            'ts_value' : 1},
            {'ts_time' : datetime.datetime.now()+datetime.timedelta(hours=1),
            'ts_value' : 2},
            {'ts_time' : datetime.datetime.now()+datetime.timedelta(hours=2),
            'ts_value' : 'hello'},
        ]

Note that the final ts_value is a string, demonstrating that any form of value can be contained in this element.

The array frormat described in :ref:`array_format` can also be a used in a ts_value element.


Timeseries JSON format
----------------------
A series of timestamps and values (which can be single values or multi-dimensional arrays).

Using single values...
::
    
    {
        '0':
            {
                '2014-09-09 12:00:00': 12.10,
                '2014-09-09 13:00:00': 13.20,
                '2014-09-09 14:00:00': 14.40,
            }
    }

But why the '0' at the beginning?
How about we look at an array structure...
::

    {
        '0': 
            {
                '2014-09-09 12:00:00': 12.10,
                '2014-09-09 13:00:00': 13.20,
                '2014-09-09 14:00:00': 14.40,
            },
        '1': 
            {
                '2014-09-09 12:00:00': 22.10,
                '2014-09-09 13:00:00': 33.20,
                '2014-09-09 14:00:00': 44.40,
            }
    }

And we can make it even more interesting by not using numbers, but tags.
::

    {
        'OBSERVER1': 
            {
                '2014-09-09 12:00:00': 12.10,
                '2014-09-09 13:00:00': 13.20,
                '2014-09-09 14:00:00': 14.40,
            },
        'OBSERVER2': 
            {
                '2014-09-09 12:00:00': 22.10,
                '2014-09-09 13:00:00': 33.20,
                '2014-09-09 14:00:00': 44.40,
            }
    }


Metadata
--------

All datsets can have any arbitraty metadata associated with it. This is achieved by encoding metadata as simple name, value pairs.

A single piece of metadata looks like:


.. code-block:: python

    metadata = {'name':'observed_by', 'value':'Stephen Knox'}

A dataset takes a list of these pairs.


.. code-block:: python

    dataset['metadata']
        = [
            {'name':'observed_by', 'value':'Stephen Knox'},
            {'name':'observed_at', 'value':'Niagara Falls'}
        ]

Dataset Collections
-------------------
