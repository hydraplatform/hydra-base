Time Series
===========

Hydra supports three types of time series: "normal" time series, equally spaced
time series and seasonal time series. The SOAP interface provides methods to
store data on the server and to retrieve them from the server.

Saving time series data to the server
-------------------------------------

"Normal" time series and seasonal time series
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Time series are sent to the server as ``TimeSeries`` objects. Every element of
the time series added to the ``TimeSeries`` object as a ``TimeSeriesData``
object. 

A ``TimeSeries`` object only has the field ``ts_values`` which holds an array of
``TimeSeriesData`` objects. A ``TimeSeriesData`` object has the fields ``ts_time``
and ``ts_value``. While the value can be anything, a string, a number or an
array, the time needs to follow a certain pattern::

    '%Y-%m-%d %H:%M:%S.%f%z'

following these specifications

    ====== ========================================= =========
    ``%Y`` year, four digits                         ``2013``
    ``%m`` month, two digits                         ``10``
    ``%d`` day, two digits                           ``03``
    ``%H`` hour, two digits                          ``00``
    ``%M`` minute, two digits                        ``49``
    ``%S`` seconds, two digits                       ``17``
    ``%f`` fractional seconds                        ``568``
    ``%z`` time zone offset, sign and four digits    ``-0400``
    ====== ========================================= =========

for example::

    '2013-10-03 00:49:17.568-0400'

A time stamp can take any value from January 1st, year 2 (year 1 in reserved for
seasonal time series). In a time stamp for a seasonal time series the year is
replaced by either ``'0001'`` or ``'XXXX'``. We recommend to use the latter,
since using year 1 explicitly might not be supported in future versions of
Hydra.


An example in Python
********************

In Python the code to generate a complete time series object could look like
this:

.. code-block:: python
   :linenos:

   from suds import Client

   url = 'http://localhost:8000?wsdl'
   client = Client(url)

   #These should obviously be valid IDs
   scenario_id = AAA
   resource_attribute_id = BBB

   dataset = [(datetime(2002, 1, 1), 4.0), (datetime(2002, 1, 2), 4.5)]

   #In general, data sent to the server must be contained in a 'dataset' obhect
   dataset = self.client.factory.create('ns1:Dataset')
   dataset.type      = 'timeseries'
   dataset.name      = 'Max Capacity'
   dataset.unit      = 'metres cubed'
   dataset.dimension = 'Volume'


   #Create the timeseries object
   timeseries = {"Header": {}}
   for time, value in dataset:
        t = PluginLib.date_to_string(time)
        timeseries["0"][t] = value

   timeseries.ts_values = json.dumps(timeseries)

   dataset.value = timeseries

   client.service.add_data_to_attribute(scenario_id, resource_attribute_id, dataset)

If you would like to save a seasonal time series to the server an optional
parameter to ``PluginLib.date_to_string()`` should to the trick::

   tsdata.ts_time = PluginLib.date_to_string(time, seasonal=True)
    

Retrieving time series from the server
--------------------------------------


