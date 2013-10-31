Time Series
===========

Hydra supports three types of time series: "normal" time series, equally spaced
time series and seasonal time series. The SOAP interface provides methods to
store data on the server and to retrieve them from the server.

Saving time series data to the server
-------------------------------------

"Normal" time series and seasonal time series
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Time series are sent to the serve as ``TimeSeries`` objects. Every element of
the time series added to the ``TimeSeries`` object as a ``TimeSeriesData``
object. 

In Python the code to generate a complete time series object could look like
this:

.. code-block:: python
   :linenos:

   from suds import Client

   url = 'http://localhost:8000?wsdl'
   client = Client(url)

   dataset = [(datetime(2002, 1, 1), 4.0), (datetime(2002, 1, 2), 4.5)]

   timeseries = client.factory.create('ns1:TimeSeries')

   for time, value in dataset:
       tsdata = client.factory.create('ns1:TimeSeriesData')
       tsdata.ts_time = PluginLib.date_to_string(time)
       tsdata.ts_value = value

       timeseries.ts_values.TimeSeriesData.append(tsdata)

   client.service.add_dataset(timeseries)


Equally space time series
~~~~~~~~~~~~~~~~~~~~~~~~~


Retrieving time series from the server
--------------------------------------


