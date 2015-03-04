import logging
log = logging.getLogger(__name__)

from decimal import Decimal
import pandas as pd
import zlib

def generate_data_hash(dataset_dict):

    d = dataset_dict
    if d.get('metadata') is None:
        d['metadata'] = {}

    hash_string = "%s %s %s %s %s %s"%(
                                str(d['data_name']),
                                str(d['data_units']),
                                str(d['data_dimen']),
                                str(d['data_type']),
                                d['value'],
                                d['metadata'])

    log.debug("Generating data hash from: %s", hash_string)

    data_hash  = hash(hash_string)

    log.debug("Data hash: %s", data_hash)

    return data_hash

def get_val(dataset, timestamp=None):
    """
        Turn the string value of a dataset into an appropriate
        value, be it a decimal value, array or time series.

        If a timestamp is passed to this function, 
        return the values appropriate to the requested times.

        If the timestamp is *before* the start of the timeseries data, return None
        If the timestamp is *after* the end of the timeseries data, return the last
        value.

        The raw flag indicates whether timeseries should be returned raw -- exactly
        as they are in the DB (a timeseries being a list of timeseries data objects,
        for example) or as a single python dictionary

    """
    if dataset.data_type == 'array':
        try:
            json.loads(dataset.value)
        except ValueError:
            #Didn't work? Maybe because it was compressed.
            val = zlib.decompress(dataset.value)
            return json.loads(val)
    elif dataset.data_type == 'descriptor':
        return str(dataset.value)
    elif dataset.data_type == 'scalar':
        return Decimal(str(dataset.value))
    elif dataset.data_type == 'timeseries':
        try:
            timeseries = pd.read_json(dataset.value)
        except ValueError:
            #Didn't work? Maybe because it was compressed.
            val = zlib.decompress(dataset.value)
            timeseries = pd.read_json(val)

        if timestamp is None:
            return timeseries
        else:
            try:
                idx = timeseries.index
                #Seasonal timeseries are stored in the year
                #1900. Therefore if the timeseries is seasonal, 
                #the request must be a seasonal request, not a 
                #standard request
                if type(idx) == pd.DatetimeIndex:
                    if set(idx.year) == set([1900]):
                        if type(timestamp) == list:
                            seasonal_timestamp = []
                            for t in timestamp:
                                t_1900 = t.replace(year=1900)
                                seasonal_timestamp.append(t_1900)
                            timestamp = seasonal_timestamp
                        else:
                            timestamp = timestamp.replace(year=1900)

                pandas_ts = timeseries.reindex(timestamp, method='ffill')

                #If there are no values at all, just return None
                if len(pandas_ts.dropna()) == 0:
                    return None

                #Replace all numpy NAN values with None
                pandas_ts = pandas_ts.where(pandas_ts.notnull(), None)

                val_is_array = False
                if len(pandas_ts.columns) > 1:
                    val_is_array = True

                if val_is_array:
                    if type(timestamp) is list and len(timestamp) == 1:
                        ret_val = pandas_ts.loc[timestamp[0]].values.tolist()
                    else:
                        ret_val = pandas_ts.loc[timestamp].values.tolist()
                else:
                    if type(timestamp) is list and len(timestamp) == 1:
                        ret_val = pandas_ts.loc[timestamp[0]][0]
                    else:
                        ret_val = pandas_ts.loc[timestamp][0].values.tolist()

                return ret_val

            except Exception, e:
                log.critical("Unable to retrive data. Check timestamps.")
                log.critical(e)
