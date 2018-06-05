import json
import pandas as pd


""" Descriptor for Scalar JSON encoding/decoding"""
class ScalarJSON(object):
    def __get__(self, instance, owner):
        return json.dumps({owner.tag : { arg : getattr(instance, arg) for arg in owner.ctor_fmt }})

    def __set__(self, instance, encstr):
        jo = json.loads(encstr)[instance.tag]
        for arg in instance.ctor_fmt:
            setattr(instance, arg, jo[arg])


""" Descriptor for Array JSON encoding/decoding"""
class ArrayJSON(object):
    def __get__(self, instance, owner):
        return json.dumps({owner.tag : instance.data.to_dict()})

    def __set__(self, instance, encstr):
        data = json.loads(encstr)[instance.tag]
        instance.data = pd.DataFrame.from_dict(data)


""" Descriptor for (Hydra) Descriptor JSON encoding/decoding"""
class DescriptorJSON(object):
    def __get__(self, instance, owner):
        return ""

    def __set__(self, instance, encstr):
        pass


""" Descriptor for Dataframe JSON encoding/decoding"""
class DataframeJSON(object):
    def __get__(self, instance, owner):
        return "{}"

    def __set__(self, instance, encstr):
        pass


""" Descriptor for Timeseries JSON encoding/decoding"""
class TimeseriesJSON(object):
    def __get__(self, instance, owner):
        return "{}"

    def __set__(self, instance, encstr):
        pass
