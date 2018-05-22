import json
import pandas as pd


""" Descriptor for Scalar encoding/decoding"""
class ScalarJSON(object):
    def __get__(self, instance, owner):
#        return json.dumps({owner.tag : [instance.value]})
        return json.dumps({owner.tag : { arg : getattr(instance, arg) for arg in owner.ctor_fmt }})

    def __set__(self, instance, encstr):
#        instance.value = json.loads(encstr)[instance.tag][0]
        jo = json.loads(encstr)[instance.tag]
        for arg in instance.ctor_fmt:
            setattr(instance, arg, jo[arg])


""" Descriptor for Boolean encoding/decoding"""
class BooleanJSON(object):
    def __get__(self, instance, owner):
        return json.dumps({owner.tag : [instance.true, instance.false]})

    def __set__(self, instance, encstr):
        bd = json.loads(encstr)[instance.tag]
        instance.true, instance.false = bd[0], bd[1]


""" Descriptor for Enumeration encoding/decoding"""
class EnumJSON(object):
    def __get__(self, instance, owner):
        return json.dumps({owner.tag : instance.values})

    def __set__(self, instance, encstr):
        instance.values = json.loads(encstr)[instance.tag]


""" Descriptor for Array encoding/decoding"""
class ArrayJSON(object):
    def __get__(self, instance, owner):
        return json.dumps({owner.tag : instance.data.to_dict()})

    def __set__(self, instance, encstr):
        data = json.loads(encstr)[instance.tag]
        instance.data = pd.DataFrame.from_dict(data)
