#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""MODULE : dict_tools, part of hydrology.util

"""

__all__ = ["DictDiff", "sortPrintDict", "dictToSortList", "dictToSortArray"]


class DictDiff(object):
    """
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values

    This code is copied from an answer contributed by user hughdbrown
    at http://stackoverflow.com/questions/1165352.
    """

    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), \
                                              set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)

    def added(self):
        return self.set_current - self.intersect

    def removed(self):
        return self.set_past - self.intersect

    def changed(self):
        return [o for o in self.intersect if self.past_dict[o] != \
                self.current_dict[o]]

    def unchanged(self):
        return [o for o in self.intersect if self.past_dict[o] == \
                self.current_dict[o]]


def sortPrintDict(indict):
    """Print the content of a dictionary sorted by its keys"""
    dictitems = indict.items()
    dictitems.sort()
    for i in range(len(dictitems)):
        print dictitems[i]


def dictToSortList(indict):
    """Convert the content of a dictionary to a sorted, multidimensional
    list."""
    dictkeys = indict.keys()
    dictkeys.sort()
    data = []
    for i in dictkeys:
        entry = []
        entry.append(i)
        for j in indict[i]:
            entry.append(j)
        data.append(entry)

    return data


def dictToSortArray(indict):
    """Convert the content of a directory to a sorted array."""

    from numpy import zeros

    dictkeys = indict.keys()
    dictkeys.sort()
    datalen = len(indict[dictkeys[0]])
    data = zeros((len(dictkeys), datalen + 1))

    for i in range(len(dictkeys)):
        data[i][0] = dictkeys[i]
        for j in range(datalen):
            data[i][j + 1] = indict[dictkeys[i]][j]

    return data


if __name__ == '__main__':
    """This is the place, where automated tests can be designed. They will run
    if this file is called directly from console."""
    pass
