#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" MODULE : stat, part of hydrology.stat

"""

__all__ = ["transitionProbability", "TSvar", "TSstdev"]

from hydrolopy.TS import TimeSeries
from numpy import isnan, array, zeros

def transitionProbability(timeseries, size):
    """Calculate the transition probability between the seasons of a given time
    series."""

    # Collect the data for each season
    seasonalData = TimeSeries()
    for d in timeseries:
        seasonalData.append(d.getTime()[0].repeated(), \
                d.getData(d.getTime()[0])[0])

    # Calculate class boundaries
    cbound = TimeSeries()
    trProb = TimeSeries()
    for sd in seasonalData:
        asd = sd.getData(sd.getTime()[0])
        # strip nan values
        asd = [x for x in asd if not isnan(x)]
        asd = array(asd)
        cwidth = (asd.max() - asd.min()) / size
        # Add boundaries
        cbound.add(sd.getTime()[0], \
                [asd.min() + x * cwidth for x in range(size + 1)])
        # preallocate transition probability matrices
        trProb.add(sd.getTime()[0], zeros((size, size)))

    # Get the class center
    ccenter = TimeSeries()
    for bound in cbound:
        thisbound = bound.getData(bound.getTime()[0])
        thisbound.sort()
        thiscenter = []
        for i in range(len(thisbound) - 1):
            thiscenter.append((thisbound[i + 1] + thisbound[i]) / 2)

        ccenter.add(bound.getTime()[0], thiscenter)

    # Calculate probabilities
    # iterate over the sorted data (hydrologyTS is an unsorted dict)
    timestamps = timeseries.getTime()
    timestamps.sort()
    for t in range(len(timestamps) - 1):
        oldData = timeseries.getData(timestamps[t])[0]
        newData = timeseries.getData(timestamps[t + 1])[0]
        oldbound = cbound.getData(timestamps[t].repeated())
        newbound = cbound.getData(timestamps[t + 1].repeated())
        oldInterv = _getinterval(oldData, oldbound)
        newInterv = _getinterval(newData, newbound)
        if oldInterv >= 0 and newInterv >= 0:
            transMat = trProb.getData(timestamps[t + 1].repeated())[0]
            transMat[oldInterv][newInterv] += 1
            trProb.add(timestamps[t + 1].repeated(), transMat)

    for transPData in trProb:
        transMat = transPData.getData(transPData.getTime()[0])[0]
        for i in range(transMat.shape[0]):
            if transMat[i].sum():
                transMat[i] = transMat[i] / transMat[i].sum()

        trProb.add(transPData.getTime()[0], transMat)

    return trProb, ccenter, cbound


def TSstdev(timeseries):
    """Calculate the standard deviation for each time step. This of course only
    works if each time step of the time series contains a list of at least 3
    values. This function returns a time series of all the standard
    deviations."""
    timestamps = timeseries.getTime()

    stdTS = TimeSeries()
    for t in timestamps:
        data = timeseries.getData(t)
        meandata = sum(data) / len(data)
        squarediff = [(data[i] - meandata) ** 2 for i in range(len(data))]
        std = (sum(squarediff) / len(squarediff)) ** .5
        stdTS.add(t, std)

    return stdTS


def TSvar(timeseries):
    """Calculate the variance for each time step. This of course only works if
    each time step of the time series contains a list of at least 3 values.
    This function returns a time series of all variances."""
    timestamps = timeseries.getTime()

    varTS = TimeSeries()
    for t in timestamps:
        data = timeseries.getData(t)
        meandata = sum(data) / len(data)
        squarediff = [(data[i] - meandata) ** 2 for i in range(len(data))]
        var = sum(squarediff) / len(squarediff)
        varTS.add(t, var)

    return varTS


def _getinterval(data, bound):
    """IntervalNumber = _getinterval(data, boundaries)

    Determine the number of the interval <data> belongs to with respect to the
    given boundaries (base zero). If the boundaries or the data contains no
    data a value of -1 is returned"""

    # Make sure the boundaries are in ascending order
    bound.sort()
    try:
        return [i for i in range(len(bound) - 1) \
                if data > bound[i] and data < bound[i + 1]][0]
    except IndexError:
        return -1


if __name__ == '__main__':
    """This is the place, where automated tests can be designed. They will run
    if this file is called directly from console."""
    pass
