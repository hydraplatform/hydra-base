#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" MODULE : TSmanipulation, part of hydrology.TS

"""

__all__ = ["movingAverage", "lin_fillgaps"]


def movingAverage(TS, window, pos='center'):
    """Calculate a moving average of the given time series with a given window
    size. This function returns a time series of the same length as the input.

    It also supports time series featuring a vector at each time step. The
    moving average is then calculated over all the first elements, all the
    second elements, and so on.

    The optional parameter 'pos' determines whether the moving average should
    calculate the averaged value from the preceding values ('left'), the
    following values ('right'), or both ('center', default)."""

    from TimeSeries import TimeSeries

    timeAxis = TS.getTime()
    timeAxis.sort()

    # get the lenght of the vector stored for each date
    veclen = len(TS.getData(timeAxis[1]))

    # get a separate list for all members of the aboe vector
    TsData = dict()
    for i in range(veclen):
        TsData[i] = []
        for t in timeAxis:
            TsData[i].append(TS.getData(t)[i])

    TSlen = len(TsData[0])

    # calculate the moving average for each time series
    sidx = 0
    eidx = 1
    avgVec = dict()
    endflag = False
    for i in range(veclen):
        avgVec[i] = []
        while eidx - sidx > 0:
            tmpdata = TsData[i][sidx:eidx]
            avgVec[i].append(sum(tmpdata) / len(tmpdata))
            eidx += 1
            if eidx - sidx > window or endflag:
                sidx += 1
            if eidx > TSlen:
                eidx -= 1
                endflag = True
        sidx = 0
        eidx = 1
        endflag = False

    avgData = TimeSeries()
    for i in range(veclen):
        for j in range(len(timeAxis)):
            if pos == 'right':
                avgData.append(timeAxis[j], avgVec[i][j])
            elif pos == 'left':
                avgData.append(timeAxis[j], avgVec[i][j + window - 1])
            elif pos == 'center':
                avgData.append(timeAxis[j], avgVec[i][j + window / 2])

    return avgData


def lin_fillgaps(TS, NODATA=-9999):
    """Fill gaps in the time series with linear interpolation. Use this
    function with care, in case of large data gaps it can lead to very strange
    time series."""

    import numpy as np
    from hydrolopy.util import linearInterp

    timeaxis = TS.getTime()
    timeaxis.sort()

    # Find gaps
    beforegaps = dict()
    gaps = dict()
    aftergaps = dict()
    i = 0
    gapfirst = True  # used if a gap is longet than one day
    for t in timeaxis:
        tdata = TS.getData(t)[0]
        if tdata == NODATA or np.isnan(tdata):
            if gapfirst:  # If we are at the first element of a gap
                gaps[i] = []
                beforegaps[i] = t - 1
            gaps[i].append(t)
            gapfirst = False

            nextdata = TS.getData(t + 1)[0]
            if nextdata != NODATA and not np.isnan(nextdata):
                # If we are at the last position of the gap
                gapfirst = True
                aftergaps[i] = t + 1
                i += 1

    lendata = len(TS.getData(timeaxis[0]))

    for i in gaps.keys():  # delete no data values
        for t in gaps[i]:
            TS.add(t, [])

    for i in gaps.keys():
        for j in range(lendata):
            xdata = [beforegaps[i].toordinal()[0], \
                     aftergaps[i].toordinal()[0]]
            ydata = [TS.getData(beforegaps[i])[j], \
                     TS.getData(aftergaps[i])[j]]
            for t in gaps[i]:
                y0 = linearInterp(xdata, ydata, t.toordinal()[0])
                TS.append(t, y0)

    return TS


# TESTS
if __name__ == '__main__':
    """This is the place, where automated tests can be designed. They will run
    if this file is called directly from console."""

    # Test the function novingAverage

    #-- generate test dataset
    from TimeSeries import TSdate, TimeSeries
    import matplotlib.pylab as plt
    import numpy as np

    timeAxis = TSdate()
    startdate = TSdate((2012,1,1))
    enddate = TSdate((2012,10,31))
    timeAxis.addRange(startdate, enddate)

    testdata = TimeSeries()

    for t in timeAxis:
        testdata.add(t, [float(t.month()[0]), float(t.month()[0] * 2)])

    # Insert two data gaps, a long and a short one
    startgap = TSdate((2012, 8, 12))
    endgap = TSdate((2012, 8, 20))
    gapT = TSdate()
    gapT.addRange(startgap, endgap)
    for t in gapT:
        testdata.add(t, [np.nan, np.nan])
    testdata.add(TSdate((2012, 6, 11)), [np.nan, np.nan])

    # Moving average
    testavg = movingAverage(testdata, 11, pos='center')

    # Fill data
    testdataFill = lin_fillgaps(testdata, NODATA=np.nan)


    testdata.plot()
    testdataFill.plot()
    testavg.plot(label=['1', '2', '1f', '2f', '1a', '2a'])
    plt.show()
