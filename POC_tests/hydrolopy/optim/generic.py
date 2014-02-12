#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" MODULE : generic, part of hydrology.optim

"""

__all__ = ["reservoirData", "importReservoirData", "generateCombinations"]

from datetime import datetime
from hydrolopy.TS.TimeSeries import _generateTSdate, TimeSeriesError, TSdate
from hydrolopy.util import linearInterp
from hydrolopy.data import importODS

## Classes:
class reservoirData(object):
    """reservoirData - A class which allows to store and manipulate data
    associated to reservoirs. Each reservoir is identified by a unique ID and a
    name.  It allows to store geometry data associated to the reservoir and
    time series data."""

    numReservoir = 0

    def __init__(self):
        self.id = []
        self.name = []
        self.TimeSeries = []
        self.Geometry = []
        self.parameter = []

    def addReservoir(self, id, name):
        "Add a new reservoir."
        if id in self.id:
            raise TimeSeriesError(2, "Duplicate id.")
        self.id.append(int(id))
        self.name.append(name)
        self.TimeSeries.append(dict())
        self.Geometry.append(dict())
        self.parameter.append(dict())
        self.numReservoir += 1

    def getid(self):
        """Get the id's of all the reservoirs stored in the reservoir
        database."""

        return self.id

    def getname(self, id):
        """Get the name of the reservoir identified by 'id'. The name is stored
        in the database only to help the user to keep track of many reservoirs,
        it has no internal meaning."""

        for i in range(len(self.id)):
            if self.id[i] == id:
                idx = i
        return self.name[idx]

    def addParameter(self, id, parname, parval):
        """Add a constant parameter to the reservoir database. The parameter is
        identified by *parname* and its value is passed trough *parval*."""

        for i in range(self.numReservoir):
            if self.id[i] == id:
                idx = i
        self.parameter[idx].update({parname: parval})

    def getParameter(self, id, parname):
        """Retrieve the parameter identified by *parname* from the reservoir
        database."""

        for i in range(self.numReservoir):
            if self.id[i] == id:
                idx = i
        return self.parameter[idx].get(parname)

    def addTS(self, id, keyword, TS, date, dateformat='mmm'):
        """Add time series data specified by *keyword* with the corresponding
        dates. If for a certain date there is already data it is replaced by
        the newly added data. The data can be monthly or repeated yearly."""

        # get the reservoir index
        for i in range(self.numReservoir):
            if self.id[i] == id:
                idx = i

        if keyword in self.TimeSeries[idx]:
            pass
        else:
            self.TimeSeries[idx].update({keyword: dict()})

        # Test for the nature of the input (TS) we are dealing with:
        isList = isinstance(date, list) and len(TS) == len(date)
        timeIsValid = isinstance(date, str) or isinstance(date, datetime)
        timeIsTSdate = isinstance(date, TSdate)
        dataIsValid = isinstance(TS, int) or isinstance(TS, float) or \
                      isinstance(TS, list)

        if isList:
            for i in range(len(date)):
                self.TimeSeries[idx][keyword].update( \
                     {_generateTSdate(date[i], dateformat): TS[i]})
        elif timeIsValid and dataIsValid:
            self.TimeSeries[idx][keyword].update( \
                    {_generateTSdate(date, dateformat): TS})
        elif timeIsTSdate and dataIsValid:
            self.TimeSeries[idx][keyword].update({date: TS})
        else:
            raise TimeSeriesError(2, "reservoirData.addTS: Invalid input.")

    def getTS(self, id, keyword, date):
        """Get time series data specified by *keyword* for a specified date or
        a dare range. If no data for that specific date exists the method
        returns the corresponding monthly value or a value which is repeated
        for each year. This way one can save available time series data in the
        class and using monthly data or yearly average data as a fall-back for
        periods where no data are available."""

        TSdata = []
        # get the reservoir index
        for i in range(self.numReservoir):
            if self.id[i] == id:
                idx = i

        #if isinstance(date, TSdate):
        #    date = [date]

        if keyword in self.TimeSeries[idx]:
            for d in date:
                if d in self.TimeSeries[idx][keyword]:
                    TSdata.append(self.TimeSeries[idx][keyword][d])
                elif d.monthly() in self.TimeSeries[idx][keyword]:
                    TSdata.append(self.TimeSeries[idx][keyword][d.monthly()])
                elif d.repeated() in self.TimeSeries[idx][keyword]:
                    TSdata.append(self.TimeSeries[idx][keyword][d.repeated()])
                elif d.repeated().monthly() in self.TimeSeries[idx][keyword]:
                    TSdata.append( \
                        self.TimeSeries[idx][keyword][d.repeated().monthly()])
                else:
                    raise TimeSeriesError(4, 'No data found.')

        if len(TSdata) == 1:
            return TSdata[0]
        else:
            return TSdata

    def addGeometry(self, id, h, AVQS):
        """Add geometry data to the reservoir database. The geometry of a
        reservoir is here defined in a very strict sense. It consists only of a
        table relating the water level to the water volume and the surface area
        of the reservoir (level-area-volume table) and the spillgate capacity
        which is usually dependent on the water level. The geometry information
        can be added bit by bit (in order to allow updates at runtime)."""

        for i in range(self.numReservoir):
            if self.id[i] == id:
                idx = i

        self.Geometry[idx].update({h: AVQS})

    def getGeometry(self, id):
        """Retrieve the level-area-volume table of the reservoir identified by
        *id* from the reservoir database."""

        for i in range(self.numReservoir):
            if self.id[i] == id:
                idx = i

        return self.Geometry[idx]

    def getHeads(self, id):
        """Get the heads at which geometry information is available."""

        heads = self.getGeometry(id).keys()
        heads.sort()

        return heads

    def getHfromV(self, id, v0):
        """reservoirData.getHfromV(id, volume)
        Get the corresponding head of a given volume."""
        for i in range(self.numReservoir):
            if self.id[i] == id:
                idx = i

        heads = self.getHeads(id)
        V = []
        for hd in heads:
            V.append(self.Geometry[idx][hd][1])

        return linearInterp(V, heads, v0)

    def getVfromH(self, id, h):
        """reservoirData.getVfromH(id, head)
        Get the corresponding volume of a given head."""
        for i in range(self.numReservoir):
            if self.id[i] == id:
                idx = i

        heads = self.getHeads(id)
        V = []
        for hd in heads:
            V.append(self.Geometry[idx][hd][1])

        return linearInterp(heads, V, h)

    def getAfromH(self, id, h):
        """Get the corresponding area of a given head."""

        for i in range(self.numReservoir):
            if self.id[i] == id:
                idx = i

        heads = self.getHeads(id)
        A = []
        for hd in heads:
            A.append(self.Geometry[idx][hd][0])

        return linearInterp(heads, A, h)

    def getQSfromH(self, id, h):
        """Get the corresponding spillgate capacity of a given head."""

        for i in range(self.numReservoir):
            if self.id[i] == id:
                idx = i

        heads = self.getHeads(id)
        QS = []
        for hd in heads:
            QS.append(self.Geometry[idx][hd][2])

        # To calculate the maximal spillage it is better to use linear
        # interpolation than polynomial interpolation
        return linearInterp(heads, QS, h)

    def getEnergyFromH(self, id, h, Q):
        """Get the energy produced dependent on the flow and the head over the
        turbines."""

        Qmax = self.getParameter(id, 'R_max')
        floss = self.getParameter(id, 'FrictionLoss')
        eff = self.getParameter(id, 'TurbineEff')
        tH = self.getParameter(id, 'TurbineElev')
        rho = 1000  # density of water
        g = 9.81

        if Q > Qmax:
            Q = Qmax

        return rho * g * (h - tH - floss) * Q * eff


## Public functions:
def importReservoirData(filename):
    """Import reservoir data from an OpenDocument spreadsheet
    file using :py:func:`importODS`. The data are rearranged to
    a more meaningful structure in order to facilitate the incoorporation of
    the data into applications using the reservoirData class.
    
    This function expects two sheets with specific names: ``Reservoirs`` and
    ``Geometry``. All the other sheets found in the file are treated as time
    series and can be accessed through the :py:func:`getTS` function or the
    :py:class:`reservoirData` class."""

    rawData = importODS(filename)

    # Get the number of reservoirs, their names and IDs.
    Names = rawData['Reservoirs'].pop('Name')
    ID = rawData['Reservoirs'].pop('Id')

    ResData = reservoirData()

    # Add all the reservoirs to the reservoir data struct.
    for i in range(len(ID)):
        ResData.addReservoir(ID[i], Names[i])

    # Add parameters
    reservoirParameters = rawData.pop('Reservoirs')
    for param in reservoirParameters.keys():
        for i in range(ResData.numReservoir):
            ResData.addParameter(ResData.id[i], param, \
                                 reservoirParameters[param][i])

    # Add reservoir geometry
    reservoirGeometry = rawData.pop('Geometry')
    geomkeys = [x.lower() for x in reservoirGeometry.keys()]
    if 'a' and 'h' and 'id' and 'v' and 'qsmax' in geomkeys:
        id = reservoirGeometry.pop('Id')
        # choose case-insensitive
        if 'h' in reservoirGeometry:
            h = reservoirGeometry['h']
        elif 'H' in reservoirGeometry:
            h = reservoirGeometry['H']
        if 'a' in reservoirGeometry:
            a = reservoirGeometry['a']
        elif 'A' in reservoirGeometry:
            a = reservoirGeometry['A']
        if 'v' in reservoirGeometry:
            v = reservoirGeometry['v']
        elif 'V' in reservoirGeometry:
            v = reservoirGeometry['V']
        if 'qsmax' in reservoirGeometry:
            QSmax = reservoirGeometry['qsmax']
        elif 'QSmax' in reservoirGeometry:
            QSmax = reservoirGeometry['QSmax']
        elif 'Qsmax' in reservoirGeometry:   # Because LibreOffice always wants
                                             # to make the first letter capital
            QSmax = reservoirGeometry['Qsmax']

        for i in range(len(id)):
            ResData.addGeometry(id[i], h[i], (a[i], v[i], QSmax[i]))

    # Add time series, they are all that's left after the parameters and the
    # geometry are popped.
    TSkeys = rawData.keys()
    for key in TSkeys:
        TSdata = rawData.pop(key)
        TSid = TSdata.pop('Id')
        if len(TSid) > len(ResData.getid()):
            # Allow time series to contain a list of parameters for each time
            # step.
            for date in TSdata.keys():
                for id in ResData.getid():
                    tsmondata = []
                    for i in range(len(TSid)):
                        if TSid[i] == id:
                            tsmondata.append(TSdata[date][i])
                    ResData.addTS(id, key, tsmondata, date)

        else:
            for date in TSdata.keys():
                for i in range(len(TSid)):
                    ResData.addTS(TSid[i], key, TSdata[date][i], date)

    return ResData


def generateCombinations(invec):
    """Generate all possible combinations of the single values in all the
    columns of the input vecor."""

    combinations = list()
    combinations.append(())
    numCol = len(invec)
    n = 0
    while n < numCol:
        newcomb = list()
        for i in range(len(combinations)):
            for j in range(len(invec[n])):
                newcomb.append(combinations[i] + (invec[n][j],))
        combinations = newcomb
        n += 1

    return combinations


if __name__ == '__main__':
    """This is the place, where automated tests can be designed. They will run
    if this file is called directly from console."""
    pass
