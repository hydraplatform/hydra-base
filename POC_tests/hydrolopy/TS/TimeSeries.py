#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) Philipp Meier <philipp@diemeiers.ch>

""" MODULE : TimeSeries, part of hydrology.TS

"""

__all__ = ["TSdate", "TimeSeries", "TimeSeriesError"]


from datetime import datetime, date
from numpy import isnan
import matplotlib.pyplot as plt


## Classes
class TSdate(object):
    """A flexible class which allows to handle calendar dates usually used
       for time series. It allows to represent monthly data and data (monthly
       or daily) which is repeated each year. It can represent a single
       date or a series of dates (time axis of a time series).
    """

    def __init__(self, tsdate=False, strform='YYYYmmmDD'):
        if tsdate:
            self.date = []
            self.add(tsdate, strform)
        else:
            self.date = []

#    def __del__(self):
#        # Close the generator object defined in __iter__. Usually python takes
#        # care of that, thus this function is not used.
#        self.__iter__.close()

    def __cmp__(self, other):
        # Return zero if self == other, -1 if self < other, 1 if self > other
        sy = self.year()

        if len(sy) != 1:
            raise TimeSeriesError(4, "TSdate: only single dates allowed.")

        sy = sy[0]
        oy = other.year()[0]
        sm = self.month()[0]
        om = other.month()[0]
        sd = self.day()[0]
        od = other.day()[0]

        if sy == oy and sm == om and sd == od:
            return 0
        elif (sy < oy) or \
             (sy == oy and sm < om) or \
             (sy == oy and sm == om and sd < od):
            return -1
        elif (sy > oy) or \
             (sy == oy and sm > om) or \
             (sy == oy and sm == om and sd > od):
            return 1
        elif (sy == 'rep' and sd == 'mon') or \
                (oy == 'rep' and od == 'mon') or \
                (sy == 'rep' and od == 'mon') or \
                (sd == 'mon' and oy == 'rep'):
            if sm == om:
                return 0
            elif sm < om:
                return -1
            elif sm > om:
                return 1
        elif sy == 'rep' or oy == 'rep':
            if sm == om and sd == od:
                return 0
            elif sm < om or (sd < od and sm <= om):
                return -1
            elif sm > om or (sd > od and sm >= om):
                return 1
        elif sd == 'mon' or od == 'mon':
            if sy == oy and sm == om:
                return 0
            elif sy < oy or (sm < om and sy <= oy):
                return -1
            elif sy > oy or (sm > om and sy >= oy):
                return 1
        else:
            raise TimeSeriesError(3, "TSdate: invalid format of date.")

    def __hash__(self):
        y = self.year()[0]
        m = self.month()[0]
        d = self.day()[0]
        add = 0
        f = 1
        if isinstance(y, str) and y == 'rep':
            y = 4
            add = 1095  # corresponds to three complete years, because we need
                        # to accomodate data for a leap year
            f = -1

        if isinstance(d, str) and d == 'mon':
            d = 1
            add = -366
            f = -1

        t = date(int(y), int(m), int(d))
        return f * t.toordinal() + add

    def __iter__(self):
        # Generator function to allow iterations over the content of a TSdata
        # container.
        for d in range(len(self.date)):
            yield TSdate(self.date[d])

    def __len__(self):
        return len(self.date)

    def __add__(self, other):
        if isinstance(other, int):
            numdate = []
            for d in self:
                numdate.append(d.toordinal()[0] + other)
        else:
            raise TimeSeriesError(6, "TSdate: Only integers can be added to a\
                    date.")

        self = TSdate()
        for i in numdate:
            self.fromordinal(i)

        return self

    def __sub__(self, other):
        return self.__add__(-other)

    def __str__(self):
        outstr = ''
        sepstr = ''
        if len(self) > 1:
            sepstr = ', '
        for d in self.date:
            if d[1] < 10:
                fillm = '0'
            else:
                fillm = ''

            if d[2] < 10:
                filld = '0'
            else:
                filld = ''

            outstr = outstr + str(d[0]) + '-' + \
                fillm + str(d[1]) + '-' + \
                filld + str(d[2]) + sepstr
        return outstr

    @staticmethod
    def today():
        """Return the date of today."""
        t = date.today()
        return TSdate((t.year, t.month, t.day))

    @staticmethod
    def isleap(self):
        """Check whether the current year is a leap year."""
        return ((self.year()[0] % 4) == 0 and (self.year()[0] % 100) != 0) \
            or (self.year()[0] % 400) == 0

    def add(self, indate, strform='YYYYmmmDD'):
        """Add a date to the variable."""
        if isinstance(indate, tuple) and len(indate) == 3:
            self.date.append(indate)
        elif isinstance(indate, TSdate):
            self.date.append((indate.year(), indate.month(), indate.day()))
        else:
            indate = _generateTSdate(indate, strform)
            self.date.append(indate.date[0])

    def addRange(self, startdate, enddate):
        """Add a range of consecutive dates to a variable."""
        monflag = 0
        if isinstance(startdate, tuple) and isinstance(enddate, tuple):
            startdate = date(startdate[0], startdate[1], startdate[2])
            enddate = date(enddate[0], enddate[1], enddate[2])
        elif isinstance(startdate, TSdate) and isinstance(enddate, TSdate):
            # handle special case of monthly data
            if startdate.day()[0] == 'mon' or enddate.day()[0] == 'mon':
                monflag = 1
                yrrange = range(startdate.year()[0], enddate.year()[0])
                startmonth = startdate.month()[0]
                endmonth = enddate.month()[0]
                for yr in yrrange:
                    if yr < enddate.year()[0]:
                        while startmonth <= 12:
                            self.add((yr, startmonth, 'mon'))
                            startmonth += 1
                        startmonth = 1
                    else:
                        while startmonth <= endmonth:
                            self.add((yr, startmonth, 'mon'))
                            startmonth += 1

            else:
                startdate = date(startdate.year()[0], startdate.month()[0],
                                 startdate.day()[0])
                enddate = date(enddate.year()[0], enddate.month()[0],
                               enddate.day()[0])
        else:
            raise TimeSeriesError(5, "TSdate: Start and end date need to be of\
                    the same type")

        if monflag != 1:
            drange = range(startdate.toordinal(), enddate.toordinal())
            drange = [date.fromordinal(x) for x in drange]

            for d in drange:
                self.add((d.year, d.month, d.day))

    def getIterator(self, enddate):
        """This function is similar to addRange. However, it returns an
        iterator object, where the next date in a range can be called using the
        next command.

        Example:
        startdate = TSdate((2002, 1, 1))
        iterator = startdate.getIterator((2002, 1, 31))
        iterator.next()  # returns TSdate((2002, 1, 2))"""

        while self <= enddate:
            yield self
            self += 1
        return

    def day(self):
        """Return the day of month of the current date as numeric value."""
        dout = []
        for i in range(len(self.date)):
            dout.append(self.date[i][2])
        return dout

    def month(self):
        """Return the month of the current date as numeric value."""
        mout = []
        for i in range(len(self.date)):
            mout.append(self.date[i][1])
        return mout

    def year(self):
        """Return the year of the current date as numeric value."""
        yout = []
        for i in range(len(self.date)):
            yout.append(self.date[i][0])
        return yout

    def monthly(self):
        """Convert the current date to a monthly date."""
        mondate = TSdate()
        for i in range(len(self.date)):
            mondate.add((self.date[i][0], self.date[i][1], 'mon'))
        return mondate

    def repeated(self):
        """Define the current date as a repeated date."""
        repdate = TSdate()
        for i in range(len(self.date)):
            repdate.add(('rep', self.date[i][1], self.date[i][2]))
        return repdate

    def toordinal(self):
        """Return the ordinal value of the date, in days from 1st of January in
        year 1."""
        d = []
        for t in self:
            td = datetime(t.year()[0], t.month()[0], t.day()[0])
            d.append(td.toordinal())
        return d

    def fromordinal(self, orddate):
        """Generate a date from its ordinal value (in days from 1st of January
        in year 1)."""
        if isinstance(orddate, int):
            orddate = [orddate]
        for d in orddate:
            td = datetime.fromordinal(d)
            self.add((td.year, td.month, td.day))


class TimeSeries(object):
    """Class to save hydrological time series in a concise manner."""

    def __init__(self, timestamp=None, data=None):
        self.data = dict()
        if timestamp and data:
            self.add(timestamp, data)

    def __iter__(self):
        for d in self.data.keys():
            yield TimeSeries(d, self.data[d])

    def __len__(self):
        return len(self.data)

    def __add__(self, other):
        """Add two time series in a manner that only time steps are added which
        exist in both of the time series. In general the resulting time series
        is shorter."""

        newTS = TimeSeries()
        if isinstance(other, TimeSeries):
            for t in self.data.keys():
                if t in other.data:
                    A = self.getData(t)
                    B = other.getData(t)
                    if len(A) == len(B):
                        newTS.add(t, [])
                        for i in range(len(A)):
                            newTS.append(t, A[i] + B[i])
        else:
            for t in self.data.keys():
                newTS.add(t, self.getData(t) + other)

        return newTS

    def __sub__(self, other):
        """Add two time series in a manner that only time steps are added which
        exist in both of the time series. In general the resulting time series
        is shorter."""

        newTS = TimeSeries()
        if isinstance(other, TimeSeries):
            for t in self.data.keys():
                if t in other.data:
                    newTS.add(t, self.getData(t) - other.getData(t))
        else:
            for t in self.data.keys():
                newTS.add(t, self.getData(t) - other)

        return newTS

    def __mul__(self, other):
        """Add two time series in a manner that only time steps are added which
        exist in both of the time series. In general the resulting time series
        is shorter."""

        newTS = TimeSeries()
        if isinstance(other, TimeSeries):
            for t in self.data.keys():
                if t in other.data:
                    newTS.add(t, self.getData(t) * other.getData(t))
        else:
            for t in self.data.keys():
                newTS.add(t, self.getData(t) * other)

        return newTS

    def __div__(self, other):
        """Add two time series in a manner that only time steps are added which
        exist in both of the time series. In general the resulting time series
        is shorter."""

        newTS = TimeSeries()
        if isinstance(other, TimeSeries):
            for t in self.data.keys():
                if t in other.data:
                    newTS.add(t, self.getData(t) / other.getData(t))
        else:
            for t in self.data.keys():
                newTS.add(t, self.getData(t) / other)

        return newTS

    def getData(self, getdate, default=-9999):
        """Get data for the specified date. If no data is found, the default
        value is returned."""
        if getdate in self.data:
            outdata = self.data[getdate]
        elif getdate.monthly() in self.data:
            outdata = self.data[getdate.monthly()]
        elif getdate.repeated() in self.data:
            outdata = self.data[getdate.repeated()]
        elif getdate.repeated().monthly() in self.data:
            outdata = self.data[getdate.repeated().monthly()]
        else:
            outdata = [default]

        return outdata

    def getTime(self):
        """Return the time axis of the TimeSeries data."""
        return self.data.keys()

    def add(self, timestamp, data):
        "This function adds data to the class. Existing data are overwritten."
        if isinstance(timestamp, TSdate):
            if isinstance(data, list):
                self.data.update({timestamp: data})
            else:
                self.data.update({timestamp: [data]})
        else:
            raise TimeSeriesError(10, "hydrologyTS: timestamp needs to be\
                    of TSdate type.")

    def delete(self, timestamp):
        "This function deletes a single data point from the class."
        if isinstance(timestamp, TSdate):
            del self.data[timestamp]
        else:
            raise TimeSeriesError(10, "hydrologyTS: timestamp needs to be\
                    of TSdate type.")

    def append(self, timestamp, data):
        "This function appends data to existing data."
        if isinstance(timestamp, TSdate):
            if timestamp in self.data:
                stepdata = self.getData(timestamp)
                stepdata.append(data)
                self.data.update({timestamp: stepdata})
            else:
                self.add(timestamp, data)

    def monthlymean(self):
        "Return the monthly mean of the time series."
        monthlyTS = TimeSeries()
        timeaxis = self.getTime()
        # Collect data
        for i in range(len(self.getData(timeaxis[0]))):
            tmpTS = TimeSeries()
            for t in timeaxis:
                # filter NaN
                data = self.getData(t)[i]
                if not isnan(data):
                    tmpTS.append(t.monthly(), self.getData(t)[i])

            # Calculate the monthly average
            for t in tmpTS.getTime():
                D = tmpTS.getData(t)
                monthlyTS.append(t, sum(D) / len(D))

        return monthlyTS

    def LTaverage(self):
        "Retrun long-term average for each day or month."
        # Collect data
        avgTS = TimeSeries()
        timeaxis = self.getTime()

        for i in range(len(self.getData(timeaxis[0]))):
            tmpTS = TimeSeries()
            for t in self.getTime():
                tmpTS.append(t.repeated(), self.getData(t)[i])

            # Calculate the average and return
            for t in tmpTS.getTime():
                avgTS.append(t, sum(tmpTS.getData(t)) / len(tmpTS.getData(t)))

        return avgTS

    def addAverageData(self):
        "Add average data which is used to fill data gaps (if any)."
        # Collect data
        tmpTS = TimeSeries()
        for t in self.getTime():
            tmpTS.append(t.repeated(), self.getData(t)[0])

        # Calculate average and add to time series
        for t in tmpTS.getTime():
            self.add(t, sum(tmpTS.getData(t)) / len(tmpTS.getData(t)))

    def importTS(self, extdata, dateformat='YYYYmmmDD'):
        """Import time series data from a multidimensional list, which might be
        the product of a imported CSV-file or similar."""

        for line in extdata:
            timestamp = _generateTSdate(line[0], dateformat)
            self.add(timestamp, line[1:])

    def startdate(self):
        "Return the start date of the time series."
        dates = self.data.keys()
        return min(dates)

    def enddate(self):
        "Return the start date of the time series."
        dates = self.data.keys()
        return max(dates)

    def plot(self, ylabel='Data', label=['Data']):
        "TS.plot() - Plot the time series."
        timestamp = self.data.keys()
        timestamp.sort()
        plotdata = []
        plottime = []
        for t in timestamp:
            year = t.year()[0]
            month = t.month()[0]
            day = t.day()[0]
            if day == 'mon':
                day = 1

            if year == 'rep':
                #repflag = True
                year = 4

            plottime.append(datetime(year, month, day))
            plotdata.append(self.getData(t))

        plt.axes([.1, .2, .85, .75])
        plt.plot(plottime, plotdata)
        plt.ylabel(ylabel)
        plt.legend(label)
        plt.xticks(rotation=90)
        plt.draw()


class TimeSeriesError(Exception):
    """TimeSeriesError - Error class for all errors related to time series
    processing."""

    def __init__(self, errno, msg):
        self.args = (errno, msg)
        self.errno = errno
        self.errmsg = msg


def _generateTSdate(indate, strform='YYYYmmmDD'):
    """TSdate = _generateTSdate(indate, strform)

    Convert a given date to a TSdate object. The input can either be a
    string, a date or datetime, a tuple (year, month, day) or a TSdate. If a
    string is used its format needs to be specified.

    Example: '1996oct23' ==> strform = 'YYYYmmmDD'
             '01. Aug 85' ==> strform = 'DD. mmm YY'
             '09/22/1979' ==> strform = 'MM/DD/YYYY'
             '22.09.1979' ==> strform = 'DD.MM.YYYY'
    """

    from string import lower

    MonthNumber = dict({'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5,
                        'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10,
                        'nov': 11, 'dec': 12, 'mrz': 3, 'mai': 5, 'okt': 10,
                        'dez': 12})

    day = 'mon'
    month = 0
    year = 'rep'

    if isinstance(indate, str):
        # Get the day, month and year according to the format specified.
        datesep = ['', '', '', '']

        formfun = {
            'D': lambda x: [x, '', '', ''],
            'M': lambda x: ['', x, '', ''],
            'm': lambda x: ['', '', x, ''],
            'Y': lambda x: ['', '', '', x]}

        default = lambda x: ['', '', '', '']

        for i, fc in enumerate(strform):
            dateadd = formfun.get(fc, default)(indate[i])
            datesep = [datesep[j] + dateadd[j] for j in range(len(datesep))]

        if len(datesep[0]):
            day = int(datesep[0])
        if len(datesep[1]):
            month = int(datesep[1])
        if len(datesep[2]):
            month = MonthNumber[lower(datesep[2])[0:3]]
        if len(datesep[3]):
            year = int(datesep[3])
            if year < 100:
                if year > int(str(TSdate().today().year()[0])[2:4]):
                    year += 1900
                else:
                    year += 2000

        newdate = TSdate((year, month, day))
    elif isinstance(indate, date) or isinstance(indate, datetime):
        day = indate.day
        month = indate.month
        year = indate.year
        newdate = TSdate((year, month, day))
    elif isinstance(indate, TSdate):
        newdate = indate
    elif isinstance(indate, tuple) and len(indate) == 3:
        newdate = TSdate(indate)
    elif isinstance(indate, int) or isinstance(indate, float):
        indate = int(indate)
        if strform.lower() == 'matlab':
            matlabdate = date.fromordinal(indate - 366)
            year = matlabdate.year
            month = matlabdate.month
            day = matlabdate.day
        elif strform.lower() == 'python':
            pythondate = date.fromordinal(indate)
            year = pythondate.year
            month = pythondate.month
            day = pythondate.day
        newdate = TSdate((year, month, day))
    else:
        raise TimeSeriesError(1,
                              '_generateTSdate: Input is not a valid indate.')

    return newdate


if __name__ == '__main__':
    """This is the place, where automated tests can be designed. They will run
    if this file is called directly from console."""
    pass
