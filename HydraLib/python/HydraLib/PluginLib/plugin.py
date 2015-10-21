# (c) Copyright 2013, 2014, University of Manchester
#
# HydraLib is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
# -*- coding: utf-8 -*-

__all__ = ['JSONPlugin', 'SOAPPlugin']

import re
import logging
log = logging.getLogger(__name__)

from datetime import timedelta
from dateutil.relativedelta import relativedelta

from HydraLib.units import Units
from HydraLib.hydra_dateutil import get_time_period, get_datetime
from HydraLib.HydraException import HydraPluginError

from connection import JsonConnection
from connection import SoapConnection


class JSONPlugin(object):

    def __init__(self):
        self.units = Units()

    def connect(self, args):
        self.session_id = args.session_id
        self.server_url = args.server_url
        self.app_name = self.__class__.__bases__[0].__name__

        self.connection = JsonConnection(self.server_url, self.session_id,
                                         self.app_name)

        if self.session_id is None:
            self.session_id = self.connection.login()

        self.units = Units()

    def parse_time_step(self, time_step, target='s'):
        """
            Read in the time step and convert it to seconds.
        """
        log.info("Parsing time step %s", time_step)
        # export numerical value from string using regex
        value = re.findall(r'\d+', time_step)[0]
        valuelen = len(value)

        try:
            value = float(value)
        except:
            HydraPluginError("Unable to extract number of time steps (%s) from time step %s" % (value, time_step))

        units = time_step[valuelen:].strip()

        period = get_time_period(units)

        log.info("Time period is %s", period)

        converted_time_step = self.units.convert(value, period, target)

        log.info("Time period is %s %s", converted_time_step, period)

        return float(converted_time_step), value, period

    def get_time_axis(self, start_time, end_time, time_step, time_axis=None):
        """
            Create a list of datetimes based on an start time, end time and
            time step.  If such a list is already passed in, then this is not
            necessary.

            Often either the start_time, end_time, time_step is passed into an
            app or the time_axis is passed in directly. This function returns a
            time_axis in both situations.
        """
        if time_axis is not None:
            actual_dates_axis = []
            for t in time_axis:
                #If the user has entered the time_axis with commas, remove them.
                t = t.replace(',', '').strip()
                if t == '':
                    continue
                actual_dates_axis.append(get_datetime(t))
            return actual_dates_axis

        else:
            if start_time is None:
                raise HydraPluginError("A start time must be specified")
            if end_time is None:
                raise HydraPluginError("And end time must be specified")
            if time_step is None:
                raise HydraPluginError("A time-step must be specified")

            start_date = get_datetime(start_time)
            end_date = get_datetime(end_time)
            delta_t, value, units = self.parse_time_step(time_step)

            time_axis = [start_date]

            value = int(value)
            while start_date < end_date:
                #Months and years are a special case, so treat them differently
                if(units.lower() == "mon"):
                    start_date = start_date + relativedelta(months=value)
                elif (units.lower() == "yr"):
                    start_date = start_date + relativedelta(years=value)
                else:
                    start_date += timedelta(seconds=delta_t)
                time_axis.append(start_date)
            return time_axis


class SOAPPlugin(object):
    def connect(self, args):
        self.session_id = args.session_id
        self.server_url = args.server_url
        self.app_name = self.__class__.__bases__[0].__name__

        self.connection = SoapConnection(self.server_url, self.session_id,
                                         self.app_name)
        self.service = self.connection.client.service
        self.factory = self.connection.client.factory

        if self.session_is is None:
            self.session_id = self.connection.login()

        self.units = Units()
