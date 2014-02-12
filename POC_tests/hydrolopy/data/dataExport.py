#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" MODULE : dataExport, part of hydrology.data

"""

__all__ = ["TStoDat"]

def TStoDat(TSdata, filename, form=' {0:12.2f}', NoData="          NaN"):
    """Export time series data to a formatted text datafile."""

    time = TSdata.getTime()

    time.sort()

    datf = open(filename, 'w')

    for t in time:
        data = TSdata.getData(t)
        datf.write(str(t))
        for d in data:
            if d:
                datf.write(form.format(d))
            else:
                if isinstance(NoData, str):
                    datf.write(NoData)
                else:
                    datf.write(form.format(NoData))
        datf.write("\n")

    datf.close()

if __name__ == '__main__':
    """This is the place, where automated tests can be designed. They will run
    if this file is called directly from console."""
    pass
