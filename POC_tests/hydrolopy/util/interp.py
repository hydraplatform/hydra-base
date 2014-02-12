#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""MODULE : interp_tools, part of hydrology.util

"""

__all__ = ["polynomInterp", "linearInterp"]


def polynomInterp(xdata, ydata, x0):
    """y0 = polynomInterp(x, y, x0)

    This function retruns the value of the best fit polynom of order n+1 at a
    single position x0, where n is the numer of given points (the length of x
    and y). To do this the Aitken-Neville algorithm is used. The efficiency of
    this algrithm is O(n^2)."""

    from numpy import zeros

    if x0 in xdata:
        idx = [i for i, x in enumerate(xdata) if x == x0][0]
        return ydata[idx]

    # Allocate matrix for the solution scheme
    n = len(xdata)
    p = zeros((n, n))

    for i in range(n):
        p[i][i] = ydata[i]

    r = 1
    while r < n:
        for j in range(r, n):
            i = j - r
            p[i][j] = ((xdata[j] - x0) * p[i][j - 1] \
                      + (x0 - xdata[i]) * p[i + 1][j]) \
                      / (xdata[j] - xdata[i])
        r += 1

    return p[0][n - 1]


def linearInterp(xdata, ydata, x0):
    """y0 = linearInterp(x, y, x0)

    This function interpolates the value at x0 linearly between the nearest
    elements of (x, y) pairs."""

    if x0 in xdata:
        idx = [i for i, x in enumerate(xdata) if x == x0][0]
        return ydata[idx]

    i2 = 0
    while xdata[i2] < x0:
        i2 += 1

    i1 = i2 - 1

    return ydata[i2] - \
           ((ydata[i2] - ydata[i1]) / (xdata[i2] - xdata[i1])) * xdata[i2] + \
           ((ydata[i2] - ydata[i1]) / (xdata[i2] - xdata[i1])) * x0


if __name__ == '__main__':
    """This is the place, where automated tests can be designed. They will run
    if this file is called directly from console."""
    pass
