Package documentation
=====================

.. _TS:

Using time series data
----------------------

.. automodule:: hydrolopy.TS
   :members: TSdate, TimeSeries, movingAverage, lin_fillgaps


.. _data:

Basic data import and export
----------------------------

Import
~~~~~~

.. automodule:: hydrolopy.data
   :members: importODS, importCSV


Export
~~~~~~

.. automodule:: hydrolopy.data
   :members: TStoDat

.. _evap:

Evapotranspiration
------------------

.. automodule:: hydrolopy.evap
   :members:


.. _util:

General utility functions
--------------------------


Dictionary manipulation
~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: hydrolopy.util
   :members: DictDiff, sortPrintDict, dictToSortList, dictToSortArray


Data interpolation
~~~~~~~~~~~~~~~~~~

.. automodule:: hydrolopy.util
   :members: polynomInterp, linearInterp


.. _stat:

Statistics
----------

.. automodule:: hydrolopy.stat
   :members: transitionProbability


.. _optim:

Optimisation
------------

Generic functions
~~~~~~~~~~~~~~~~

.. automodule:: hydrolopy.optim
   :members: reservoirData, importReservoirData, generateCombinations

Linear Programming
~~~~~~~~~~~~~~~~~~

.. automodule:: hydrolopy.optim
   :members:

Non-Linear Programming
~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: hydrolopy.optim
   :members: one_reservoir_NLP

Stochastic Dynamic Programming
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: hydrolopy.optim
   :members: sdpSolve, generateOptimalAllocationTable


.. _model:

Model interfaces
----------------

.. automodule:: hydrolopy.model
   :members:


.. _assim:

Data assimilation
-----------------

.. automodule:: hydrolopy.assim
   :members:

