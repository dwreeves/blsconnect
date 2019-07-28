RequestBLS
==========

The ``RequestBLS`` class is the bread and butter of the blsconnect package. This class provides a convenient connection to the BLS.gov's website, with which you can easily grab data as you need based on the Series ID.

Initializing RequestBLS
~~~~~~~~~~~~~~~~~~~~~~~

When working with RequestBLS, you should import ``pandas`` (since the class creates pandas DataFrames) and you should specify a BLS key. The RequestBLS class stores an API key, which is used to get data from the BLS API with the ``.series()`` method.

.. code-block:: python

    import pandas as pd
    from blsconnect import RequestBLS

    MY_API_KEY = ""
    bls = RequestBLS(key=MY_API_KEY)

If the BLS API key is undefined by the user, the API year limit is set to 10 years instead of 20 years. In addition, you will be unable to use the catalog feature. The user can always define a date range that exceeds the year limit; the requests are pulled in chunks and returned in a single DataFrame.

There are a few more kwargs you can specify; for more, see the docstring in the source code. In general, specifying ``key`` should be sufficient.

Getting a Series
~~~~~~~~~~~~~~~~

If you don't know what Series ID(s) you want, you should go to the BLS.gov website and `browse their data`_. After clicking on a series you want, you'll see the Series ID in either the metadata (below the table) or at the end of the URL.

Once you have your Series ID(s), you can get your data. Specifying only a ``start_year`` or an ``end_year`` will get you the maximum amount of data for a single request starting from or up to whatever year you specified. Not specifying any year will get you the last 20 (or 10) years of data. Generally it is recommended to set both of these keyword arguments.

.. code-block:: python

    my_series = ['CUSR0000SA0', 'LNS14000000']
    df = bls.series(my_series, start_year=1970, end_year=2019)
    df.head()

The ``.series()`` method can handle year ranges larger than 20 years; it will simply pull these in chunks.

By default, the data is pulled in ``'wide'`` format, which means every data series gets its own column. You can instead opt to pull the data in ``'long'`` format, which puts all the numeric values in a single column. For example, this might be useful if you are working with cross-sectional data and you want to merge your series to another table based on a particular geography.

.. code-block:: python

    regional_cpi_series = ['CUUR0100SA0', 'CUUR0200SA0', 'CUUR0300SA0', 'CUUR0400SA0']
    df = bls.series(regional_cpi_series, start_year=2005, end_year=2019, shape='long')
    df.head()

If you want to see more about your most recent data pull, you have access to a few attributes: ``messages`` and ``catalog``. 

.. code-block:: python

    bls.messages
    bls.catalog # only available with API key.

The ``messages`` list contains information about the process of pulling the data from the BLS API, e.g. information about years where data is missing. (Messages are also logged at a warning level by default; you can change this with the ``msg_log_level`` kwarg when initializing the RequestBLS class).

The ``catalog`` is part of the json returned by the API, which gives some detailed metadata about the series pulled. You can use this, for example, to verify whether you pulled the correct data.

Transforming your Data
~~~~~~~~~~~~~~~~~~~~~~

The ``.series()`` method also has a few keyword arguments for transforming data to ensure that your data is as clean as possible and you can start working with it immediately.

``interpolate`` lets you fill in missing values. This just quickly passes data into pandas's `built-in interpolate method`_, although there are two reasons why you might want to do this as a kwarg instead of once you retrieve the data: First, this appropriately handles cross-sectional data (i.e. ``shape='long'``), albeit very slowly. Second, interpolation occurs before grouping (described below), in the event you want to do both.

``groupby`` and ``groupby_method`` let you group data according to a specific frequency.

``groupby`` takes the following inputs:

.. list-table::
   :widths: 10, 10
   :header-rows: 1

   * - groupby=?
     - Frequency
   * - ``y`` or ``a``
     - Year
   * - ``s``
     - Semi-annual
   * - ``q``
     - Quarter
   * - ``m``
     - Month

``groupby_method`` goes into a ``groupby().agg()``, so any input for this works, but these are likely the most useful:

.. list-table::
   :widths: 10, 20
   :header-rows: 1

   * - groupby_method=?
     - Description
   * - ``first``
     - First non-missing value of a group
   * - ``last``
     - Last non-missing value of a group
   * - ``min``
     - Minimum value within a group
   * - ``max``
     - Maximum value within a group
   * - ``mean``
     - Mean of all non-missing values

Later in development, it is planned to send to the ``messages`` attribute what transformations affected what data. At the moment, the user is not informed of what transformations happen.

.. _browse their data: https://beta.bls.gov/dataQuery/search
.. _built-in interpolate method: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.interpolate.html