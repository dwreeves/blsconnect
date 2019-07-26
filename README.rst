BLS Connect
===========

BLS Connect is an integration of BLS's API built for Python. It provides users an easy and intuitive way to import BLS data into pandas DataFrames.

``RequestBLS().series()`` takes a Series ID input and returns a pandas DataFrame. It contains tons of useful functionality in keyword arguments, including:

- ``shape`` : Import multiple time series in either wide or long formats.
- ``interpolate`` : Interpolate missing data.
- ``groupby`` : Group by period.

``bls_search()`` makes it easy and intuitive to retrieve the Series ID's for the data you want for various popular series. This function seamlessly handles list inputs, returning a dictionary of all possible permutations from the lists provided.

**Note:** Functionality for ``bls_search()`` is currently very limited to states in the LAUS (Local Area Unemployment Statistics) and the popular CPS employment series at a national level. Adding more popular time series is a key focus of future development.

Docs coming soon. In the meanwhile, check out the docstrings.

Installation and Setup
----------------------

Install and update using `pip`_:

.. code-block:: text

    pip install -U blsconnect

The BLS Connect module works better if you have a BLS API key; you can register for one `here`_.

A Simple Example
----------------

.. code-block:: python

    import pandas as pd
    from blsconnect import RequestBLS, bls_search

    MY_API_KEY = ""
    bls = RequestBLS(key=MY_API_KEY)

    # Get seasonally-adjusted unemployment
    # rates for Florida, Georgia, and all U.S.
    series_names = bls_search(data="U3",
                              state=["FL", "GA", "US"],
                              sa=True)
    df = bls.series(series_names,
                    start_year=2010,
                    end_year=2019)
    df.head()

About
-----

BLS Connect was created by Daniel Reeves in collaboration with `Employ America`_, a Washington D.C. based organization that seeks to promote macroeconomic policies that ensure the sustained advancement of labor market outcomes for all American workers.

.. _Employ America: https://employamerica.org/
.. _here: https://data.bls.gov/registrationEngine/
.. _pip: https://pip.pypa.io/en/stable/quickstart/
.. _check out the docs: docs/
