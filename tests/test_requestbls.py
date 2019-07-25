# -*- coding: utf-8 -*-
import pytest
import os
import sys
import json
import pandas as pd
from pandas.util.testing import assert_frame_equal
from blsconnect import RequestBLS
import datetime

current_year = datetime.datetime.now().year
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# api_key.txt has just one line for the API key and is in the parent directory.
with open(os.path.join(ROOT_DIR, '../api_key.txt')) as f:
    api_key = f.readlines()[0].strip()
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@pytest.mark.parametrize(
    'key_bool, start_year, end_year, expected_groupings', [
    (True, 2000, 2010, [(2000, 2010)]),
    (True, 1950, 1980, [(1961, 1980),(1950, 1960)]),
    (False, 1950, 1980, [(1971, 1980),(1961, 1970),(1951, 1960),(1950, 1950)])
])
def test_year_groups(key_bool, start_year, end_year, expected_groupings):
    c = RequestBLS(api_key) if key_bool else RequestBLS()
    assert c._year_groups(start_year, end_year) == expected_groupings

@pytest.mark.parametrize(
    'key_bool, start_year, end_year, expected_year_tuple', [
    (True, None, None, (current_year-19, current_year)),
    (True, None, 2003, (1984, 2003)),
    (True, 1986, None, (1986, 2005)),
    (False, None, None, (current_year-9, current_year)),
    (False, None, 2003, (1994, 2003)),
    (False, 1986, None, (1986, 1995))
])
def test_year_handler(key_bool, start_year, end_year, expected_year_tuple):
    c = RequestBLS(api_key) if key_bool else RequestBLS()
    assert c._year_handler(start_year, end_year) == expected_year_tuple

@pytest.mark.parametrize(
    'json_file, args', [
    ('u3_2009.json', (['LNS14000000'], 2009, 2009)),
    ('cpi_1999-2000.json', (['CUSR0000SA0L1E', 'CUUR0000SA0L1E'], 1999, 2000))
])
def test_request_series(json_file, args):
    with open(os.path.join(ROOT_DIR, f'static/{json_file}')) as f:
        benchmark = json.load(f)
    req = RequestBLS(api_key)._request(*args)
    assert req.json()['Results'] == benchmark['Results']

@pytest.mark.parametrize(
    'json_file, pickle_file, args', [
    ('u3_2009.json', 'u3_2009.pickle', ('wide', True)),
    ('cpi_1999-2000.json', 'cpi_1999-2000_long.pickle', ('long', False)),
    ('cpi_1999-2000.json', 'cpi_1999-2000_wide.pickle', ('wide', False))
])
def test_tablefy(json_file, pickle_file, args):
    benchmark = pd.read_pickle(os.path.join(ROOT_DIR, f'static/{pickle_file}'))
    with open(os.path.join(ROOT_DIR, f'static/{json_file}')) as f:
        json_data = f.readlines()[0]
    c = RequestBLS()
    df = c._cleanup_df(c._tablefy(json_data, *args), args[0])
    assert_frame_equal(df, benchmark)