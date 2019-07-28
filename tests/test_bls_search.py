# -*- coding: utf-8 -*-
import pytest
from blsconnect import bls_search

kwargs_input = {}
output = {}

kwargs_input[0] =  {'data' : 'ur'}
output[0] = {(('data', 'ur'),): 'LNS14000000'}

kwargs_input[1] = {'data' : 'cpi-food-energy', 'sa' : [True, False]}
output[1] = {(('sa', True),): 'CUSR0000SA0L1E', (('sa', False),): 'CUUR0000SA0L1E'}

# Test whether it correct reverts to non seasonally adjusted even when not specified.
kwargs_input[2] = {'data' : 'cpi', 'region' : ['northeast', 'midwest', 'south', 'west']}
output[2] = {
    (('region', 'northeast'),): 'CUUR0100SA0',
    (('region', 'midwest'),): 'CUUR0200SA0',
    (('region', 'south'),): 'CUUR0300SA0',
    (('region', 'west'),): 'CUUR0400SA0'
}

kwargs_input[3] = {'data' : ['cpi-food-energy', 'ur'], 'return_type' : 'list'}
output[3] = ['CUSR0000SA0L1E', 'LNS14000000']


@pytest.mark.parametrize(
    'kwargs, expected_output',
    [(kwargs_input[i], output[i]) for i in range(len(kwargs_input))]
)
def test_bls_search(kwargs, expected_output):
    assert bls_search(**kwargs) == expected_output