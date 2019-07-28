# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd
import logging
import urllib.request
import certifi
from functools import lru_cache

# API instructions:
# https://www.bls.gov/developers/api_signature_v2.htm
#
# BLS data browser:
# https://www.bls.gov/data/

BLS_BASE_URL = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

class InputError(Exception):
    pass

class RequestBLS(object):
    """The RequestBLS class stores an API key, which is used to get data from the BLS API with the
    .series() method.
    
    If the BLS API key is undefined by the user, the API year limit is set to 10 years instead of
    20 years. In addition, you will be unable to use the catalog feature. The user can always
    define a date range that exceeds the year limit; the requests are pulled in chunks and returned
    in a single DataFrame.
    
    :param key: BLS API key.
    :param msg_log_level: What level to log messages returned from an API request. Default level
                          is WARNING.
    :param start_year: Default start_year for series(). It's generally better to set this in the
                       .series() method instead of at the class level.
    :param end_year: Default end_year for series(). It's generally better to set this in the
                     .series() method instead of at the class level.
    :attr messages: Returns messages from last time .series() was run.
    :attr catalog: Returns data catalog from last time .series() was run. Only available if API
                   key is set.
    """
    
    def __init__(self, key:str=None, msg_log_level:int=logging.WARNING, start_year:int=None,
                 end_year:int=None):
        # Setup logging stuff
        self.logger = logging.getLogger(__name__)
        if isinstance(msg_log_level, str):
            self.msg_log_level = logging.getLevelName(msg_log_level)
        else:
            self.msg_log_level = msg_log_level
        
        # Setup key
        self.key = key
        self.api_year_limit = 10 if not key else 20

        # Setup other
        self.start_year = start_year
        self.end_year = end_year
        self.messages = []
        self._catalog = {}
    
    def series(
        self,
        series,
        start_year:int=None,
        end_year:int=None,
        shape:str='wide',
        keep_footnotes:bool=False,
        catalog:bool=True,
        rtn_msg:bool=False,
        interpolate:str=None,
        groupby:str=None, # Does not work right now
        groupby_method:str='mean' # Does not work right now
    ):
        """Get a data series from the BLS API by Series ID for a given date range.
        
        You can find a data series ID with the `BLS Data Finder`_ website or using the
        :func:`bls_search()` function from :file:`search.py`.
        
        .. _BLS Data Finder: https://www.bls.gov/bls/data_finder.htm
        
        :param series: Series ID of series to request. You can input either a single string, a list
                       of Series ID's, or a dict where the .values() form a list of Series ID's.
        :param start_year: Earliest year of data to pull. See :meth:`_year_handler()` for more on
                           how undefined years are handled.
        :param end_year: Latest year of data to pull. See :meth:`_year_handler()` for more on how
                         undefined years are handled.
        :param shape: The shape of the data, which can be either 'long' or 'wide'. ``'long'``
                      appends all series to DataFrame; ``'wide'`` merges all series to DataFrame.
        :param keep_footnotes: If True, keeps the Footnotes field from the data. Otherwise this
                               field is dropped.
        :param catalog: Grabs the catalog from the API call and stores it in self._catalog. Only
                        available if API key is set.
        :param interpolate: Fills in missing values. This just passes a string to df.interpolate().
                            Notably, this occurs before groupby, which is why you might want to
                            specify this in .series() instead of with the returned DataFrame.
        :param groupby: Collapses data according to some frequency. Valid inputs are 'y', 's', 'q',
                        'm'.
        :param groupby_method: How to collapse data if it will be collapsed. 'mean' is the default
                               method. Can also do 'first', 'last', 'min', and 'max'.
        :returns: DataFrame
        """
        
        self.messages = []
        self._catalog = {}
        
        # Handle user inputs
        # There is a lot of LBYL instead of EAFP to avoid eating up unnecessary API calls.
        if shape not in ['wide', 'long']:
            raise InputError('shape kwarg must be either "wide" or "long".')
        if interpolate:
            pd.DataFrame({'a' : [0]}) \
                .interpolate(method=interpolate) # raises error if interpolate method is invalid.
        start_year, end_year = self._year_handler(start_year, end_year)
        if isinstance(series, dict):
            series = list(series.values())
        if not isinstance(series, list):
            series = [series]
        if keep_footnotes and shape == 'wide' and len(series) > 1:
            raise InputError('Cannot return footnotes with more than 1 series in "wide" format. '
                             'Set shape="long" or set keep_footnotes=False.')
        
        # Get data, put in DataFrame
        df = pd.DataFrame()
        r = {}
        for s_y, e_y in self._year_groups(start_year, end_year):
            r[s_y] = self._request(series, s_y, e_y, catalog)
            next_df = self._tablefy(r[s_y].content, shape, keep_footnotes)
            df = df.append(next_df)
        df = self._cleanup_df(df, shape)
        
        # Transform data
        if interpolate:
            if shape == 'wide':
                df = df.interpolate(method=interpolate)
            if shape == 'long':
                df = df.groupby('value') \
                    .apply(lambda group: group.interpolate(method=interpolate))
        if groupby:
            df = self._group(df, series, shape, groupby, groupby_method)
        
        # Return
        if self.key and catalog:
            self._catalog = {
                s['seriesID'] : s['catalog']
                for s in r[start_year].json()['Results']['series']
            }
        return df
    
    @property
    def catalog(self):
        """Returns self._catalog if self.key is defined; otherwise yell at the user for not setting
        a key, which is required for this specific functionality.
        """
        if not self.key and not self._catalog:
            raise AttributeError('Catalog is not available without a key.')
        return self._catalog
    
    def _year_handler(self, start_year, end_year):
        """Handles the user input for years with the following logic:
        
        - If both are ``None``: Pulls last 20 (or 10 if ``self.key=None``) years of data.
        - If only start_year is ``None``: Pulls 20 (or 10) years starting in start_year.
        - If only end_year is ``None``: Pulls 20 (or 10) years up to and including end_year.
        
        :param start_year: Earliest year of data to pull.
        :param end_year: Latest year of data to pull.
        :returns: Tuple of adjusted years.
        """
        start_year = start_year or self.start_year or None
        end_year = end_year or self.end_year or None
        if start_year is None and end_year is None:
            import datetime
            end_year = datetime.datetime.now().year
        if start_year is None:
            start_year = int(end_year) - (self.api_year_limit-1)
        if end_year is None:
            end_year = int(start_year) + (self.api_year_limit-1)
        if start_year > end_year:
            self.logger.log(self.msg_log_level,
                f'End year should be less than or equal to end year. User input:'
                f'(start_year={start_year}, end_year={end_year}). These values were flipped to '
                'continue the request.')
            start_year, end_year = end_year, start_year
        return start_year, end_year

    def _request(self, series:list, start_year:int, end_year:int, catalog:bool):
        """This method is what actually posts requests after all the logic of what is supposed to
        be posted is sorted out.
        
        Messages in the Response are logged at the level set by :meth:`__init__()`.
        
        :param series: List of Series ID's.
        :param start_year: Earliest year of data to pull.
        :param end_year: Latest year of data to pull.
        :returns: ``requests.models.Response``
        """
        post_data = {
            'seriesid': series,
            'startyear': str(start_year),
            'endyear': str(end_year)
        }
        if self.key:
            post_data['registrationKey'] = self.key
            if catalog:
                post_data['catalog'] = catalog
        r = requests.post(BLS_BASE_URL,
                          data=json.dumps(post_data),
                          headers={'Content-type': 'application/json'})
        # Handle invalid key
        if len(r.json()['message']) > 0:
            cond = \
                r.json()['message'][0].find('Please provide a proper key') >= 0 or \
                j.json()['message'][0].find('Request could not be serviced, as the daily') >= 0
            if cond:
                raise InputError(r.json()['message'][0])
        for msg in r.json()['message']:
            self.logger.log(self.msg_log_level, msg)
        return r

    def _year_groups(self, start_year, end_year):
        """Because the API limits to 20 years (or 10 without key), you need to do some requests
        in chunks if the range is larger than 20 years. This method gives you the groups in a list.
        
        :param start_year: Earliest year of data to pull.
        :param end_year: Latest year of data to pull.
        :returns: list of (start_year, end_year) tuples.
        """
        return [
            (
                max(end_year - (i+1)*self.api_year_limit + 1, start_year), #start_year of tuple
                end_year - i*self.api_year_limit #end_year of tuple
            )
            for i in range(-(-(end_year - start_year + 1) // self.api_year_limit))
        ]
    
    def _tablefy(self, json_data, shape, keep_footnotes):
        """Turns the results of a request to the BLS API into a pandas DataFrame.
        
        :param json_data: ``Response.content`` that contains BLS data as a json string.
        :param shape: ``'wide'`` or ``'long'`` that defines the DataFrame's shape.
        :param keep_footnotes: If True, keeps the Footnotes field from the data. Otherwise this
                               field is dropped. keep_footnotes cannot be 
        :returns: pandas DataFrame.
        """
        
        # set up blank df
        cols = ['year', 'period', 'periodName']
        if shape == 'long':
            cols = ['seriesID'] + cols
        if keep_footnotes:
            cols.append('footnotes')
        df = pd.DataFrame(columns=cols)
        
        # add data to the df
        series_list = json.loads(json_data)['Results']['series']
        for bls_series in series_list:
            small_df = pd.DataFrame(bls_series['data'])
            if shape == 'long':
                small_df['seriesID'] = bls_series['seriesID']
            small_df = small_df[cols + ['value']] #reorders cols + drops footnotes if needed
            small_df['value'] = pd.to_numeric(small_df['value'])
            small_df['year'] = pd.to_numeric(small_df['year'])
                
            if shape == 'wide':
                small_df = small_df \
                    .rename(columns={'value' : bls_series['seriesID']})
                if keep_footnotes:
                    df = small_df
                else:
                    df = df.merge(right=small_df,
                                  on=['year', 'period', 'periodName'],
                                  how='outer')
            elif shape == 'long':
                df = df.append(small_df, sort=False)
        
        # clean up columns
        if not keep_footnotes:
            df = df[[c for c in df.columns if c.lower()[:9] != 'footnotes']]
        df = df[[c for c in df.columns if c.lower()[:6] != 'latest']]
        
        return df
    
    def _cleanup_df(self, df, shape):
        """Handles the clean-up after the Pandas dataframes are all put together.
        
        :param df: pandas DataFrame with BLS data.
        :param shape: ``'wide'`` or ``'long'`` that defines the DataFrame's shape.
        :returns: pandas DataFrame.
        """    
        if shape == 'wide':
            df = df.sort_values(by=['year', 'period'])
        if shape == 'long':
            df = df.sort_values(by=['seriesID', 'year', 'period'])
        df = df.reset_index()
        df = df[[c for c in df.columns if c != 'index']]
        return df
    
    def _group(self, df, series, shape, groupby, groupby_method):
        """Handles the aggregation by a specific time period.
        
        :returns: pandas DataFrame.
        """
        
        # create new period in a dataframe
        groupby = groupby.upper()
        groupby = 'A' if groupby == 'Y' else groupby
        
        ts = pd.DataFrame()
        ts['A'] = pd.Series(['A01' for i in range(12)])
        ts['S'] = pd.Series([i//6 + 1 for i in range(12)]).apply(lambda i : 'S'+str(i).zfill(2))
        ts['Q'] = pd.Series([i//3 + 1 for i in range(12)]).apply(lambda i : 'Q'+str(i).zfill(2))
        ts['M'] = pd.Series([i+1 for i in range(12)]).apply(lambda i : 'M'+str(i).zfill(2))
        
        series1 = pd.concat([ts['A'], ts['S'], ts['Q'], ts['M']], axis=0)
        series1 = series1.rename('old_period')
        series2 = pd.concat([ts[groupby], ts[groupby], ts[groupby], ts[groupby]], axis=0)
        series2 = series2.rename('new_period')
        
        gb_df = pd.concat([series1, series2], axis=1)
        
        # add it to the BLS data
        df = df.merge(right=gb_df, how='left', left_on='period', right_on='old_period')
        if shape == 'wide':
            gb_li = ['year', 'new_period']
            li = gb_li + series
        if shape == 'long':
            gb_li = ['seriesID', 'year', 'new_period']
            li = ['seriesID', 'year', 'new_period', 'value']
        df = df.groupby(gb_li).agg(groupby_method).reset_index()
        df = df[li].rename(columns={'new_period' : 'period'})
        df = self._cleanup_df(df, shape)
        
        return df