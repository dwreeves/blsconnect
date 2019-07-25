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

class RequestBLS(object):
    """The RequestBLS class stores an API key, which is used to get data from the BLS API with the
    .series() method.
    
    If the BLS API key is undefined by the user, the API year limit is set to 10 years instead of
    20 years. The user can always define a date range that exceeds the year limit; the requests
    are pulled in chunks and returned in a single DataFrame.
    
    :param key: BLS API key.
    :param msg_log_level: What level to log messages returned from an API request. Default level
                          is WARNING.
    """
    
    def __init__(self, key:str=None, msg_log_level:int=logging.WARNING):
        # Setup logging stuff
        self.logger = logging.getLogger(__name__)
        if isinstance(msg_log_level, str):
            self.msg_log_level = logging.getLevelName(msg_log_level)
        else:
            self.msg_log_level = msg_log_level
        
        # Setup key
        self.key = key
        if key is None:
            self.api_year_limit = 10
        else:
            self.api_year_limit = 20

    def series(
        self,
        series,
        start_year:int=None,
        end_year:int=None,
        shape:str='wide',
        keep_footnotes:bool=False,
        rtn_msg:bool=False
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
        :param rtn_msg: Returns messages alongside the DataFrame.
        :returns: DataFrame, or (DataFrame, [messages]) if :param:`rtn_msg` is True.
        """
        
        # Handle user inputs
        if shape not in ['wide', 'long']:
            raise InputError('shape kwarg must be either "wide" or "long".')
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
            r[s_y] = self._request(series, s_y, e_y)
            next_df = self._tablefy(r[s_y].content, shape, keep_footnotes)
            df = df.append(next_df)
        df = self._cleanup_df(df, shape)
        
        # Return
        if rtn_msg:
            messages = []
            for yr in r:
                for msg in r[yr].json()['message']:
                    messages.append(msg)
            return df, messages
        else:
            return df
    
    def _year_handler(self, start_year, end_year):
        """Handles the user input for years with the following logic:
        
        - If both are ``None``: Pulls last 20 (or 10 if ``self.key=None``) years of data.
        - If only start_year is ``None``: Pulls 20 (or 10) years starting in start_year.
        - If only end_year is ``None``: Pulls 20 (or 10) years up to and including end_year.
        
        :param start_year: Earliest year of data to pull.
        :param end_year: Latest year of data to pull.
        :returns: Tuple of adjusted years.
        """
        if start_year is None or end_year is None:
            if start_year is None and end_year is None:
                import datetime
                end_year = datetime.datetime.now().year
            if start_year is None:
                start_year = int(end_year) - (self.api_year_limit-1)
            if end_year is None:
                end_year = int(start_year) + (self.api_year_limit-1)
        if start_year > end_year:
            self.logger.warning(f'End year should be less than or equal to end year. '
                                f'User input: (start_year={start_year}, end_year={end_year}). '
                                'These values were flipped to continue the request.')
            start_year, end_year = end_year, start_year
        return start_year, end_year

    def _request(self, series:list, start_year:int, end_year:int):
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
        r = requests.post(BLS_BASE_URL,
                          data=json.dumps(post_data),
                          headers={'Content-type': 'application/json'})
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

    
    # def _create_date(self, df):
        # """Handles the logic of setting up a date column. It's here to keep the
        # _tablefy() method a bit cleaner."""
        # period_type = df['period'][0][0] # first char of period
        # if period_type == 'A':
            # df = df.assign(date=pd.to_datetime(df['year'])) \
                # .set_index('date') \
                # .to_period(freq='A-JAN')
        # if period_type == 'S':
            # p = df['year'].str.cat(df['period'].str.replace('', 'Q')))
            # df = df.assign(date=pd.to_datetime(p) \
                # .set_index('date').to_period(freq='A-JAN')
        # if period_type == 'Q':
            # p = df['year'].str.cat(df['period'].str.replace('Q0', 'Q')))
            # df = df.assign(date=pd.to_datetime(p) \
                # .set_index('date').to_period(freq='A-JAN')
        
        # return pd.to_datetime(,format='%Y%m%d')
    
    # def collect_cpi_data(msa=):
        # pass