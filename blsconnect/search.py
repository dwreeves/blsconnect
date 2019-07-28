from .data import *
            
def bls_search(return_type='short', **kwargs):
    """Takes some kwargs and returns BLS Series IDs.
    
    To see how the Series ID is determined for each set of inputs, check out the nested function,
    :meth:`_single_series_search()`.
    
    The outer function's purpose is to set up the queries that are run individually, and handle the
    output. This function lets you submit kwargs as either single values or lists of values, and
    this function will take lists and "expand" them to get all the permutations available. So if
    you specify ``sa=[True, False]``, it will return both seasonally-adjusted and not seasonally-
    adjusted series for all your data.
    
    The way the data is return can be specified with this:
    
    - ``return_type='short'`` : The key in the dict will only contain the elements that
        were in list format. So for example, if the only list you input is in the ``state`` kwarg,
        this option will make it so each key is just ``{'state'=some_state}``. If there are no
        iterable kwargs, it will behave like ``return_type='full'``.
    - ``return_type='full'`` : Returns the tupled kwargs (other than return_type) as the key,
        whether or not it was iterated over.
    - ``return_type='list'`` : Only returns the list of Series ID's without a way to identify which
        is which.
    
    :param return_type: Specifies how the data will be returned. See docstring for descriptions of
                        the valid options.
    :returns: {tuple of tuples : str}, and sometimes [str]
    
    """
    def _single_series_search(data:str=None, state:str=None, msa:str=None, region:str=None, sa:bool=None, sizeclass:str=None):
        fips = _state_to_fips(state)
        geo = 'state' if fips!='00' else 'us'
        if geo == 'state' or data[:3] == 'cpi':
            seas = 'S' if (sa or sa is None) else 'U'
        elif geo == 'us':
            seas = 'S1' if (sa or sa is None) else 'U0'
        if data[:3] == 'cpi' and len(data)>3: # handle cpi data
            if region and sa:
                raise InputError('Seasonally adjusted data does not exist for regional CPI data.')
            if sizeclass and sa:
                raise InputError('Seasonally adjusted data does not exist for size class CPI data.')
            if region or sizeclass:
                sa = False
            less = _cpi_less(data)
        else:
            less = ''
        region = CPI_REGION[region]
        sizeclass = SIZE_CLASS[sizeclass]
        data_id = _find_data_series(data=data, geo=geo)
        return data_id.format(seas=seas, fips=fips, region=region, less=less, sizeclass=sizeclass)
    
    transform = lambda d: tuple(sorted(d.items()))
    li = expand_dict_lists(kwargs)
    
    if return_type == 'list':
        return [_single_series_search(**i) for i in li]
    if isinstance(li, dict): # occurs when there are no iterable kwargs
        return {transform(li) : _single_series_search(**li)}
    if return_type == 'short':
        constants, iterables = split_dict_lists(li)
        return {transform(i) : _single_series_search(**constants, **i) for i in iterables}
    if return_type == 'full':
        return {transform(i) : _single_series_search(**i) for i in li}

def expand_dict_lists(d):
    """Neat little function that expands every list in a dict. So instead of having one dict with
    a list of 5 things + a list of 3 things, you get a list of 5x3=15 dicts with each dict
    representing each possible permutation of things.
    
    :param d: Dictionary that can contain some (or no) lists in the .values().
    :returns: List of dictionaries with the lists in the .values() broken down.
    """
    def _flatten(big_li):
        """Turns a list of lists into a single expanded list.
        """
        flat_list = []
        for sub_li in big_li:
            if isinstance(sub_li, list): # make sure we only iterate over lists
                for item in sub_li:
                    flat_list.append(item)
            else:
                flat_list.append(sub_li)
        return flat_list
    def _get_permutations(d):
        """Recursively breaks down each list in the .values() of the dict until there are no more
        lists to break down.
        """
        for key, val in d.items():
            if isinstance(val, list):
                return _flatten([
                    _get_permutations(
                        {**{key : i}, **{_k : _v for _k, _v in d.items() if _k != key}}
                    )
                    for i in val
                ])
        return d
    return _get_permutations(d)

def split_dict_lists(li):
    """Splits a list of dicts into: a dict with all constants, and a list of dicts where the values
    vary across different dicts in the input list.
    
    :param li: List of dicts.
    :returns: (dict, [dict])
    """
    def _all_same_values(li, key):
        """See if all the dicts have the same value for a given key."""
        check = None
        for d in li:
            if check is None:
                check = d[key]
            elif check != d[key]:
               return False
        return True
    
    constant_keys = {key : _all_same_values(li, key) for key in li[0].keys()}
    constants = {k:v for k,v in li[0].items() if constant_keys[k]}
    iterables = [{k:v for k,v in d.items() if not constant_keys[k]} for d in li]
    return constants, iterables

def _cpi_less(data):
    li = data.split("-")
    s = [CPI_EXCLUDE[i] for i in li]
    s.sort()
    if ('5' in s and len(s) > 1) or (s == '2E'):
        raise InputError('Invalid series name.')
    return 'L' + ''.join(s)

def _state_to_fips(state):
    """Takes a state input and returns a FIPS number for the state.
    """
    if state is None:
        return '00'
    if len(state) > 2:
        state = STATES_LONG_TO_SHORT[state.lower()]
    return str(STATE_TO_FIPS[state.upper()]).zfill(2)

def _find_data_series(data=None, geo=None):
    if data[:3] == 'cpi':
        return FORMAT_STRING[geo][SERIES_NAME_DICT['cpi']]
    return FORMAT_STRING[geo][SERIES_NAME_DICT[data.lower()]]