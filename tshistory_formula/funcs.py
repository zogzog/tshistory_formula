from datetime import timedelta, datetime
from typing import Union, Optional, Tuple
from numbers import Number
import calendar
from functools import reduce
import operator

import numpy as np
import pandas as pd
import pytz
from dateutil.relativedelta import relativedelta

from psyl.lisp import (
    buildargs,
    Symbol
)
from tshistory.util import (
    compatible_date,
    empty_series,
    ensuretz,
    patchmany,
    tzaware_serie
)
from tshistory_formula.registry import (
    finder,
    func,
    history,
    insertion_dates,
    metadata,
    argscope
)
from tshistory_formula.helper import (
    NONETYPE,
    seriesname
)
from tshistory_formula.interpreter import Interpreter


@func('options')
def options(series: pd.Series,
            fill: Union[str, Number, NONETYPE]=None,
            limit: Optional[int]=None,
            weight: Optional[Number]=None) -> pd.Series:
    """
    The `options` operator takes a series and three keywords to modify
    the behaviour of series.

    * `fill` to specify a filling policy to avoid `nans` when the
      series will be `add`ed with others; accepted values are
      `"ffill"` (forward-fill), `"bfill"` (backward-fill) or any
      floating value.

    * `limit`: if `fill` is specified, this is the maximum number of
      consecutive NaN values to forward/backward fill. In other words,
      if there is a gap with more than this number of consecutive
      NaNs, it will only be partially filled. If `fill` is not
      specified, this is the maximum number of entries along the
      entire axis where NaNs will be filled. Must be greater than 0 if
      not None.

    * `weight` to provide a weight (float) value to be used by other
      operators like e.g. `row-mean`

    The `fill`, `limit` and `weight` options are put on the series object for
    later use.

    """
    series.options = {
        'fill': fill,
        'limit': limit
    }

    if weight is not None:
        series.options['weight'] = weight

    return series


def _normalize_dates(dates):
    if (all(d.tzinfo is None for d in dates) or
        all(d.tzinfo is not None for d in dates)):
        return dates

    for d in dates:
        if d.tzinfo:
            # we know there must be one
            tzone = d.tzinfo.zone
    return [
        d.tz_localize(tzone) if not d.tzinfo else d
        for d in dates
    ]


@func('min')
def scalar_min(*args: Number) -> Number:
    args = list(filter(None, args))
    if args and  isinstance(args[0], datetime):
        args = _normalize_dates(args)
    return min(args)


@func('max')
def scalar_max(*args: Number) -> Number:
    args = list(filter(None, args))
    if args and  isinstance(args[0], datetime):
        args = _normalize_dates(args)
    return max(args)


@func('series', auto=True)
def series(__interpreter__,
           __from_value_date__,
           __to_value_date__,
           __revision_date__,
           name: seriesname,
           fill: Union[str, Number, NONETYPE]=None,
           limit: Optional[int]=None,
           weight: Optional[Number]=None) -> pd.Series:
    """
    The `series` operator accepts several keywords:

    * `fill` to specify a filling policy to avoid `nans` when the
      series will be `add`ed with others; accepted values are
      `"ffill"` (forward-fill), `"bfill"` (backward-fill) or any
      floating value.

    * `limit`: if `fill` is specified, this is the maximum number of
      consecutive NaN values to forward/backward fill. In other words,
      if there is a gap with more than this number of consecutive
      NaNs, it will only be partially filled. If `fill` is not
      specified, this is the maximum number of entries along the
      entire axis where NaNs will be filled. Must be greater than 0 if
      not None.

    * `weight` to provide a weight (float) value to be used by other
      operators like e.g. `row-mean`

    For instance in `(add (series "a" #:fill 0) (series "b")` will
    make sure that series `a`, if shorter than series `b` will get
    zeroes instead of nans where `b` provides values.

    """
    i = __interpreter__
    exists = i.tsh.exists(i.cn, name)
    if not exists:
        if i.tsh.othersources and i.tsh.othersources.exists(name):
            exists = True

    if not exists:
        raise ValueError(f'No such series `{name}`')

    meta = i.tsh.metadata(i.cn, name)
    if meta is None:
        meta = i.tsh.othersources.metadata(name)
    tzaware = meta['tzaware']

    args = {
        'from_value_date': __from_value_date__,
        'to_value_date': __to_value_date__,
        'revision_date': __revision_date__
    }

    ts = i.get(name, args)
    # NOTE: we are cutting there now, but we shouldn't have to
    # the issue lies in the "optimized" way histories are computed:
    # the history interpreter needs a cut there
    ts = ts.loc[
        compatible_date(tzaware, __from_value_date__):
        compatible_date(tzaware, __to_value_date__)
    ]
    ts.options = {
        'fill': fill,
        'limit': limit
    }
    if weight is not None:
        ts.options['weight'] = weight

    return ts


@metadata('series')
def series_metas(cn, tsh, stree):
    name = stree[1]
    meta = tsh.metadata(cn, name)
    # alt sources lookup
    if meta is None and tsh.othersources:
        meta = tsh.othersources.metadata(name)
    return {name: meta}


@finder('series')
def series_finder(cn, tsh, stree):
    name = stree[1]
    return {name: stree}


def asof_transform(tree):
    posargs, _kwargs = buildargs(tree[1:])
    return [
        Symbol('let'), Symbol('revision_date'), posargs[0], tree
    ]


@func('asof')
@argscope('asof', asof_transform)
def asof(revision_date: pd.Timestamp,
         series: pd.Series) -> pd.Series:
    """
    Fetch the series in the asof scope with the specified revision date.

    Example: `(asof (shifted (today) #:days -1) (series "i-have-many-versions"))`

    """
    return series


@func('tzaware-stamp')
def tzaware_date(dt: pd.Timestamp, tzone: str) -> pd.Timestamp:
    if dt is None:
        return
    if dt.tzinfo is None:
        return pd.Timestamp(dt, tz=tzone)
    # not naive, we don't touch it
    return dt.tz_convert(tzone)


def dedupe(series):
    if series.index.duplicated().any():
        return series.groupby(series.index).mean()
    return series


def naive_transform(tree):
    posargs, _kwargs = buildargs(tree[1:])
    tzone = posargs[-1]

    top = [Symbol('let')]
    for name in ('from_value_date', 'to_value_date'):
        top += [Symbol(name),
                [Symbol('tzaware-stamp'), Symbol(name), tzone]]

    top.append(tree)
    return top


@func('naive')
@argscope('naive', naive_transform)
def naive(series: pd.Series, tzone: str) -> pd.Series:
    """
    Allow demoting a series from a tz-aware index to a tz-naive index.

    One must provide a target timezone.

    Example: `(naive (series "tz-aware-series-from-poland") "Europe/Warsaw")`

    """
    if not len(series):
        return empty_series(False)

    if not tzaware_serie(series):
        return dedupe(series)

    series.index = series.index.tz_convert(tzone).tz_localize(None)
    return dedupe(series)


@func('date')
def timestamp(strdate: str,
              tz: Optional[str]='UTC') -> pd.Timestamp:
    """
    Produces an utc timestamp from its input string date in iso format.

    The `tz` keyword allows to specify an alternate time zone.
    The `naive` keyword forces production of a naive timestamp.
    Both `tz` and `naive` keywords are mutually exlcusive.
    """
    if tz:
        pytz.timezone(tz)
    if tz is None:
        return pd.Timestamp(strdate)
    return pd.Timestamp(strdate, tz=tz)


@func('shifted')
def shifted(date: pd.Timestamp,
            years: int=0,
            months: int=0,
            weeks: int=0,
            days: int=0,
            hours: int=0,
            minutes: int=0) -> pd.Timestamp:
    """
    Takes a timestamp and a number of years, months, weekds, days,
    hours, minutes (int) and computes a new date according to the asked
    delta elements.

    Example: `(shifted (date "2020-1-1") #:weeks 1 #:hours 2)`

    """
    if date is None:
        return None
    return date + relativedelta(
        years=years,
        months=months,
        weeks=weeks,
        days=days,
        hours=hours,
        minutes=minutes
    )


@func('today')
def today(__interpreter__,
          naive: Optional[bool]=False,
          tz: Optional[str]=None) -> pd.Timestamp:
    """
    Produces a timezone-aware timestamp as of today

    The `naive` keyword forces production of a naive timestamp.
    The `tz` keyword allows to specify an alternate time zone
    (if unpecified and not naive).
    Both `tz` and `naive` keywords are mutually exlcusive.

    Example: `(today)`
    """
    # impl. note: if not naive and tz is none,
    # tz will be utc
    return __interpreter__.today(naive, tz)


@func('start-of-month')
def start_of_month(date: pd.Timestamp) -> pd.Timestamp:
    """
    Produces a timezone-aware timestamp equal to the first day of the
    given date current month.

    Example: `(start-of-month (date "1973-05-20 09:00"))`

    """
    return date.replace(day=1)


@func('end-of-month')
def end_of_month(date: pd.Timestamp) -> pd.Timestamp:
    """
    Produces a timezone-aware timestamp equal to the last day of the
    given date current month.

    Example: `(end-of-month (date "1973-05-20 09:00"))`

    """
    _, end = calendar.monthrange(date.year, date.month)
    return date.replace(day=end)


@func('constant', auto=True)
def constant(__interpreter__,
             __from_value_date__,
             __to_value_date__,
             __revision_date__,
             value: Number,
             fromdate: pd.Timestamp,
             todate: pd.Timestamp,
             freq: str,
             revdate: pd.Timestamp) -> pd.Series:
    """
    Produces a constant-valued timeseries over a pre-defined horizon
    and a given granularity and for a given revision date.

    Example: `(constant 42.5 (date "1900-1-1") (date "2039-12-31") "D" (date "1900-1-1"))`

    This will yield a daily series of value 42.5 between 1900 and
    2040, dated from 1900.

    """
    assert fromdate.tzinfo is not None
    assert todate.tzinfo is not None
    assert revdate.tzinfo is not None

    return _constant(__interpreter__,
                     {'revision_date': __revision_date__,
                      'from_value_date': __from_value_date__,
                      'to_value_date': __to_value_date__},
                     value, fromdate, todate, freq, revdate)


def _constant(__interpreter__, args, value, fromdate, todate, freq, revdate):
    getargs = __interpreter__.getargs
    qrevdate = args.get('revision_date')
    if qrevdate and ensuretz(qrevdate) < revdate:
        return empty_series(True)

    qfromidate = getargs.get('from_insertion_date')
    if qfromidate and ensuretz(qfromidate) > revdate:
        return empty_series(True)

    qtoidate = getargs.get('to_insertion_date')
    if qtoidate and ensuretz(qtoidate) < revdate:
        return empty_series(True)

    mindate = args.get('from_value_date')
    if mindate:
        mindate = ensuretz(mindate)

    maxdate = args.get('to_value_date')
    if maxdate:
        maxdate = ensuretz(maxdate)

    dates = pd.date_range(
        start=fromdate,
        end=todate,
        freq=freq
    )

    return pd.Series(
        [value] * len(dates),
        name='constant',
        index=dates,
        dtype='float64'
    ).loc[mindate:maxdate]


@metadata('constant')
def metadata(cn, tsh, tree):
    return {
        'constant': {
        'tzaware': True,
        'index_type': 'datetime64[ns, UTC]',
        'value_type': 'float64',
        'index_dtype': '|M8[ns]',
        'value_dtype': '<f8'
        }
    }


@history('constant')
def constant_history(__interpreter__, value, fromdate, todate, freq, revdate):
    series = _constant(
        __interpreter__, __interpreter__.getargs, value, fromdate, todate, freq, revdate
    )
    if len(series):
        return {
            revdate: series
        }
    return {}


@insertion_dates('constant')
def constant_idates(cn, tsh, tree,
                    from_insertion_date=None, to_insertion_date=None,
                    from_value_date=None, to_value_date=None):
    itrp = Interpreter(cn, tsh, {})
    revdate = itrp.evaluate(tree[-1])
    if (from_insertion_date and
        from_insertion_date >= revdate or
        to_insertion_date and
        to_insertion_date <= revdate):
        return []
    return [revdate]


@func('+')
def scalar_add(
        num: Number,
        num_or_series: Union[Number, pd.Series]) -> Union[Number, pd.Series]:
    """
    Add a constant quantity to a series.

    Example: `(+ 42 (series "i-feel-undervalued"))`
    """
    opts = None
    if isinstance(num_or_series, pd.Series):
        assert isinstance(num, (int, float))
        opts = num_or_series.options

    res = num + num_or_series
    if opts is not None:
        # we did a series + scalar and want to propagate
        # the original series options
        res.options = opts
    return res


@func('*')
def scalar_prod(
        num: Number,
        num_or_series: Union[Number, pd.Series]) -> Union[Number, pd.Series]:
    """
    Performs a scalar product on a series.

    Example: `(* -1 (series "positive-things"))`
    """
    opts = None
    if isinstance(num_or_series, pd.Series):
        assert isinstance(num, (int, float))
        opts = num_or_series.options

    res = num * num_or_series
    if opts is not None:
        # we did a series * scalar and want to propagate
        # the original series options
        res.options = opts
    return res


@func('/')
def scalar_div(
        num_or_series: Union[Number, pd.Series],
        num: Number) -> Union[Number, pd.Series]:
    """
    Perform a scalar division between numbers or a series and a scalar.

    Example: `(/ (series "div-me") (/ 3 2))`
    """
    opts = None
    if isinstance(num_or_series, pd.Series):
        assert isinstance(num, (int, float))
        opts = num_or_series.options

    res = num_or_series / num
    if opts is not None:
        res.options = opts
    return res


@func('**')
def scalar_pow(
        series: pd.Series,
        num: Number) -> pd.Series:
    """
    Performs an exponential power on a series.

    Example: `(** (series "positive-things") 2)`
    """
    opts = series.options

    res = series ** num
    res.options = opts
    return res


def _fill(df, colname, fillopt):
    """ in-place application of the series fill policies
    which can be a int/float or a coma separated string
    like e.g. 'ffill,bfill'
    """
    filler = fillopt['fill']
    limit = fillopt.get('limit')
    if isinstance(filler, str):
        for method in filler.split(','):
            df[colname] = df[colname].fillna(
                method=method.strip(),
                limit=limit
            )
    elif isinstance(filler, (int, float)):
        df[colname] = df[colname].fillna(
            value=filler,
            limit=limit
        )


def _group_series(*serieslist):
    dfs = []
    opts = {}

    # join everything
    for idx, ts in enumerate(serieslist):
        if ts.options.get('fill') is None and not len(ts):
            # at least one series without fill policy and no data
            # entails an empty result
            return pd.DataFrame(dtype='float64')

        ts.name = f'{idx}'  # do something unique
        opts[ts.name] = ts.options
        dfs.append(ts)

    df = pd.concat(dfs, axis=1, join='outer')

    # apply the filling rules
    for name, fillopt in opts.items():
        if fillopt:
            _fill(df, name, fillopt)

    return df


@func('add')
def series_add(*serieslist: pd.Series) -> pd.Series:
    """
    Linear combination of two or more series. Takes a variable number
    of series as input.

    Example: `(add (series "wallonie") (series "bruxelles") (series "flandres"))`

    To specify the behaviour of the `add` operation in the face of
    missing data, the series can be built with the `fill`
    keyword. This option is only really applied when several series
    are combined. By default, if an input series has missing values
    for a given time stamp, the resulting series has no value for this
    timestamp (unless a fill rule is provided).

    """
    assert [
        isinstance(s, pd.Series)
        for s in serieslist
    ]

    return _group_series(*serieslist).dropna().sum(axis=1)


@func('mul')
def series_multiply(*serieslist: pd.Series) -> pd.Series:
    """
    Element wise multiplication of series. Takes a variable number of
    series as input.

    Example: `(mul (series "banana-spot-price ($)") (series "$-to-€" #:fill 'ffill'))`

    This might convert a series priced in dollars to a series priced
    in euros, using a currency exchange rate series with a
    forward-fill option.
    """
    df = _group_series(*serieslist)
    if not len(df):
        return empty_series(
            tzaware_serie(serieslist[0])
        )

    res = None
    for col in df.columns:
        if res is None:
            res = df[col].to_frame()
            continue
        res = res.multiply(df[col], axis=0)

    return res[res.columns[0]].dropna()


@func('div')
def series_div(s1: pd.Series, s2: pd.Series) -> pd.Series:
    """
    Element wise division of two series.

    Example: `(div (series "$-to-€") (series "€-to-£"))`
    """
    df = _group_series(*(s1, s2))
    if not len(df) or len(df.columns) < 2:
        return empty_series(
            tzaware_serie(s1)
        )

    c1, c2 = df.columns
    return (df[c1] / df[c2]).dropna()


@func('priority')
def series_priority(*serieslist: pd.Series) -> pd.Series:
    """
    The priority operator combines its input series as layers. For
    each timestamp in the union of all series time stamps, the value
    comes from the first series that provides a value.

    Example: `(priority (series "realized") (series "nominated") (series "forecasted"))`

    Here `realized` values show up first, and any missing values come
    from `nominated` first and then only from `forecasted`.
    """
    if len(serieslist) == 1:
        return serieslist[0]

    series = list(serieslist)
    series.reverse()
    return patchmany(series)


@func('clip')
def series_clip(series: pd.Series,
                min: Optional[Number]=None,
                max: Optional[Number]=None,
                replacemin: Optional[bool]=False,
                replacemax: Optional[bool]=False) -> pd.Series:
    """
    Set an upper/lower threshold for a series. Takes a series as
    positional parameter and accepts four optional keywords `min` and
    `max` which must be numbers, `replacemin` and
    `replacemax` to control filling out of bounds data with min and
    max respectively.

    Example: `(clip (series "must-be-positive") #:min 0 #:replacemin #t)`

    """
    options = series.options.copy()
    if max is not None:
        mask = series <= max
        if replacemax:
            series[~mask] = max
        else:
            series = series[mask]
    if min is not None:
        mask = series >= min
        if replacemin:
            series[~mask] = min
        else:
            series = series[mask]
    series.options = options
    return series


def slice_transform(tree):
    _posargs, kwargs = buildargs(tree[1:])
    qargs = {}
    for treeparam, targetparam in (
            ('fromdate', 'from_value_date'),
            ('todate', 'to_value_date')
    ):
        if treeparam in kwargs:
            qargs[targetparam] = kwargs[treeparam]

    if not qargs:
        # nothing to transform
        return tree

    argfunc = {
        'from_value_date': 'max',
        'to_value_date': 'min',
        'revision_date': None
    }
    top = [Symbol('let')]
    for name, value in qargs.items():
        func = argfunc[name]
        top += [Symbol(name),
                [Symbol(func), Symbol(name), value] if func else value]

    top.append(tree)
    return top


@func('slice')
@argscope('slice', slice_transform)
def slice(series: pd.Series,
          fromdate: Optional[pd.Timestamp]=None,
          todate: Optional[pd.Timestamp]=None) -> pd.Series:
    """
    This allows cutting a series at date points. It takes one
    positional parameter (the series) and two optional keywords
    `fromdate` and `todate` which must be strings in the
    iso8601 format.

    Example: `(slice (series "cut-me") #:fromdate (date "2018-01-01"))`
    """
    if not len(series):
        return series

    if fromdate is None and todate is None:
        return series

    # the series operator did request with fromdate/todate
    # because of our `scope` hint
    # hence we a little to do
    return series


@func('row-mean')
def row_mean(*serieslist: pd.Series, skipna: Optional[bool]=True) -> pd.Series:
    """
    This operator computes the row-wise mean of its input series using
    the series `weight` option if present. The missing points are
    handled as if the whole series were absent.

    Example: `(row-mean (series "station0") (series "station1" #:weight 2) (series "station2"))`

    Weights are provided as a keyword to `series`. No weight is
    interpreted as 1.
    """
    weights = [
        series.options.get('weight', 1)
        for series in serieslist
    ]

    allseries = pd.concat(serieslist, axis=1)
    weights_in_vertical_matrix = np.array(
        [[w] for w in weights]
    )
    if skipna:
        weighted_sum = allseries.fillna(0).values.dot(
            weights_in_vertical_matrix
        )
    else:
        weighted_sum = allseries.values.dot(
            weights_in_vertical_matrix
        )
    denominator = (~allseries.isnull()).values.dot(
        weights_in_vertical_matrix
    )

    return pd.Series(
        (weighted_sum / denominator).flatten(),
        index=allseries.index
    ).dropna()


@func('row-min')
def row_min(*serieslist: pd.Series, skipna: Optional[bool]=True) -> pd.Series:
    """
    Computes the row-wise minimum of its input series.

    Example: `(row-min (series "station0") (series "station1") (series "station2"))`
    Example: `(row-min (series "station0") (series "station1") #:skipna #f)`

    The `skipna` keyword (which is true by default) controls the
    behaviour with nan values.
    """
    allseries = pd.concat(serieslist, axis=1)
    return allseries.min(axis=1, skipna=skipna).dropna()


@func('row-max')
def row_max(*serieslist: pd.Series, skipna: Optional[bool]=True) -> pd.Series:
    """
    Computes the row-wise maximum of its input series.

    Example: `(row-max (series "station0") (series "station1") (series "station2"))`
    Example: `(row-max (series "station0") (series "station1") #:skipna #f)`

    The `skipna` keyword (which is true by default) controls the
    behaviour with nan values.
    """
    allseries = pd.concat(serieslist, axis=1)
    return allseries.max(axis=1, skipna=skipna).dropna()


@func('std')
def row_std(*serieslist: pd.Series, skipna: Optional[bool]=True) -> pd.Series:
    """
    Computes the standard deviation over its input series.

    Example: `(std (series "station0") (series "station1") (series "station2"))`
    Example: `(std (series "station0") (series "station1") #:skipna #f)`

    The `skipna` keyword (which is true by default) controls the
    behaviour with nan values.
    """
    allseries = pd.concat(serieslist, axis=1)
    return allseries.std(axis=1, skipna=skipna).dropna()


@func('resample')
def resample(series: pd.Series,
             freq: str,
             method: str='mean') -> pd.Series:
    """
    Resamples its input series using `freq` and the aggregation method
    `method` (as described in the pandas documentation).

    Example: `(resample (series "hourly") "D")`

    """
    if not len(series):
        return series

    resampled = series.resample(freq)

    # check method
    meth = getattr(resampled, method, None)
    if meth is None:
        raise ValueError(f'bad resampling method `{method}`')

    return resampled.apply(method).dropna()


@func('cumsum')
def cumsum(series: pd.Series) -> pd.Series:
    """
    Return cumulative sum over a series.

    Example: `(cumsum (series "sum-me"))`

    """

    return series.cumsum()


def time_shifted_transform(tree):
    _posargs, kwargs = buildargs(tree[1:])

    if not kwargs:
        # the signature of time-shifted allows a no-kw
        # call (meaningless but possible)
        return tree[1]

    def negate(items):
        return [
            -x if isinstance(x, (int, float)) else x
            for x in items
        ]

    top = [Symbol('let')]
    for name in ('from_value_date', 'to_value_date'):
        top += [
            Symbol(name),
            [Symbol('shifted'), Symbol(name)] +
            negate(
                reduce(operator.add, kwargs.items())
            )
        ]

    top.append(tree)
    return top


@func('time-shifted')
@argscope('time-shifted', time_shifted_transform)
def time_shifted(series: pd.Series,
                 weeks: int=0,
                 days: int=0,
                 hours: int=0,
                 minutes: int=0) -> pd.Series:
    """
    Shift the dates of a series.

    Takes the following keywords: `weeks`, `days`, `hours`, with
    positive or negative values.

    Example `(time-shifted (series "shifted") #:days 2 #:hours 7)`

    """
    # note: relativedelta is unfit there as it cannot be
    # broadcast on the index
    series.index = series.index + timedelta(
        weeks=weeks,
        days=days,
        hours=hours,
        minutes=minutes
    )

    return series


@func('rolling')
def rolling(series: pd.Series,
            window: int,
            method: str='mean') -> pd.Series:
    """
    Computes a calculation `method` (mean by default) to a rolling
    window (as described in the pandas documentation).

    Example: `(rolling (series "foo") 30 #:method "median"))`

    """
    if not len(series):
        return series

    rolled = series.rolling(window)
    df = rolled.agg((method,)).dropna()
    return df[df.columns[0]]
