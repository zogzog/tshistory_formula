import pandas as pd
from tshistory.testutil import (
    assert_df,
    assert_hist
)

from tshistory_formula.tsio import timeseries


def test_local_formula_remote_series(mapi):
    rtsh = timeseries('test-mapi-2')
    rtsh.update(
        mapi.engine,
        pd.Series(
            [1, 2, 3],
            index=pd.date_range(pd.Timestamp('2020-1-1'), periods=3, freq='H'),
        ),
        'remote-series',
        'Babar',
        insertion_date=pd.Timestamp('2020-1-1', tz='UTC')
    )

    mapi.register_formula(
        'test-localformula-remoteseries',
        '(+ 1 (series "remote-series"))'
    )

    ts = mapi.get('test-localformula-remoteseries')
    assert_df("""
2020-01-01 00:00:00    2.0
2020-01-01 01:00:00    3.0
2020-01-01 02:00:00    4.0
""", ts)

    hist = mapi.history('test-localformula-remoteseries')
    assert_hist("""
insertion_date             value_date         
2020-01-01 00:00:00+00:00  2020-01-01 00:00:00    2.0
                           2020-01-01 01:00:00    3.0
                           2020-01-01 02:00:00    4.0
""", hist)

    f = mapi.formula('test-localformula-remoteseries')
    assert f == '(+ 1 (series "remote-series"))'

    none = mapi.formula('nosuchformula')
    assert none is None

    # altsource formula
    rtsh.register_formula(
        mapi.engine,
        'remote-formula-remote-series',
        '(+ 2 (series "remote-series"))'
    )
    f = mapi.formula('remote-formula-remote-series')
    assert f == '(+ 2 (series "remote-series"))'

    assert_df("""
2020-01-01 00:00:00    3.0
2020-01-01 01:00:00    4.0
2020-01-01 02:00:00    5.0
""", mapi.get('remote-formula-remote-series'))
