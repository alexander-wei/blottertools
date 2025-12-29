"""functions.py"""

import decimal
import logging
from enum import IntEnum

import pandas

from blottertools.adapters import Executor
from blottertools.decorators import df_task

# pylint: disable=missing-function-docstring,missing-class-docstring

INDEX = ["date", "lkid", "ticker"]

# Task functions


@df_task
def step1_create_pk(_df: pandas.DataFrame) -> pandas.DataFrame:
    """retain a copy of the primary key as "index" from original row order.
    This will be referenced by future steps for integrity under initial row order.
    ::

        _df.reset_index(inplace=True)
        return _df
    """
    _df.reset_index(inplace=True)
    return _df


@df_task
def step2_sector_pal_date(_df: pandas.DataFrame) -> pandas.DataFrame:
    """ETL on (sector, pal, exposure, date)
    tokenize sector
    rename sector
    cast (pal, exposure) to Decimal
    cast date to datetime from string
    ::

        logging.info(_df.dtypes)
        _df["sector"] = _df["sector"].astype(pandas.CategoricalDtype(ordered=True))
        _df["sector"] = _df["sector"].cat.rename_categories(
            {"Technology": "Information Technology"}
        )

        decimal.getcontext().prec = 28  # decimal library default precision
        _df["pal"] = _df["pal"].apply(decimal.Decimal)
        _df["exposure"] = _df["exposure"].apply(decimal.Decimal)

        _df["date"] = pandas.to_datetime(_df["date"], yearfirst=True)

        return _df
    """
    logging.info(_df.dtypes)
    _df["sector"] = _df["sector"].astype(pandas.CategoricalDtype(ordered=True))
    _df["sector"] = _df["sector"].cat.rename_categories(
        {"Technology": "Information Technology"}
    )

    decimal.getcontext().prec = 28  # decimal library default precision
    _df["pal"] = _df["pal"].apply(decimal.Decimal)
    _df["exposure"] = _df["exposure"].apply(decimal.Decimal)

    _df["date"] = pandas.to_datetime(_df["date"], yearfirst=True)

    return _df


@df_task
def step3_impute_ticker(_df: pandas.DataFrame) -> pandas.DataFrame:
    """impute ticker column if missing
    ::

        if "ticker" not in _df.columns:
            ticker_series = _df.apply(impute_ticker_symbol, axis=PandasAxisType.COLS)
            _df["ticker"] = ticker_series

        return _df
    """
    if "ticker" not in _df.columns:
        ticker_series = _df.apply(impute_ticker_symbol, axis=PandasAxisType.COLS)
        _df["ticker"] = ticker_series

    return _df


@df_task
def step4_compute_open_liq(_df, _executor: Executor) -> pandas.DataFrame:
    """compute liquidity/exposure for lkid at open of day.
    ::

        _df["open_liq"] = None

        logging.info(_df.to_markdown())

        for __, _lkid_ticker_df in _executor.groupby(["lkid", "ticker"], as_index=False):
            # reduce over (lkid, ticker) groups

            # In the case of a collision:
            # reduce (*multiple values, same lkid, same ticker, same day)
            #       -> (aggregated(*), lkid, ticker, day)
            _lkid_ticker_df = _lkid_ticker_df.groupby("date", as_index=True).agg(
                {
                    "index": first_alphabetical,
                    "analyst": first_alphabetical,
                    "sector": first_alphabetical,
                    "pal": "sum",
                    "exposure": "sum",
                },
            )

            # EOD[i] becomes Open[i+1]
            _lkid_ticker_df["open_liq"] = _lkid_ticker_df["exposure"].shift(-1, "D")

            logging.info("Shifting day to calculate net liquidity at open in each position")
            logging.info(_lkid_ticker_df.to_markdown())

            # Impute each group of consecutive days with a first open calculated by
            # eod - pal
            is_nan = _lkid_ticker_df["open_liq"].isnull()
            logging.info("null rows")
            logging.info(_lkid_ticker_df[is_nan].to_markdown())
            _lkid_ticker_df[is_nan] = _lkid_ticker_df[is_nan].apply(
                impute_first_day_open_liq, axis=PandasAxisType.COLS
            )
            logging.info("imputing first of sequence for open net liq")
            logging.info(_lkid_ticker_df.to_markdown())

            logging.info("reset index")
            # reassign pk to "index" for left join
            _lkid_ticker_df = _lkid_ticker_df.set_index("index")
            logging.info(_lkid_ticker_df.to_markdown())

            _df.update(_lkid_ticker_df)
    """

    _df["open_liq"] = None

    logging.info(_df.to_markdown())

    for __, _lkid_ticker_df in _executor.groupby(["lkid", "ticker"], as_index=False):
        # reduce over (lkid, ticker) groups

        # In the case of a collision:
        # reduce (*multiple values, same lkid, same ticker, same day)
        #       -> (aggregated(*), lkid, ticker, day)
        _lkid_ticker_df = _lkid_ticker_df.groupby("date", as_index=True).agg(
            {
                "index": first_alphabetical,
                "analyst": first_alphabetical,
                "sector": first_alphabetical,
                "pal": "sum",
                "exposure": "sum",
            },
        )

        # EOD[i] becomes Open[i+1]
        _lkid_ticker_df["open_liq"] = _lkid_ticker_df["exposure"].shift(-1, "D")

        logging.info("Shifting day to calculate net liquidity at open in each position")
        logging.info(_lkid_ticker_df.to_markdown())

        # Impute each group of consecutive days with a first open calculated by
        # eod - pal
        is_nan = _lkid_ticker_df["open_liq"].isnull()
        logging.info("null rows")
        logging.info(_lkid_ticker_df[is_nan].to_markdown())
        _lkid_ticker_df[is_nan] = _lkid_ticker_df[is_nan].apply(
            impute_first_day_open_liq, axis=PandasAxisType.COLS
        )
        logging.info("imputing first of sequence for open net liq")
        logging.info(_lkid_ticker_df.to_markdown())

        logging.info("reset index")
        # reassign pk to "index" for left join
        _lkid_ticker_df = _lkid_ticker_df.set_index("index")
        logging.info(_lkid_ticker_df.to_markdown())

        _df.update(_lkid_ticker_df)
    return _df


@df_task
def step5_compute_fund_value(_df: pandas.DataFrame, _executor) -> pandas.DataFrame:
    """compute total fund open_liq over all (lkid, ticker) for each day
    ::

        logging.info("updating")
        logging.info(_df.to_markdown())

        logging.info("getting total fund liq for each day")

        _df.set_index("index", drop=True, inplace=True)
        _df["return"] = None

        for __, _date_df in _executor.groupby("date"):
            total_daily_fund = _date_df["open_liq"].agg("sum")
            logging.info("beginning of day total fund value %f", total_daily_fund)
            _date_df["return"] = _date_df["pal"] / total_daily_fund
            logging.info("calculate return of security")
            logging.info(_date_df.to_markdown())
            logging.info("reset index")

            logging.info(_date_df.to_markdown())
            _df.update(_date_df)

        logging.info("update df")
        logging.info(_df.to_markdown())
        return _df
    """

    logging.info("updating")
    logging.info(_df.to_markdown())

    logging.info("getting total fund liq for each day")

    _df.set_index("index", drop=True, inplace=True)
    _df["return"] = None

    for __, _date_df in _executor.groupby("date"):
        total_daily_fund = _date_df["open_liq"].agg("sum")
        logging.info("beginning of day total fund value %f", total_daily_fund)
        _date_df["return"] = _date_df["pal"] / total_daily_fund
        logging.info("calculate return of security")
        logging.info(_date_df.to_markdown())
        logging.info("reset index")

        logging.info(_date_df.to_markdown())
        _df.update(_date_df)

    logging.info("update df")
    logging.info(_df.to_markdown())
    return _df


@df_task
def step6_agg_daily(_df: pandas.DataFrame) -> pandas.DataFrame:
    """compute daily returns for (lkid, date) entities
    ::

        logging.info("applying the replacement transformations and such")
        logging.info("reducing over ticker now.")

        # select * from df     left join      select * from (
        #   first(pk), first(...) from df group by (date, lkid)
        # ) as A
        #  on df.pk = A.pk
        # This requires copying the index to a column to use first(pk) with pandas.agg(...)
        _df.reset_index(drop=False, inplace=True)
        another_df = _df.groupby(["date", "lkid"], as_index=False).agg(
            {
                "analyst": first_alphabetical,
                "sector": first_alphabetical,
                "pal": "sum",
                "exposure": "sum",
                "return": "sum",
                "index": get_first_index_label,
            },
        )
        # !
        # Reassigning to pal, exposure, return    may result in an invalid state of entities
        # In practice, the join should be made to new columns.
        _df["pal"] = None
        _df["exposure"] = None
        _df["return"] = None

        # reassign pk to df.index
        _df.set_index("index", inplace=True)
        _df.update(another_df)

        # drop where null because these rows already contributed values under (sum, first) aggregations
        # on join
        _df.dropna(inplace=True)
        _df.set_index(["lkid", "date"], inplace=True)
        _df.sort_index(axis=PandasAxisType.ROWS, inplace=True)
        _df.reset_index(drop=False, inplace=True)

        logging.info(_df)
        logging.info(_df.columns)
        return _df
    """

    logging.info("applying the replacement transformations and such")
    logging.info("reducing over ticker now.")

    # select * from df     left join      select * from (
    #   first(pk), first(...) from df group by (date, lkid)
    # ) as A
    #  on df.pk = A.pk
    # This requires copying the index to a column to use first(pk) with pandas.agg(...)
    _df.reset_index(drop=False, inplace=True)
    another_df = _df.groupby(["date", "lkid"], as_index=False).agg(
        {
            "analyst": first_alphabetical,
            "sector": first_alphabetical,
            "pal": "sum",
            "exposure": "sum",
            "return": "sum",
            "index": get_first_index_label,
        },
    )
    # !
    # Reassigning to pal, exposure, return    may result in an invalid state of entities
    # In practice, the join should be made to new columns.
    _df["pal"] = None
    _df["exposure"] = None
    _df["return"] = None

    # reassign pk to df.index
    _df.set_index("index", inplace=True)
    _df.update(another_df)

    # drop where null because these rows already contributed values under (sum, first) aggregations
    # on join
    _df.dropna(inplace=True)
    _df.set_index(["lkid", "date"], inplace=True)
    _df.sort_index(axis=PandasAxisType.ROWS, inplace=True)
    _df.reset_index(drop=False, inplace=True)

    logging.info(_df)
    logging.info(_df.columns)
    return _df


# Utility functions


class PandasAxisType(IntEnum):
    ROWS = 0
    COLS = 1


def first_alphabetical(series: pandas.Series):
    """
    ::

        return series.min()
    """

    return series.min()


def impute_first_day_open_liq(row: pandas.Series):
    """
    ::

        row["open_liq"] = row["exposure"] - row["pal"]
    """

    row["open_liq"] = row["exposure"] - row["pal"]
    return row


def impute_ticker_symbol(_row):
    """
    The presence of a ticker column makes no difference in the computed return.
    Return is aggregated over (date, lkid), so arbitrary values can be imputed if
    allowed by resource constraints.
    ::

        return f"ticker_{_row["index"]}"
    """
    return f"ticker_{_row["index"]}"


def get_first_index_label(group):
    """Returns the index label of the first row in the group
    ::

        return group.index[0]
    """
    return group.index[0]
