"""test_mytests.py"""

import csv
import filecmp
import os
import re
import unittest
from dataclasses import asdict
from datetime import datetime
from typing import Callable

import dotenv
import pandas

from blottercli import PIPELINE_STEPS, BlotterCLI, PandasReadCSVArgs
from src.blottertools.adapters import PandasExecutor, Pipeline
from src.blottertools.decorators import df_task

# pylint: disable=missing-function-docstring

_DIRECTORY_NONCE = "164016634"


def clean_up_test_blotter_cli_directory(nonce):
    """Deletes the output file if it exists in the proper working directory."""
    cwd = os.getcwd()
    if not os.path.isfile(os.path.join(cwd, f".{nonce}")):
        raise FileNotFoundError(f"Nonce not found in cwd {cwd}. Aborting cleanup.")

    os.remove(get_default_output_path(cwd))


def get_cleanup_nonce():
    """Unique value prevents deletion of blotter-new from the wrong directory in test cleanup."""
    return _DIRECTORY_NONCE


def get_default_output_path(cwd):
    """Returns the default output path to use."""
    return os.path.join(cwd, "blotter-new.csv")


def get_default_input_path():
    """Returns the default input path."""
    return dotenv.get_key(".env", "INPUT_PATH")


def _simple_nondecorated_transform_task(df: pandas.DataFrame):
    return df


@df_task
def _decorated_simple_transform_task(df: pandas.DataFrame):
    return df


@df_task
def _rebinding_eager_df(df: pandas.DataFrame):
    """Returning a different DataFrame object (rebinding) will cause RuntimeError in Eager mode."""
    df = df.reset_index()
    print(df)


class TestDecoratorDFTask(unittest.TestCase):
    """The df_task decorator returns a callable. Decorated functions return a DataFrame."""

    def test_decorated_fct_returns_df(self):
        df = pandas.DataFrame([])
        self.assertIsInstance(_decorated_simple_transform_task(df), pandas.DataFrame)

    def test_decorator_yields_callable(self):
        self.assertIsInstance(df_task(_simple_nondecorated_transform_task), Callable)


class TestEagerTasks(unittest.TestCase):
    """Executor executes a Pipeline eagerly."""

    @classmethod
    def setUpClass(cls):
        cls.df = pandas.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        cls.df = cls.df.reset_index()
        cls.pipeline = Pipeline(
            [
                _rebinding_eager_df,
            ]
        )

    def test_rebind_raises_error(self):
        executor = PandasExecutor(self.df, run_eagerly=True)
        print(self.df)
        with self.assertRaises(RuntimeError):
            self.pipeline.run(executor=executor)


class TestExecutor(unittest.TestCase):
    """The run(...) method of an Executor results in a valid state: .df exists."""

    @classmethod
    def setUpClass(cls):
        cls.pipeline = Pipeline(PIPELINE_STEPS)

    def test_default(self):
        """Executor with no arguments"""
        executor = PandasExecutor(
            pandas.read_csv(get_default_input_path(), **asdict(PandasReadCSVArgs()))
        )
        self.pipeline.run(executor)
        self.assertIsInstance(executor.df, pandas.DataFrame)

    def test_eager(self):
        executor = PandasExecutor(
            pandas.read_csv(get_default_input_path(), **asdict(PandasReadCSVArgs())),
            run_eagerly=True,
        )
        self.pipeline.run(executor)
        self.assertIsInstance(executor.df, pandas.DataFrame)

    def test_on_copy(self):
        executor = PandasExecutor(
            pandas.read_csv(get_default_input_path(), **asdict(PandasReadCSVArgs())),
            run_eagerly=False,
        )
        self.pipeline.run(executor)
        self.assertIsInstance(executor.df, pandas.DataFrame)


class TestBlotterCLI(unittest.TestCase):
    """Executable BlotterCLI program"""

    def setUp(self):
        try:
            clean_up_test_blotter_cli_directory(get_cleanup_nonce())
        except FileNotFoundError:
            pass

    def test_writes_to_blotter_new_default(self):
        """requested to write to blotter-new.csv, so assert that this is default behavior"""
        self.assertFalse(os.path.isfile(get_default_output_path(os.getcwd())))
        BlotterCLI.main(BlotterCLI.CLIArgs(input_path=get_default_input_path()))
        self.assertTrue(os.path.isfile(get_default_output_path(os.getcwd())))

    def test_tool_no_args(self):
        with self.assertRaises(Exception):
            BlotterCLI.main(BlotterCLI.CLIArgs())

    def test_tool_with_args(self):
        BlotterCLI.main(BlotterCLI.CLIArgs(input_path=get_default_input_path()))
        BlotterCLI.main(
            BlotterCLI.CLIArgs(
                input_path=get_default_input_path(),
                output_path=get_default_output_path(os.getcwd()),
            )
        )

    def test_diff_output(self):
        """recover the same output"""
        BlotterCLI.main(BlotterCLI.CLIArgs(input_path=get_default_input_path()))

        assert filecmp.cmp(
            get_default_output_path(os.getcwd()),
            dotenv.get_key(".env", "DIFF_OUTPUT_PATH"),
            shallow=False,
        )


class TransformationTasksTestSample1(unittest.TestCase):
    """functions.py defines valid transformations."""

    @classmethod
    def setUpClass(cls):
        try:
            clean_up_test_blotter_cli_directory(get_cleanup_nonce())
        except FileNotFoundError:
            pass
        cls.pipeline = Pipeline(PIPELINE_STEPS)
        cls.executor = PandasExecutor(
            pandas.read_csv(get_default_input_path(), **asdict(PandasReadCSVArgs()))
        )
        cls.pipeline.run(cls.executor)

        BlotterCLI.main(BlotterCLI.CLIArgs(input_path=get_default_input_path()))

        with open(get_default_output_path(os.getcwd()), "r", encoding="utf-8") as fi:
            reader = csv.reader(fi)
            cls.header, _row = next(reader), next(reader)
            cls.firstrow_dict = dict(zip(cls.header, _row))

    def test_date_format(self):
        datetime.strptime(self.firstrow_dict["date"], "%Y-%m-%d")

    def test_rename_technology(self):
        sectors = set(self.executor.df["sector"].unique())
        self.assertNotIn("Technology", sectors)

    def test_return_format(self):
        if not re.fullmatch(r"^\d+\.\d+", self.firstrow_dict["return"]):
            raise ValueError("column 'return' format")

    def test_pal_format(self):
        if not re.fullmatch(r"^\d+\.*\d+", self.firstrow_dict["pal"]):
            raise ValueError("column 'pal' format")
        if not re.fullmatch(r"^\d+\.*\d+", self.firstrow_dict["exposure"]):
            raise ValueError("column 'exposure' format")

    def test_col_order(self):
        idx = {j: i for i, j in enumerate(self.header)}
        check_cols = ["lkid", "date", "analyst", "sector", "pal", "exposure", "return"]
        for i in range(len(check_cols) - 1):
            self.assertLess(idx[check_cols[i]], idx[check_cols[i + 1]])


class TransformationTasksTestSample2(TransformationTasksTestSample1):
    """test for a csv sample that has cooccuring (lkid, date, ticker)"""

    # pylint: disable=useless-parent-delegation

    @classmethod
    def setUpClass(cls):
        try:
            clean_up_test_blotter_cli_directory(get_cleanup_nonce())
        except FileNotFoundError:
            pass
        cls.pipeline = Pipeline(PIPELINE_STEPS)
        cls.executor = PandasExecutor(
            pandas.read_csv(
                dotenv.get_key(".env", "SAMPLE2_PATH"), **asdict(PandasReadCSVArgs())
            )
        )
        cls.pipeline.run(cls.executor)

        BlotterCLI.main(
            BlotterCLI.CLIArgs(input_path=dotenv.get_key(".env", "SAMPLE2_PATH"))
        )

        with open(get_default_output_path(os.getcwd()), "r", encoding="utf-8") as fi:
            reader = csv.reader(fi)
            cls.header, _row = next(reader), next(reader)
            cls.firstrow_dict = dict(zip(cls.header, _row))

    def test_date_format(self):
        return super().test_date_format()

    def test_rename_technology(self):
        return super().test_rename_technology()

    def test_return_format(self):
        return super().test_return_format()

    def test_pal_format(self):
        return super().test_pal_format()

    def test_col_order(self):
        return super().test_col_order()

    def test_no_null_values_in_output(self):
        self.assertFalse(self.executor.df.isna().any().any())
