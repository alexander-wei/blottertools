"""blottercli.py"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import asdict, dataclass

import pandas

import functions as F
from blottertools.adapters import PandasExecutor, Pipeline

# pylint: disable=missing-class-docstring, missing-function-docstring


class InvalidInputPathError(Exception):
    """raise when input path is invalid"""


@dataclass(frozen=True)
class PandasReadCSVArgs:
    """pass to pandas.read_csv()"""

    dtype = {
        "date": object,
        "sector": pandas.CategoricalDtype(),
        "pal": object,
        "exposure": object,
        "ticker": pandas.CategoricalDtype(),
    }


PIPELINE_STEPS = [
    F.step1_create_pk,
    F.step2_sector_pal_date,
    F.step3_impute_ticker,
    F.step4_compute_open_liq,
    F.step5_compute_fund_value,
    F.step6_agg_daily,
]


class BlotterCLI:
    """
    Usage: blottercli [filename] -o [output]
    """

    _pipeline: Pipeline
    _executor: PandasExecutor
    _args: CLIArgs | None

    @dataclass(frozen=True)
    class CLIArgs:
        input_path: str = ""
        output_path: str = ""
        eager: bool = True
        loglevel_info: bool = False

    def __init__(self, pipeline, executor) -> None:
        self._pipeline = pipeline
        self._executor = executor

    def run(self):
        self._pipeline.run(self._executor)

    @staticmethod
    def main(cli_args: CLIArgs | None = None) -> None:
        cli_arg_container = BlotterCLI._normalize_args(cli_args)
        BlotterCLI._validate_args(cli_arg_container)
        if cli_arg_container.loglevel_info:
            logging.basicConfig(level=logging.INFO)

        steps = PIPELINE_STEPS

        pipeline = Pipeline(steps)

        df = pandas.read_csv(
            cli_arg_container.input_path,
            **asdict(PandasReadCSVArgs()),
        )

        executor = PandasExecutor(df, run_eagerly=cli_arg_container.eager)
        blottertool = BlotterCLI(pipeline=pipeline, executor=executor)
        blottertool.run()

        blottertool._executor.df.to_csv(  # pylint: disable=protected-access
            cli_arg_container.output_path or get_default_output_path(os.getcwd()),
            index=False,
            columns=["lkid", "date", "analyst", "sector", "pal", "exposure", "return"],
            float_format="%.16f",
        )
        logging.info(cli_arg_container)

    @staticmethod
    def _normalize_args(args: CLIArgs | dict | None = None) -> CLIArgs:
        """argparse overwrites default args that are in CLIArgs"""
        if isinstance(args, BlotterCLI.CLIArgs):
            base = dict(vars(args))
        else:
            base = BlotterCLI.parse_args()

        return BlotterCLI.CLIArgs(
            **{key: value for key, value in base.items() if value is not None}
        )

    @staticmethod
    def _validate_args(args: CLIArgs) -> None:
        if args.input_path == "":
            raise InvalidInputPathError

    @staticmethod
    def parse_args() -> dict:
        parse = argparse.ArgumentParser("blottercli")
        parse.add_argument(
            dest="input_path",
            metavar="input",
            help="The input file, eg: file.csv, ./path_to_csv/file.csv, .\\path_to_csv\\file.csv",
            default=None,
        )
        parse.add_argument(
            "-o",
            dest="output_path",
            metavar="output",
            help="The output file, eg: ./path_to__output/file.csv",
            default=None,
        )
        parse.add_argument(
            "--disable-eager",
            dest="eager",
            action="store_false",
            help="Transformations will mutate copies of a DataFrame.",
            default=None,
        )
        parse.add_argument(
            "--info",
            dest="loglevel_info",
            action="store_true",
            help="set loglevel info",
            default=None,
        )
        return vars(parse.parse_args())


def get_default_output_path(cwd):
    return os.path.join(cwd, "blotter-new.csv")


if __name__ == "__main__":
    BlotterCLI.main()
