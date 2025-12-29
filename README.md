# blottertools

[![Static Badge](https://img.shields.io/badge/docs-html-6082B6?style=for-the-badge)](https://alexander-wei.github.io/blottertools/)
&emsp;[![Static Badge](https://img.shields.io/badge/github-src-8A9A5B?style=for-the-badge&logo=github)](https://github.com/alexander-wei/blottertools)

A small library and CLI for composing Pandas ETL task functions involving a DataFrame. The included CLI demonstrates transforming a CSV input using a sequence of transformation steps.

## Overview

- Library `blottertools` provides lightweight building blocks to implement ETL operations as Python functions and run them in a sequential pipeline.
- CLI `blottercli` is provided as an example entry point that reads a CSV, runs a pipeline, and writes a CSV output.

## Requirements

- Python 3.12 or newer
- See pyproject.toml for pinned dependencies; notable ones:
  - pandas ~= 2.3
  - tabulate
  - tzdata
  - dotenv, Jinja2

Recommended: use a virtual environment and install dependencies with pip.

## Installation

From the project root:

0.  Install Python 3.12 or newer. Using [Anaconda](https://www.anaconda.com/download),

        conda create --name [env-name] python=3.12

1.  Create and activate a virtual environment (recommended),

        python -m venv .venv
        source .venv/bin/activate

2.  Install the package,

        python -m pip install .

This installs a console script named `blottercli`.

## CLI Usage

    blottercli path/to/input.csv -o path/to/output.csv

    Options:

    - input (positional): path to input CSV file (required).
    - -o, --output_path: output CSV path. If omitted, the tool writes `blotter-new.csv` into the current working directory.
    - --disable-eager: run in non-eager mode (transformations operate on copies and return updated DataFrames).

    Example:

    blottercli fixtures/sample.csv -o blotter-new.csv

## Output

The CLI expects a CSV containing the columns

    lkid, date, analyst, sector, pal, exposure, ticker

If `ticker` is omitted, it is ignored.

On execution, the CLI writes a CSV containing the columns:

    lkid, date, analyst, sector, pal, exposure, return

## High-level API (overview)

The library provides a few simple moving parts to compose ETL logic as reusable functions.

- Pipeline

  - Constructed with an ordered sequence of transformation functions ("steps").
  - Usage: Pipeline(steps).run(executor)
  - Each step is a callable that accepts (df, executor, \_eager=bool) and returns or mutates a DataFrame.

- PandasExecutor

  - An in-memory executor backed by a pandas.DataFrame.
  - Construct with PandasExecutor(df, run_eagerly=True|False).
  - Key methods/properties:
    - executor.df: the retained DataFrame
    - executor.groupby(...): an adapter to pandas' DataFrame.groupby method

- df_task decorator

  - Use @df_task to adapt a transformation function to the pipeline's expected signature.
  - Typical decorated function signature:

        @df_task
        def my_step(df: pandas.DataFrame, _executor: Optional[Executor] = None) -> pandas.DataFrame:
            # mutate df and return df, or return a new DataFrame
            return df

  - Behavior:
    - If run in eager mode, the decorated function receives the actual DataFrame and must mutate it in place (and must not rebind/return a different DataFrame). If it returns a different object while eager is enabled, a runtime error is raised.
    - If not eager, the decorator passes a copy of the DataFrame; the function may return a new DataFrame which will then be assigned back to the executor.
    - If your function needs grouped reduction or access to executor-level helpers, accept the optional `_executor` parameter. This is so that a future release may support distributed execution of @df_task, for minimizing active memory consumption.

- Writing a pipeline

  - Define a list of step callables (functions decorated with @df_task or compatible with the Transformation protocol).
  - Construct a Pipeline and run it with a PandasExecutor.

  Example:

        from blottertools.adapters import Pipeline, PandasExecutor
        from functions import step1_create_pk, step2_sector_pal_date

        steps = [step1_create_pk, step2_sector_pal_date]
        pipeline = Pipeline(steps)

        df = pandas.read_csv("input.csv")
        executor = PandasExecutor(df, run_eagerly=True)
        pipeline.run(executor)

  The resulting DataFrame is accessed at `executor.df`

## Development / Running locally

- Install with `python -m pip install -e .` and run `blottercli` as shown above.
