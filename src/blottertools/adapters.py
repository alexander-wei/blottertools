"""adapters.py"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Protocol, Sequence, TypeVar

import pandas
from pandas.core.groupby.generic import DataFrameGroupBy

# pylint: disable=missing-function-docstring


class Transformation(Protocol):
    """
    Define ETL tasks as functions
    ::

        etl_task(df, _executor): ...

    that mutate a passed DataFrame df, or return a DataFrame df.

    If _executor is passed, its .groupby() method can potentially yield
    in memory savings, if implemented over partitions of groupby keys.
    """

    def __call__(
        self, df: pandas.DataFrame, _executor: Executor
    ) -> pandas.DataFrame: ...


class GroupByLike(Protocol):
    """
    GroupByLike methods are compatible with pandas:
    ::

        def groupby(
            self,
            by=None,
            axis: Axis | lib.NoDefault = lib.no_default,
            level: IndexLabel | None = None,
            as_index: bool = True,
            sort: bool = True,
            group_keys: bool = True,
            observed: bool | lib.NoDefault = lib.no_default,
            dropna: bool = True,
        )
    """

    def __call__(
        self,
        by: Any | None = None,
        axis: int = 0,
        level: Any | None = None,
        as_index: bool = True,
        sort: bool = True,
        group_keys: bool = True,
        observed: bool = False,
        dropna: bool = True,
    ) -> DataFrameGroupBy: ...


class Pipeline:
    """A Pipeline is defined as a sequence of ETL tasks. It can be run
    on any executor implementing the Executor interface."""

    def __init__(self, steps: Sequence[Transformation]) -> None:
        self.steps = list(steps)

    def run(self, executor: PandasExecutor) -> None:
        for step in self.steps:
            _df = transform(executor, step)
            if executor.is_eager():
                continue
            executor.update_df(_df)


class Executor(ABC):
    """Executor interface. Uses _df"""

    _df: DataFrameLike

    def __init__(self, df: DataFrameLike) -> None:
        self._df = df

    @abstractmethod
    def _groupby(self, *ac, **av) -> DataFrameGroupBy: ...

    @property
    def groupby(self) -> GroupByLike:
        return self._groupby

    @abstractmethod
    def update_df(self, df) -> None: ...

    @property
    def df(self) -> pandas.DataFrame:
        return self._df

    def __str__(self) -> str:
        return str(self._df)


class PandasExecutor(Executor):
    """
    Implements the Executor using an in-memory DataFrame.

    If __is_eager is defined, task functions operate on a single DataFrame
    object. Otherwise, task functions operate on copies and yield
    objects that are reassigned to _df.
    """

    __is_eager: bool | None

    def _groupby(self, *ac, **av) -> DataFrameGroupBy:
        return self._df.groupby(*ac, **av)

    def __init__(self, df, run_eagerly: bool = False) -> None:
        super().__init__(df)
        self.__is_eager = run_eagerly

    def is_eager(self) -> bool:
        return self.__is_eager

    def update_df(self, df) -> None:
        self._df = df


DataFrameLike = TypeVar("DataFrameLike")


def transform(executor: PandasExecutor, f: Transformation) -> pandas.DataFrame:
    return f(executor.df, executor, _eager=executor.is_eager())
