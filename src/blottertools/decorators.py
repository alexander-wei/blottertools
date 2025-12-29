"""decorators.py"""

import functools
import inspect
from typing import Callable, TypeVar

DataFrame = TypeVar("DataFrame")


def df_task(func) -> Callable:
    """
    df_task(f) defines the @df_task decorator.

    Functions decorated with
    ::

        @df_task
        def f(df, executor: Optional[Executor])

    are adapted to the signature
    ::

        def _f(df, executor: Executor, is_eager: bool)

    expected by .adapters.Pipeline.
    """

    sig = inspect.signature(func)
    params = sig.parameters

    @functools.wraps(func)
    def wrap_context(df: DataFrame, _executor=None, _eager=True, **av) -> DataFrame:
        av = {}
        if "_executor" in params:
            av = {"_executor": _executor}
        result = func(df if _eager else df.copy(), **av)
        if _eager and result is not df:
            raise RuntimeError(
                (
                    "Transformation task must return a DataFrame, and eager transformation "
                    "cannot rebind DataFrame. eg. avoid reassignments if eager is enabled. "
                    "eg. df = df.reset_index(), etc"
                )
            )
        return result

    return wrap_context
