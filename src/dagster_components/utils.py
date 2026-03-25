from collections.abc import Sequence
from typing import Literal

import pandas as pd

from dagster_components.types import G


def cast_all_columns_to_numeric(
    df: G,
    ignore: Sequence[str] | None = None,
    *,
    errors: Literal["coerce", "raise"] = "raise",
) -> G:
    """Casts all columns in a DataFrame to numeric, optionally skipping some.

    Applies ``pd.to_numeric`` with ``errors='coerce'`` to every column not in
    ``ignore``, converting non-parseable values to ``NaN``. Operates on a copy
    of the input, leaving the original unchanged.

    Args:
        df: The DataFrame or GeoDataFrame whose columns will be cast.
        ignore: Column names to leave untouched. Defaults to ``None``, which
            skips no columns.

    Returns:
        A copy of ``df`` with eligible columns converted to numeric dtypes.
    """
    if ignore is None:
        ignore = []

    df = df.copy()
    for col in df.columns:
        if col not in ignore:
            df[col] = pd.to_numeric(df[col], errors=errors)
    return df
