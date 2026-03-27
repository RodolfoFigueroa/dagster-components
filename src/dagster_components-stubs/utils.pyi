from collections.abc import Sequence
from typing import Literal

from dagster_components.types import BoundDFType as BoundDFType

def cast_all_columns_to_numeric(
    df: BoundDFType,
    ignore: Sequence[str] | None = None,
    *,
    errors: Literal["coerce", "raise"] = "raise",
    make_valid_int: bool = False,
) -> BoundDFType: ...
