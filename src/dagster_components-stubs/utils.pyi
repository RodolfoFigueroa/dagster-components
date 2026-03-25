from collections.abc import Sequence

from dagster_components.types import G

def cast_all_columns_to_numeric(df: G, ignore: Sequence[str] | None = None) -> G: ...
