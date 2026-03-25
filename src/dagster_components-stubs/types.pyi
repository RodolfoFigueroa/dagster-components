from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    import pandas as pd

G = TypeVar("G", bound=pd.DataFrame)
