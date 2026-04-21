from typing import TypeVar

T = TypeVar("T")
DFType = TypeVar("DFType", pd.DataFrame, gpd.GeoDataFrame)
BoundDFType = TypeVar("BoundDFType", bound=pd.DataFrame)
