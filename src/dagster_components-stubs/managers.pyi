from collections.abc import Callable as Callable
from typing import Any, Generic, Literal

import dagster as dg
import geopandas as gpd
import pandas as pd
import sqlalchemy
from dagster._config.pythonic_config.resource import TResValue as TResValue

from dagster_components.resources import PostgresResource as PostgresResource
from dagster_components.types import DFType as DFType
from dagster_components.types import T as T

class _DataFrameBasePostgresManager(
    dg.ConfigurableIOManager, Generic[DFType, TResValue]
):
    postgres_resource: dg.ResourceDependency[PostgresResource]
    def write_table(
        self, df: DFType, table_name: str, conn: sqlalchemy.Connection
    ) -> None: ...
    def load_table(
        self, table_name: str, cols_str: str, conn: sqlalchemy.Connection
    ) -> DFType: ...
    def handle_output(self, context: dg.OutputContext, obj: DFType) -> None: ...
    def load_input(self, context: dg.InputContext) -> DFType: ...

class DataFramePostgresManager(_DataFrameBasePostgresManager[pd.DataFrame, Any]):
    def write_table(
        self, df: pd.DataFrame, table_name: str, conn: sqlalchemy.Connection
    ) -> None: ...
    def load_table(
        self, table_name: str, cols_str: str, conn: sqlalchemy.Connection
    ) -> pd.DataFrame: ...

class GeoDataFramePostGISManager(_DataFrameBasePostgresManager[gpd.GeoDataFrame, Any]):
    def write_table(
        self, df: gpd.GeoDataFrame, table_name: str, conn: sqlalchemy.Connection
    ) -> None: ...
    def load_table(
        self, table_name: str, cols_str: str, conn: sqlalchemy.Connection
    ) -> gpd.GeoDataFrame: ...

class PathResource(dg.ConfigurableResource):
    out_path: str

class _DataFrameBaseFileManager(dg.ConfigurableIOManager):
    path_resource: dg.ResourceDependency[PathResource]
    extension: Literal[".parquet", ".csv", ".gpkg", ".geoparquet"]
    def handle_output(self, context: dg.OutputContext, obj: DFType) -> None: ...
    def load_input(self, context: dg.InputContext) -> DFType: ...

class DataFrameFileManager(_DataFrameBaseFileManager):
    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None: ...
    def load_input(
        self, context: dg.InputContext
    ) -> pd.DataFrame | dict[str, pd.DataFrame]: ...

class GeoDataFrameFileManager(_DataFrameBaseFileManager):
    def handle_output(
        self, context: dg.OutputContext, obj: gpd.GeoDataFrame
    ) -> None: ...
    def load_input(
        self, context: dg.InputContext
    ) -> gpd.GeoDataFrame | dict[str, gpd.GeoDataFrame]: ...
