from typing import Any, Generic

import dagster as dg
import geopandas as gpd
import pandas as pd
import sqlalchemy
from dagster._config.pythonic_config.resource import TResValue as TResValue

from dagster_components.types import DFType as DFType

class _DataFrameBasePostgresManager(
    dg.ConfigurableIOManager, Generic[DFType, TResValue]
):
    host: str
    port: str
    user: str
    password: str
    db: str
    def setup_for_execution(self, context: dg.InitResourceContext) -> None: ...
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
