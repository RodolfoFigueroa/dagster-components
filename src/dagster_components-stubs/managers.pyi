from typing import Generic

import dagster as dg
import geopandas as gpd
import pandas as pd
import sqlalchemy

from dagster_components.types import DFType

class _DataFrameBasePostgresManager(dg.ConfigurableIOManager, Generic[DFType]):
    host: str
    port: str
    user: str
    password: str
    db: str
    def write_table(
        self, df: DFType, table_name: str, conn: sqlalchemy.Connection,
    ) -> None: ...
    def load_table(
        self, table_name: str, cols_str: str, conn: sqlalchemy.Connection,
    ) -> DFType: ...
    def setup_for_execution(self, context: dg.InitResourceContext) -> None: ...
    def handle_output(self, context: dg.OutputContext, obj: DFType) -> None: ...
    def load_input(self, context: dg.InputContext) -> DFType: ...

class DataFramePostgresManager(_DataFrameBasePostgresManager[pd.DataFrame]):
    def write_table(
        self, df: pd.DataFrame, table_name: str, conn: sqlalchemy.Connection,
    ) -> None: ...
    def load_table(
        self, table_name: str, cols_str: str, conn: sqlalchemy.Connection,
    ) -> pd.DataFrame: ...

class GeoDataFramePostGISManager(_DataFrameBasePostgresManager[gpd.GeoDataFrame]):
    def write_table(
        self, df: gpd.GeoDataFrame, table_name: str, conn: sqlalchemy.Connection,
    ) -> None: ...
    def load_table(
        self, table_name: str, cols_str: str, conn: sqlalchemy.Connection,
    ) -> gpd.GeoDataFrame: ...
