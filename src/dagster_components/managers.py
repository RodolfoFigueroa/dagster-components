from collections.abc import Callable
from pathlib import Path
from typing import Any, Generic, Literal, overload

import dagster as dg
import geopandas as gpd
import pandas as pd
import sqlalchemy
from dagster._config.pythonic_config.resource import TResValue

from dagster_components.resources import PostgresResource
from dagster_components.types import DFType, T


class _DataFrameBasePostgresManager(
    dg.ConfigurableIOManager,
    Generic[DFType, TResValue],
):
    postgres_resource: dg.ResourceDependency[PostgresResource]

    def write_table(
        self,
        df: DFType,
        table_name: str,
        conn: sqlalchemy.Connection,
    ) -> None:
        msg = "write_table must be implemented by subclasses"
        raise NotImplementedError(msg)

    def load_table(
        self,
        table_name: str,
        cols_str: str,
        conn: sqlalchemy.Connection,
    ) -> DFType:
        msg = "load_table must be implemented by subclasses"
        raise NotImplementedError(msg)

    def handle_output(
        self,
        context: dg.OutputContext,
        obj: DFType,
    ) -> None:
        table = context.definition_metadata["table_name"]

        with self.postgres_resource.connect() as conn:
            self.write_table(obj, table, conn)

            if "primary_key" in context.definition_metadata:
                primary_key = context.definition_metadata["primary_key"]

                if primary_key not in obj.columns:
                    err = f"Primary key {primary_key} not found in DataFrame columns"
                    raise ValueError(err)

                conn.execute(
                    sqlalchemy.text(
                        f'ALTER TABLE {table} ADD PRIMARY KEY ("{primary_key}");',
                    ),
                )

            if "foreign_keys" in context.definition_metadata:
                foreign_keys = context.definition_metadata["foreign_keys"]

                for fk_map in foreign_keys:
                    fk_col = fk_map["column"]
                    ref_table = fk_map["ref_table"]
                    ref_col = fk_map["ref_column"]

                    if fk_col not in obj.columns:
                        err = f"Foreign key column {fk_col} not found in DataFrame columns."
                        raise ValueError(err)

                    conn.execute(
                        sqlalchemy.text(
                            f"""
                            ALTER TABLE {table}
                            ADD FOREIGN KEY ("{fk_col}")
                            REFERENCES {ref_table}("{ref_col}")
                            """,
                        ),
                    )

            conn.commit()

    def load_input(self, context: dg.InputContext) -> DFType:
        upstream_output = context.upstream_output
        if upstream_output is None:
            err = "No upstream output found."
            raise ValueError(err)

        table = upstream_output.definition_metadata["table_name"]

        in_metadata = context.definition_metadata
        if "columns" in in_metadata:
            wanted_cols = in_metadata["columns"]
            cols_str = ", ".join(wanted_cols)
        else:
            cols_str = "*"

        with self.postgres_resource.connect() as conn:
            return self.load_table(table, cols_str, conn)


class DataFramePostgresManager(_DataFrameBasePostgresManager[pd.DataFrame, Any]):
    def write_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        conn: sqlalchemy.Connection,
    ) -> None:
        df.to_sql(table_name, conn, if_exists="replace", index=False)

    def load_table(
        self,
        table_name: str,
        cols_str: str,
        conn: sqlalchemy.Connection,
    ) -> pd.DataFrame:
        return pd.read_sql(f"SELECT {cols_str} FROM {table_name}", conn)  # noqa: S608


class GeoDataFramePostGISManager(_DataFrameBasePostgresManager[gpd.GeoDataFrame, Any]):
    def write_table(
        self,
        df: gpd.GeoDataFrame,
        table_name: str,
        conn: sqlalchemy.Connection,
    ) -> None:
        df.to_postgis(table_name, conn, if_exists="replace")

    def load_table(
        self,
        table_name: str,
        cols_str: str,
        conn: sqlalchemy.Connection,
    ) -> gpd.GeoDataFrame:
        return gpd.read_postgis(
            f"SELECT {cols_str} FROM {table_name}",  # noqa: S608
            conn,
            geom_col="geometry",
        )


class PathResource(dg.ConfigurableResource):
    out_path: str


class _DataFrameBaseFileManager(dg.ConfigurableIOManager):
    path_resource: dg.ResourceDependency[PathResource]
    extension: Literal[".parquet", ".csv", ".gpkg", ".geoparquet"]

    def _get_single_partition_key_path(
        self, partition_key: str, asset_dir: Path
    ) -> Path:
        if not asset_dir.is_dir():
            err = f"Asset directory {asset_dir} does not exist or is not a directory."
            raise ValueError(err)

        segments = partition_key.split("|")
        fpath = asset_dir / "/".join(segments)
        return fpath.with_suffix(fpath.suffix + self.extension)

    @overload
    def _get_partitioned_asset_path(
        self,
        context: dg.InputContext | dg.OutputContext,
        asset_dir: Path,
        *,
        allow_multiple_partitions: Literal[False],
    ) -> Path: ...

    @overload
    def _get_partitioned_asset_path(
        self,
        context: dg.InputContext | dg.OutputContext,
        asset_dir: Path,
        *,
        allow_multiple_partitions: Literal[True],
    ) -> dict[str, Path]: ...

    def _get_partitioned_asset_path(
        self,
        context: dg.InputContext | dg.OutputContext,
        asset_dir: Path,
        *,
        allow_multiple_partitions: bool,
    ) -> Path | dict[str, Path]:
        if len(context.asset_partition_keys) == 1:
            return self._get_single_partition_key_path(
                context.asset_partition_key, asset_dir
            )

        if not allow_multiple_partitions:
            err = (
                "Multiple partition keys found for asset, but allow_multiple_partitions is False. "
                f"Partition keys: {context.asset_partition_keys}"
            )
            raise ValueError(err)

        final_path = {}
        for partition_key in context.asset_partition_keys:
            final_path[partition_key] = self._get_single_partition_key_path(
                partition_key, asset_dir
            )
        return final_path

    @overload
    def _get_path(
        self,
        context: dg.InputContext | dg.OutputContext,
        *,
        allow_multiple_partitions: Literal[False] = False,
    ) -> Path: ...

    @overload
    def _get_path(
        self,
        context: dg.InputContext | dg.OutputContext,
        *,
        allow_multiple_partitions: Literal[True],
    ) -> dict[str, Path]: ...

    def _get_path(
        self,
        context: dg.InputContext | dg.OutputContext,
        *,
        allow_multiple_partitions: bool = False,
    ) -> Path | dict[str, Path]:
        out_path = Path(self.path_resource.out_path)
        asset_dir = out_path / "/".join(context.asset_key.path)

        if context.has_asset_partitions:
            return self._get_partitioned_asset_path(
                context, asset_dir, allow_multiple_partitions=allow_multiple_partitions
            )

        return asset_dir.with_suffix(asset_dir.suffix + self.extension)

    @overload
    def _dispatch_multiple_partitions(
        self,
        fpath: Path,
        func: Callable[[Path], T],
    ) -> T: ...

    @overload
    def _dispatch_multiple_partitions(
        self,
        fpath: dict[str, Path],
        func: Callable[[Path], T],
    ) -> dict[str, T]: ...

    def _dispatch_multiple_partitions(
        self,
        fpath: Path | dict[str, Path],
        func: Callable[[Path], T],
    ) -> T | dict[str, T]:
        if isinstance(fpath, Path):
            return func(fpath)

        if isinstance(fpath, dict):
            return {k: func(v) for k, v in fpath.items()}

        err = f"Unexpected type for file path: {type(fpath)}"
        raise TypeError(err)

    def handle_output(self, context: dg.OutputContext, obj: DFType) -> None:
        raise NotImplementedError

    def load_input(self, context: dg.InputContext) -> DFType:
        raise NotImplementedError


class DataFrameFileManager(_DataFrameBaseFileManager):
    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        fpath = self._get_path(context)
        fpath.parent.mkdir(parents=True, exist_ok=True)

        if self.extension == ".parquet":
            obj.to_parquet(fpath)
        elif self.extension == ".csv":
            obj.to_csv(fpath, index=True)
        else:
            err = f"Unsupported file extension: {self.extension}"
            raise ValueError(err)

    def load_input(
        self, context: dg.InputContext
    ) -> pd.DataFrame | dict[str, pd.DataFrame]:
        fpath = self._get_path(context, allow_multiple_partitions=True)

        if self.extension == ".parquet":
            return self._dispatch_multiple_partitions(fpath, pd.read_parquet)

        if self.extension == ".csv":
            return self._dispatch_multiple_partitions(
                fpath, lambda p: pd.read_csv(p, index_col=0)
            )

        err = f"Unsupported file extension: {self.extension}"
        raise ValueError(err)


class GeoDataFrameFileManager(_DataFrameBaseFileManager):
    def handle_output(self, context: dg.OutputContext, obj: gpd.GeoDataFrame) -> None:
        fpath = self._get_path(context)
        fpath.parent.mkdir(parents=True, exist_ok=True)

        if self.extension == ".gpkg":
            obj.to_file(fpath, driver="GPKG")
        elif self.extension == ".geoparquet":
            obj.to_parquet(fpath, index=True)
        else:
            err = f"Unsupported file extension: {self.extension}"
            raise ValueError(err)

    def load_input(
        self, context: dg.InputContext
    ) -> gpd.GeoDataFrame | dict[str, gpd.GeoDataFrame]:
        fpath = self._get_path(context, allow_multiple_partitions=True)

        if self.extension == ".gpkg":
            return self._dispatch_multiple_partitions(fpath, gpd.read_file)

        if self.extension == ".geoparquet":
            return self._dispatch_multiple_partitions(fpath, gpd.read_parquet)

        err = f"Unsupported file extension: {self.extension}"
        raise ValueError(err)
