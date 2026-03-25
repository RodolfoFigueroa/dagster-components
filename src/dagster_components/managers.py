import dagster as dg
import geopandas as gpd
import sqlalchemy
from pydantic import PrivateAttr


class GeoDataFramePostGISManager(dg.ConfigurableIOManager):
    host: str
    port: str
    user: str
    password: str
    db: str

    _engine: sqlalchemy.engine.Engine = PrivateAttr()

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:  # noqa: ARG002
        self._engine = sqlalchemy.create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}?client_encoding=utf8",
        )

    def handle_output(self, context: dg.OutputContext, obj: gpd.GeoDataFrame) -> None:
        table = context.definition_metadata["table_name"]

        with self._engine.connect() as conn:
            obj.to_postgis(table, conn, if_exists="replace")

            if "primary_key" in context.definition_metadata:
                primary_key = context.definition_metadata["primary_key"]

                if primary_key not in obj.columns:
                    err = f"Primary key {primary_key} not found in GeoDataFrame columns"
                    raise ValueError(err)

                conn.execute(
                    sqlalchemy.text(
                        f'ALTER TABLE {table} ADD PRIMARY KEY ("{primary_key}");',
                    ),
                )

            conn.commit()

    def load_input(self, context: dg.InputContext) -> gpd.GeoDataFrame:
        upstream_output = context.upstream_output
        if upstream_output is None:
            err = "No upstream output found for GeoDataFramePostGISManager"
            raise ValueError(err)

        table = upstream_output.definition_metadata["table_name"]

        in_metadata = context.definition_metadata
        if "columns" in in_metadata:
            wanted_cols = in_metadata["columns"]
            if "geometry" not in wanted_cols:
                wanted_cols.append("geometry")
            cols_str = ", ".join(wanted_cols)
        else:
            cols_str = "*"

        with self._engine.connect() as conn:
            return gpd.read_postgis(
                f"""
                SELECT {cols_str} FROM {table}
                """,  # noqa: S608
                conn,
                geom_col="geometry",
            )
