import dagster as dg
import geopandas as gpd

class GeoDataFramePostGISManager(dg.ConfigurableIOManager):
    host: str
    port: str
    user: str
    password: str
    db: str
    def setup_for_execution(self, context: dg.InitResourceContext) -> None: ...
    def handle_output(
        self,
        context: dg.OutputContext,
        obj: gpd.GeoDataFrame,
    ) -> None: ...
    def load_input(self, context: dg.InputContext) -> gpd.GeoDataFrame: ...
