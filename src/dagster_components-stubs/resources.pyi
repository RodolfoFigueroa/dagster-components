from collections.abc import Generator
from contextlib import contextmanager

import dagster as dg
import sqlalchemy

class PostgresResource(dg.ConfigurableResource):
    host: str
    port: str
    user: str
    password: str
    db: str
    def setup_for_execution(self, context: dg.InitResourceContext) -> None: ...
    @contextmanager
    def connect(self) -> Generator[sqlalchemy.engine.Connection]: ...
