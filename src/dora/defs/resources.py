import dagster as dg
from dagster import ConfigurableResource
from typing import Optional
import psycopg
import os

github_token = os.getenv("GITHUB_TOKEN")
circleci_token = os.getenv("CIRCLECI_TOKEN")
pg_password = os.getenv("PG_PASSWORD")
github_org = os.getenv("GITHUB_ORG")
circleci_org = os.getenv("CIRCLECI_ORG")

class PostgresResource(ConfigurableResource):
    host: str
    port: int = 5432
    dbname: str
    user: str
    password: str
    sslmode: Optional[str] = None  # e.g. "require"

    def connect(self):
        """Return a psycopg connection. Caller should use as a context manager or close it."""
        return psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            sslmode=self.sslmode,
        )
        
postgres_resource = PostgresResource(
    host="localhost",
    dbname="dagster",
    user="dagster",
    password=pg_password,
    sslmode=None,
)

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "pg": postgres_resource,
            "circleci_token": circleci_token,
            "github_token": github_token,
            "github_org": github_org,
            "circleci_org": circleci_org,
        }
    )