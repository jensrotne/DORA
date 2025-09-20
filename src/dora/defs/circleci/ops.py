from typing import List
import dagster as dg
import requests

from dora.defs.resources import PostgresResource




@dg.op(required_resource_keys={"pg"})
def get_pipelines(context: dg.OpExecutionContext):
    pg: PostgresResource = context.resources.pg
    with pg.connect() as conn:
        result = conn.execute("SELECT id FROM circleci_pipelines").fetchall()
    return [r[0] for r in result]


@dg.op(required_resource_keys={"pg"})
def create_table(context: dg.OpExecutionContext, create_sql: str):
    pg: PostgresResource = context.resources.pg
    with pg.connect() as conn:
        conn.execute(create_sql)


@dg.op(out=dg.DynamicOut())
def make_batches(pipeline_ids: List[str], num_splits: int = 10):
    # Split into balanced slices; each becomes a dynamic output (i.e., a mapped task)
    for i, batch in enumerate(pipeline_ids[i::num_splits] for i in range(num_splits)):
        batch = list(batch)
        if not batch:
            continue
        yield dg.DynamicOutput(batch, mapping_key=f"batch_{i}")


@dg.op(required_resource_keys={"pg", "circleci_token"})
def get_workflows(context: dg.OpExecutionContext, pipeline_ids: list[str]) -> None:
    circleci_token = context.resources.circleci_token
    pg: PostgresResource = context.resources.pg

    workflows = []
    next_page_token = None
    params = {}

    headers = {
        "Circle-Token": circleci_token
    }

    for pipeline_id in pipeline_ids:
        next_page_token = None
        params = {}
        while True:
            if next_page_token:
                params["page-token"] = next_page_token
            response = requests.get(
                f"https://circleci.com/api/v2/pipeline/{pipeline_id}/workflow",
                headers=headers,
                params=params
            )
            if response.status_code == 200:
                data = response.json()
                page_workflows = data.get("items", [])
                workflows.extend(page_workflows)
                next_page_token = data.get("next_page_token")
                if not next_page_token:
                    break

    with pg.connect() as conn:
        insert_sql = """
        INSERT INTO circleci_workflows (
            pipeline_id, 
            cancelled_by, 
            id, 
            name, 
            project_slug, 
            errored_by, 
            status, 
            tag, 
            max_auto_reruns, 
            pipeline_number, 
            created_at, 
            stopped_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """

    rows = []
    for workflow in workflows:
        rows.append((
            workflow.get("pipeline_id"),
            workflow.get("cancelled_by"),
            workflow.get("id"),
            workflow.get("name"),
            workflow.get("project_slug"),
            workflow.get("errored_by"),
            workflow.get("status"),
            workflow.get("tag"),
            workflow.get("max_auto_reruns"),
            workflow.get("pipeline_number"),
            workflow.get("created_at"),
            workflow.get("stopped_at")
        ))
    try:
        conn.executemany(insert_sql, rows)
    except Exception:
        for r in rows:
            conn.execute(insert_sql, r)
