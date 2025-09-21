from typing import List
import dagster as dg
import requests

from dora.defs.circleci.ops import create_table, get_workflows, make_batches
from dora.defs.circleci.utils import make_circleci_item_request, make_circleci_items_request
from dora.defs.resources import PostgresResource


@dg.asset(
    deps=["github_repos"],
    required_resource_keys={"pg", "circleci_token"}
)
def circleci_projects(context):
    pg = context.resources.pg
    circleci_token = context.resources.circleci_token

    with pg.connect() as conn:
        repos = conn.execute(
            "SELECT full_name FROM github_repos WHERE archived = false").fetchall()
        repo_names = [r[0] for r in repos]

    if len(repo_names) == 0:
        raise ValueError(
            "No GitHub repositories found. Skipping CircleCI project fetch.")

    with pg.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS circleci_projects (
                id TEXT PRIMARY KEY,
                slug TEXT,
                name TEXT,
                organization_name TEXT,
                organization_slug TEXT,
                organization_id TEXT,
                vcs_url TEXT,
                provider TEXT,
                default_branch TEXT
            )
        """)

        for repo_name in repo_names:
            project = make_circleci_item_request(
                f'https://circleci.com/api/v2/project/gh/{repo_name}',
                circleci_token,
                None
            )

            if not project:
                continue

            insert_sql = """
                INSERT INTO circleci_projects (
                    id, slug, name, organization_name, organization_slug, organization_id,
                    vcs_url, provider, default_branch
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    slug = circleci_projects.slug,
                    name = circleci_projects.name,
                    organization_name = circleci_projects.organization_name,
                    organization_slug = circleci_projects.organization_slug,
                    organization_id = circleci_projects.organization_id,
                    vcs_url = circleci_projects.vcs_url,
                    provider = circleci_projects.provider,
                    default_branch = circleci_projects.default_branch
            """

            conn.execute(insert_sql, (
                project.get("id"),
                project.get("slug"),
                project.get("name"),
                project.get("organization_name"),
                project.get("organization_slug"),
                project.get("organization_id"),
                project.get("vcs_info").get("vcs_url"),
                project.get("vcs_info").get("provider"),
                project.get("vcs_info").get("default_branch")
            ))


@dg.asset_check(asset="circleci_projects", required_resource_keys={"pg"})
def check_circleci_projects(context):
    pg = context.resources.pg

    count = 0

    with pg.connect() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM circleci_projects").fetchone()[0]
        if count == 0:
            return dg.AssetCheckResult(
                passed=False, metadata={"reason": "No projects found in circleci_projects table"}
            )

    return dg.AssetCheckResult(passed=True, metadata={"project_count": count})


@dg.asset(
    deps=["circleci_projects"],
    required_resource_keys={"pg", "circleci_token"}
)
def circleci_pipelines(context):
    pg = context.resources.pg
    circleci_token = context.resources.circleci_token

    project_slugs = []

    with pg.connect() as conn:
        circleci_projects = conn.execute(
            "SELECT slug FROM circleci_projects").fetchall()
        project_slugs = [r[0] for r in circleci_projects]
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS circleci_pipelines (
                id TEXT PRIMARY KEY,
                project_slug TEXT,
                number INTEGER,
                state TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                trigger_type TEXT,
                vcs_provider_name TEXT,
                vcs_target_repository_url TEXT,
                vcs_branch TEXT,
                vcs_review_id TEXT,
                vcs_review_url TEXT,
                vcs_revision TEXT,
                vcs_tag TEXT,
                vcs_commit_subject TEXT,
                vcs_commit_body TEXT,
                vcs_origin_repository_url TEXT
            )
        """)

    if len(project_slugs) == 0:
        raise ValueError(
            "No CircleCI projects found. Skipping pipeline fetch.")

    for slug in project_slugs:
        items = make_circleci_items_request(
            f'https://circleci.com/api/v2/project/{slug}/pipeline',
            circleci_token, None)
        
        with pg.connect() as conn:
            insert_sql = """
                INSERT INTO circleci_pipelines (
                    id, project_slug, number, state, created_at, updated_at,
                    trigger_type, vcs_provider_name, vcs_target_repository_url,
                    vcs_branch, vcs_review_id, vcs_review_url, vcs_revision,
                    vcs_tag, vcs_commit_subject, vcs_commit_body, vcs_origin_repository_url
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    project_slug = circleci_pipelines.project_slug,
                    number = circleci_pipelines.number,
                    state = circleci_pipelines.state,
                    created_at = circleci_pipelines.created_at,
                    updated_at = circleci_pipelines.updated_at,
                    trigger_type = circleci_pipelines.trigger_type,
                    vcs_provider_name = circleci_pipelines.vcs_provider_name,
                    vcs_target_repository_url = circleci_pipelines.vcs_target_repository_url,
                    vcs_branch = circleci_pipelines.vcs_branch,
                    vcs_review_id = circleci_pipelines.vcs_review_id,
                    vcs_review_url = circleci_pipelines.vcs_review_url,
                    vcs_revision = circleci_pipelines.vcs_revision,
                    vcs_tag = circleci_pipelines.vcs_tag,
                    vcs_commit_subject = circleci_pipelines.vcs_commit_subject,
                    vcs_commit_body = circleci_pipelines.vcs_commit_body,
                    vcs_origin_repository_url = circleci_pipelines.vcs_origin_repository_url
            """

            rows = []
            for pipeline in items:
                rows.append((
                    pipeline.get("id"),
                    pipeline.get("project_slug"),
                    pipeline.get("number"),
                    pipeline.get("state"),
                    pipeline.get("created_at"),
                    pipeline.get("updated_at"),
                    pipeline.get("trigger", {}).get("type"),
                    pipeline.get("vcs", {}).get("provider_name"),
                    pipeline.get("vcs", {}).get("target_repository_url"),
                    pipeline.get("vcs", {}).get("branch"),
                    pipeline.get("vcs", {}).get("review_id"),
                    pipeline.get("vcs", {}).get("review_url"),
                    pipeline.get("vcs", {}).get("revision"),
                    pipeline.get("vcs", {}).get("tag"),
                    pipeline.get("vcs", {}).get("commit", {}).get("subject"),
                    pipeline.get("vcs", {}).get("commit", {}).get("body"),
                    pipeline.get("vcs", {}).get("origin_repository_url"),
                ))
            try:
                conn.executemany(insert_sql, rows)
            except Exception:
                for r in rows:
                    conn.execute(insert_sql, r)

@dg.asset_check(asset="circleci_pipelines", required_resource_keys={"pg"})
def check_circleci_pipelines(context):
    pg = context.resources.pg

    count = 0

    with pg.connect() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM circleci_pipelines").fetchone()[0]
        if count == 0:
            return dg.AssetCheckResult(
                passed=False, metadata={"reason": "No pipelines found in circleci_pipelines table"}
            )

    return dg.AssetCheckResult(passed=True, metadata={"pipeline_count": count})


@dg.graph
def fetch_and_store_workflows(pipeline_ids: List[str]) -> None:
    batches = make_batches(pipeline_ids)
    batches.map(get_workflows)

    return None


@dg.asset(
    deps=["circleci_pipelines"],
)
def circleci_workflows():
    create_table("""
            CREATE TABLE IF NOT EXISTS circleci_workflows (
                pipeline_id TEXT,
                cancelled_by TEXT,
                id TEXT PRIMARY KEY,
                name TEXT,
                project_slug TEXT,
                errored_by TEXT,
                status TEXT,
                tag TEXT,
                max_auto_reruns INTEGER,
                pipeline_number INTEGER,
                created_at TIMESTAMP,
                stopped_at TIMESTAMP
            )
        """)

    pipeline_ids = get_pipelines()

    return fetch_and_store_workflows(pipeline_ids)


@dg.asset(
    deps=["circleci_workflows"]
)
def circleci_jobs(pg: dg.ResourceParam[PostgresResource]):
    if not circleci_token:
        raise ValueError("CIRCLECI_TOKEN environment variable is not set")

    headers = {
        "Circle-Token": circleci_token
    }

    workflow_ids = []

    with pg.connect() as conn:
        circleci_workflows = conn.execute(
            "SELECT id FROM circleci_workflows").fetchall()
        workflow_ids = [r[0] for r in circleci_workflows]

    jobs = []
    next_page_token = None

    for workflow_id in workflow_ids:
        while True:
            response = requests.get(
                f"https://circleci.com/api/v2/workflow/{workflow_id}/job",
                headers=headers
            )
            if response.status_code == 200:
                data = response.json()
                page_jobs = data.get("items", [])
                jobs.extend(page_jobs)
                next_page_token = data.get("next_page_token")
                if not next_page_token:
                    break
            else:
                raise ValueError(
                    f"Error fetching jobs for {workflow_id}: {response.status_code} - {response.text}")

    with pg.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS circleci_jobs (
                cancelled_by TEXT,
                job_number INTEGER,
                id TEXT PRIMARY KEY,
                started_at TIMESTAMP,
                name TEXT,
                approved_by TEXT,
                project_slug TEXT,
                status TEXT,
                type TEXT,
                stopped_at TIMESTAMP,
                approval_request_id TEXT
            )
        """)

        insert_sql = """
            INSERT INTO circleci_jobs (
                cancelled_by, 
                job_number, 
                id, 
                started_at, 
                name, 
                approved_by, 
                project_slug, 
                status, 
                type, 
                stopped_at,
                approval_request_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """

        rows = []
        for job in jobs:
            rows.append((
                job.get("cancelled_by"),
                job.get("job_number"),
                job.get("id"),
                job.get("started_at"),
                job.get("name"),
                job.get("approved_by"),
                job.get("project_slug"),
                job.get("status"),
                job.get("type"),
                job.get("stopped_at"),
                job.get("approval_request_id")
            ))
        try:
            conn.executemany(insert_sql, rows)
        except Exception:
            for r in rows:
                conn.execute(insert_sql, r)
    return jobs
