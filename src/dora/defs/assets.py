import os
from typing import List
import dagster as dg
from dagster_duckdb import DuckDBResource
import requests

from dora.defs.ops import create_table, get_pipelines, get_workflows, make_batches
from dora.defs.resources import PostgresResource




@dg.asset(required_resource_keys={"pg", "github_token"})
def github_repos(context):
    github_token = context.resources.github_token
    pg = context.resources.pg
    github_org = context.resources.github_org

    if not github_token:
        raise ValueError("GITHUB_TOKEN environment variable is not set")

    headers = {
        "Authorization": f"Bearer {github_token}"
    }
    params = {
        "per_page": 100,
        "page": 1,
        "type": "private"
    }

    repos = []
    # Iterate pages until no more results
    while True:
        response = requests.get(
            f"https://api.github.com/orgs/{github_org}/repos",
            headers=headers,
            params=params,
        )
        response.raise_for_status()

        page_repos = response.json()
        if not page_repos:
            break

        repos.extend(page_repos)

        # If we received fewer than requested, we've reached the last page
        if len(page_repos) < params["per_page"]:
            break

        params["page"] += 1

    with pg.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS github_repos (
                id BIGINT PRIMARY KEY,
                name TEXT,
                full_name TEXT,
                private BOOLEAN,
                html_url TEXT,
                description TEXT,
                fork BOOLEAN,
                url TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                pushed_at TIMESTAMP,
                stargazers_count INTEGER,
                watchers_count INTEGER,
                language TEXT,
                forks_count INTEGER,
                open_issues_count INTEGER,
                default_branch TEXT,
                archived BOOLEAN
            )
        """)

        insert_sql = """
            INSERT INTO github_repos (
                id, name, full_name, private, html_url, description, fork, url,
                created_at, updated_at, pushed_at, stargazers_count, watchers_count,
                language, forks_count, open_issues_count, default_branch, archived
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (id) DO NOTHING
        """

        rows = []
        for repo in repos:
            rows.append((
                repo.get("id"),
                repo.get("name"),
                repo.get("full_name"),
                repo.get("private"),
                repo.get("html_url"),
                repo.get("description"),
                repo.get("fork"),
                repo.get("url"),
                repo.get("created_at"),
                repo.get("updated_at"),
                repo.get("pushed_at"),
                repo.get("stargazers_count"),
                repo.get("watchers_count"),
                repo.get("language"),
                repo.get("forks_count"),
                repo.get("open_issues_count"),
                repo.get("default_branch"),
                repo.get("archived"),
            ))

        try:
            conn.executemany(insert_sql, rows)
        except Exception:
            for r in rows:
                conn.execute(insert_sql, r)

    # Return the collected repos for downstream use or testing
    return repos


@dg.asset(
    deps=["github_repos"],
    required_resource_keys={"pg", "circleci_token"}
)
def circleci_projects(context):
    circleci_token = context.resources.circleci_token
    pg = context.resources.pg
    
    if not circleci_token:
        raise ValueError("CIRCLECI_TOKEN environment variable is not set")

    github_repo_names = set()

    with pg.connect() as conn:
        github_repos = conn.execute(
            "SELECT full_name FROM github_repos WHERE archived = FALSE").fetchall()
        github_repo_names = {r[0] for r in github_repos}

    headers = {
        "Circle-Token": circleci_token
    }

    projects = []

    for repo_name in github_repo_names:
        response = requests.get(
            f"https://circleci.com/api/v2/project/gh/{repo_name}",
            headers=headers
        )
        if response.status_code == 200:
            project = response.json()
            # Process the project data as needed
            print(f"Found CircleCI project for {repo_name}: {project['slug']}")
            projects.append(project)
        elif response.status_code == 404:
            print(f"No CircleCI project found for {repo_name}")
        else:
            print(
                f"Error fetching CircleCI project for {repo_name}: {response.status_code} - {response.text}")

    with pg.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS circleci_projects (
                slug TEXT,
                name TEXT,
                id TEXT,
                organization_name TEXT,
                organization_slug TEXT,
                vcs_url TEXT,
                provider TEXT,
                default_branch TEXT
            )
        """)

        insert_sql = """
            INSERT INTO circleci_projects (
            slug, name, id, organization_name, organization_slug, vcs_url, provider, default_branch
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        rows = []
        for project in projects:
            vcs_info = project.get("vcs_info", {})
            rows.append((
                project.get("slug"),
                project.get("name"),
                project.get("id"),
                project.get("organization_name"),
                project.get("organization_slug"),
                vcs_info.get("vcs_url"),
                vcs_info.get("provider"),
                vcs_info.get("default_branch"),
            ))
        try:
            conn.executemany(insert_sql, rows)
        except Exception:
            for r in rows:
                conn.execute(insert_sql, r)
    return projects


@dg.asset(
    deps=["circleci_projects"],
    required_resource_keys={"pg", "circleci_token"}
)
def circleci_pipelines(context):
    circleci_token = context.resources.circleci_token
    pg = context.resources.pg

    if not circleci_token:
        raise ValueError("CIRCLECI_TOKEN environment variable is not set")

    headers = {
        "Circle-Token": circleci_token
    }

    project_slugs = []

    with pg.connect() as conn:
        circleci_projects = conn.execute(
            "SELECT slug FROM circleci_projects").fetchall()
        project_slugs = [r[0] for r in circleci_projects]

    if len(project_slugs) == 0:
        raise ValueError(
            "No CircleCI projects found. Skipping pipeline fetch.")

    pipelines = []
    page_token = None

    params = {}

    for slug in project_slugs:
        page_token = None
        params = {}
        while True:
            if page_token:
                params["page-token"] = page_token
            
            response = requests.get(
                f"https://circleci.com/api/v2/project/{slug}/pipeline",
                headers=headers,
                params=params
            )
            if response.status_code == 200:
                data = response.json()
                page_pipelines = data.get("items", [])
                pipelines.extend(page_pipelines)
                page_token = data.get("next_page_token")
                if not page_token:
                    break
            else:
                raise ValueError(
                    f"Error fetching pipelines for {slug}: {response.status_code} - {response.text}")

    with pg.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS circleci_pipelines (
                id TEXT PRIMARY KEY,
                project_slug TEXT,
                number INTEGER,
                state TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                trigger_type TEXT,
                branch TEXT
            )
        """)

        insert_sql = """
            INSERT INTO circleci_pipelines (
                id, project_slug, number, state, created_at, updated_at,
                trigger_type, branch
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """

        rows = []
        for pipeline in pipelines:
            actor = pipeline.get("trigger", {}).get("actor", {})
            rows.append((
                pipeline.get("id"),
                pipeline.get("project_slug"),
                pipeline.get("number"),
                pipeline.get("state"),
                pipeline.get("created_at"),
                pipeline.get("updated_at"),
                pipeline.get("trigger", {}).get("type"),
                pipeline.get("vcs", {}).get("branch")
            ))
        try:
            conn.executemany(insert_sql, rows)
        except Exception:
            for r in rows:
                conn.execute(insert_sql, r)
    return pipelines

@dg.graph
def fetch_and_store_workflows(pipeline_ids: List[str]) -> None:
    batches = make_batches(pipeline_ids)
    batches.map(get_workflows)
    
    return None

@dg.job(
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