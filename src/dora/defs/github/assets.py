import dagster as dg
import requests

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
