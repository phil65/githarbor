# GitHarbor

[![PyPI License](https://img.shields.io/pypi/l/githarbor.svg)](https://pypi.org/project/githarbor/)
[![Package status](https://img.shields.io/pypi/status/githarbor.svg)](https://pypi.org/project/githarbor/)
[![Monthly downloads](https://img.shields.io/pypi/dm/githarbor.svg)](https://pypi.org/project/githarbor/)
[![Distribution format](https://img.shields.io/pypi/format/githarbor.svg)](https://pypi.org/project/githarbor/)
[![Wheel availability](https://img.shields.io/pypi/wheel/githarbor.svg)](https://pypi.org/project/githarbor/)
[![Python version](https://img.shields.io/pypi/pyversions/githarbor.svg)](https://pypi.org/project/githarbor/)
[![Implementation](https://img.shields.io/pypi/implementation/githarbor.svg)](https://pypi.org/project/githarbor/)
[![Releases](https://img.shields.io/github/downloads/phil65/githarbor/total.svg)](https://github.com/phil65/githarbor/releases)
[![Github Contributors](https://img.shields.io/github/contributors/phil65/githarbor)](https://github.com/phil65/githarbor/graphs/contributors)
[![Github Discussions](https://img.shields.io/github/discussions/phil65/githarbor)](https://github.com/phil65/githarbor/discussions)
[![Github Forks](https://img.shields.io/github/forks/phil65/githarbor)](https://github.com/phil65/githarbor/forks)
[![Github Issues](https://img.shields.io/github/issues/phil65/githarbor)](https://github.com/phil65/githarbor/issues)
[![Github Issues](https://img.shields.io/github/issues-pr/phil65/githarbor)](https://github.com/phil65/githarbor/pulls)
[![Github Watchers](https://img.shields.io/github/watchers/phil65/githarbor)](https://github.com/phil65/githarbor/watchers)
[![Github Stars](https://img.shields.io/github/stars/phil65/githarbor)](https://github.com/phil65/githarbor/stars)
[![Github Repository size](https://img.shields.io/github/repo-size/phil65/githarbor)](https://github.com/phil65/githarbor)
[![Github last commit](https://img.shields.io/github/last-commit/phil65/githarbor)](https://github.com/phil65/githarbor/commits)
[![Github release date](https://img.shields.io/github/release-date/phil65/githarbor)](https://github.com/phil65/githarbor/releases)
[![Github language count](https://img.shields.io/github/languages/count/phil65/githarbor)](https://github.com/phil65/githarbor)
[![Github commits this month](https://img.shields.io/github/commit-activity/m/phil65/githarbor)](https://github.com/phil65/githarbor)
[![Package status](https://codecov.io/gh/phil65/githarbor/branch/main/graph/badge.svg)](https://codecov.io/gh/phil65/githarbor/)
[![PyUp](https://pyup.io/repos/github/phil65/githarbor/shield.svg)](https://pyup.io/repos/github/phil65/githarbor/)

[Read the documentation!](https://phil65.github.io/githarbor/)

GitHarbor is a unified Python client for Git hosting platforms. One API covers repositories on
[GitHub](https://github.com), [GitLab](https://gitlab.com),
[Azure DevOps](https://azure.microsoft.com/products/devops), [Gitea](https://gitea.io) /
[Codeberg](https://codeberg.org) and local Git checkouts: pass a URL (or path), get a repository
object, and work with branches, commits, issues, pull requests, releases and files without
caring which backend is behind it.

## Key features

- **One API, many hosts**: `create_repository(url)` picks the provider from the URL.
- **Typed results**: every provider returns the same dataclasses (`Branch`, `Commit`, `Issue`,
  `PullRequest`, `Release`, `Tag`, `User`, `Workflow`, `WorkflowRun`).
- **Sync and async**: every method has an `*_async` twin (`list_issues` / `list_issues_async`).
- **Functional API**: module-level functions that take a repository URL (`githarbor.functional`).
- **MCP server**: expose repository operations to LLM agents via the `githarbor` command.
- **Pick your SDKs**: each backend is an optional extra, nothing heavy is installed by default.

## Installation

Install GitHarbor with the extras for the platforms you need:

```bash
pip install "githarbor[github]"          # GitHub via PyGithub
pip install "githarbor[github,gitlab]"   # several providers
pip install "githarbor[all]"             # all providers, diff parsing and the MCP server
```

| Extra | Provides |
|-------|----------|
| `github` | GitHub (PyGithub) |
| `aiogithubapi` | GitHub (async, aiogithubapi) |
| `gitlab` | GitLab (python-gitlab) |
| `gitea` / `pygitea` | Gitea and Codeberg (giteapy / py-gitea) |
| `azure` | Azure DevOps |
| `local` | Local repositories (GitPython) |
| `diff` | Creating pull requests from unified diffs (unidiff) |
| `mcp` | MCP server (FastMCP) |

## Quickstart

```python
from githarbor import create_repository

repo = create_repository("https://github.com/phil65/githarbor")

print(repo.name, repo.default_branch)
for branch in repo.list_branches():
    print(branch.name)

latest = repo.get_latest_release()
print(latest.tag_name, latest.published_at)
```

Credentials are passed as keyword arguments or read from environment variables
(`GITHUB_TOKEN`, `GITLAB_TOKEN`, `GITEA_TOKEN`, `AZURE_DEVOPS_PAT`):

```python
from githarbor import create_repository

github_repo = create_repository("https://github.com/owner/repo", token="github_pat_...")
gitlab_repo = create_repository("https://gitlab.com/owner/repo", token="glpat-...")
azure_repo = create_repository("https://dev.azure.com/org/project/_git/repo", token="azure_pat")
local_repo = create_repository("/path/to/checkout")  # needs the "local" extra
```

> [!TIP]
> Use personal access tokens (PATs) and keep them out of your source code.

## Working with repositories

### Repository information

```python
# Language statistics, e.g. {"Python": 10000, "JavaScript": 5000}
languages = repo.get_languages()

# Activity counts over the last 30 days
activity = repo.get_recent_activity(
    days=30, include_commits=True, include_prs=True, include_issues=True
)
```

### Branches and tags

```python
branches = repo.list_branches()
main_branch = repo.get_branch("main")

tags = repo.list_tags()
tag = repo.get_tag("v1.0.0")

diff = repo.compare_branches(
    base="main", head="feature", include_commits=True, include_files=True, include_stats=True
)
```

### Commits

```python
from datetime import datetime

commit = repo.get_commit("abcd1234")

commits = repo.list_commits(
    branch="main",
    since=datetime(2024, 1, 1),
    until=datetime(2024, 2, 1),
    author="username",
    path="src/",
    max_results=100,
)

results = repo.search_commits(query="fix bug", branch="main", max_results=10)
```

### Issues and pull requests

```python
open_issues = repo.list_issues(state="open")
issue = repo.get_issue(123)
new_issue = repo.create_issue(title="Bug report", body="Steps to reproduce...", labels=["bug"])

prs = repo.list_pull_requests(state="open")  # or "closed" or "all"
pr = repo.get_pull_request(456)
```

### Releases

```python
latest = repo.get_latest_release(include_drafts=False, include_prereleases=False)
releases = repo.list_releases(include_drafts=False, include_prereleases=True, limit=10)
release = repo.get_release("v1.0.0")
```

### Repository content

```python
# Download a single file or a whole directory
repo.download(path="README.md", destination="local/")
repo.download(path="src/", destination="local/src", recursive=True)

# Iterate over files
for file_path in repo.iter_files(path="src/", ref="main", pattern="*.py"):
    print(file_path)
```

### CI/CD workflows

```python
workflows = repo.list_workflows()
workflow = repo.get_workflow("workflow_id")
run = repo.get_workflow_run("run_id")
```

### Contributors

```python
contributors = repo.get_contributors(sort_by="commits", limit=10)  # or "name" / "date"
```

### Async usage

Every operation has an async counterpart:

```python
import asyncio

from githarbor import create_repository


async def main() -> None:
    repo = create_repository("https://github.com/phil65/githarbor")
    issues, releases = await asyncio.gather(
        repo.list_issues_async(state="open"),
        repo.list_releases_async(limit=5),
    )
    print(len(issues), [r.tag_name for r in releases])


asyncio.run(main())
```

## Owners

`create_owner()` works with users and organizations (GitHub):

```python
from githarbor.users import create_owner

owner = create_owner("https://github.com/phil65")
for repository in owner.list_repositories():
    print(repository.name)
```

## Functional API

`githarbor.functional` offers the same operations as plain functions that take the repository
URL first. Repository instances are cached per URL.

```python
from githarbor.functional import get_latest_release, list_issues_async

release = get_latest_release("https://github.com/phil65/githarbor")
issues = await list_issues_async(  # inside an async function
    "https://github.com/phil65/githarbor", state="open"
)
```

These functions are also registered as Jinja globals and filters for
[jinjarope](https://github.com/phil65/jinjarope) environments.

## MCP server

With the `mcp` extra, GitHarbor runs as an [MCP](https://modelcontextprotocol.io) server:

```bash
githarbor                                  # stdio transport
githarbor --transport sse --port 8000      # SSE transport
githarbor --repo-path https://github.com/owner/repo  # tools bound to one repository
```

## Error handling

```python
from githarbor import create_repository
from githarbor.exceptions import (
    FeatureNotSupportedError,
    RepositoryNotFoundError,
    ResourceNotFoundError,
)

try:
    repo = create_repository("https://github.com/nonexistent/repo")
except RepositoryNotFoundError:
    print("Repository does not exist or no provider supports the URL")

try:
    issue = repo.get_issue(999999)
except ResourceNotFoundError:
    print("Issue does not exist")

try:
    workflows = repo.list_workflows()
except FeatureNotSupportedError:
    print("This provider does not support workflows")
```

> [!NOTE]
> Not every platform supports every operation. Unsupported operations raise
> `FeatureNotSupportedError`.

> [!IMPORTANT]
> Mind the API rate limits of the hosting platforms when making many requests.
