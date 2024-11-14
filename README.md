# GitHarbor

[![PyPI License](https://img.shields.io/pypi/l/githarbor.svg)](https://pypi.org/project/githarbor/)
[![Package status](https://img.shields.io/pypi/status/githarbor.svg)](https://pypi.org/project/githarbor/)
[![Daily downloads](https://img.shields.io/pypi/dd/githarbor.svg)](https://pypi.org/project/githarbor/)
[![Weekly downloads](https://img.shields.io/pypi/dw/githarbor.svg)](https://pypi.org/project/githarbor/)
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
[![Github commits this week](https://img.shields.io/github/commit-activity/w/phil65/githarbor)](https://github.com/phil65/githarbor)
[![Github commits this month](https://img.shields.io/github/commit-activity/m/phil65/githarbor)](https://github.com/phil65/githarbor)
[![Github commits this year](https://img.shields.io/github/commit-activity/y/phil65/githarbor)](https://github.com/phil65/githarbor)
[![Package status](https://codecov.io/gh/phil65/githarbor/branch/main/graph/badge.svg)](https://codecov.io/gh/phil65/githarbor/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![PyUp](https://pyup.io/repos/github/phil65/githarbor/shield.svg)](https://pyup.io/repos/github/phil65/githarbor/)

[Read the documentation!](https://phil65.github.io/githarbor/)

# GitHarbor User Guide

GitHarbor is a unified interface for interacting with Git hosting platforms. It provides a consistent API to work with repositories hosted on [GitHub](https://github.com), [GitLab](https://gitlab.com), [Azure DevOps](https://azure.microsoft.com/products/devops), [Gitea](https://gitea.io), [CodeBerg](https://codeberg.org) and [Bitbucket](https://bitbucket.org).

## Getting Started

The main entry point is the `create_repository()` function which accepts a repository URL and platform-specific credentials:

```python
from githarbor import create_repository

# GitHub repository
repo = create_repository("https://github.com/owner/repo", token="github_pat_...")

# GitLab repository
repo = create_repository("https://gitlab.com/owner/repo", token="glpat-...")

# Azure DevOps repository
repo = create_repository(
    "https://dev.azure.com/org/project/_git/repo",
    token="azure_pat"
)
```

> [!TIP]
> Always use personal access tokens (PATs) for authentication. Never hardcode tokens in your source code.

## Working with Repositories

### Basic Repository Information

```python
# Get repository name and default branch
print(repo.name)
print(repo.default_branch)

# Get language statistics
languages = repo.get_languages()
# Returns: {"Python": 10000, "JavaScript": 5000}

# Get recent activity statistics
activity = repo.get_recent_activity(
    days=30,
    include_commits=True,
    include_prs=True,
    include_issues=True
)
```

### Branches and Tags

```python
# List all branches
branches = repo.list_branches()

# Get specific branch
main_branch = repo.get_branch("main")

# List all tags
tags = repo.list_tags()

# Get specific tag
tag = repo.get_tag("v1.0.0")

# Compare branches
diff = repo.compare_branches(
    base="main",
    head="feature",
    include_commits=True,
    include_files=True,
    include_stats=True
)
```

### Commits

```python
# Get specific commit
commit = repo.get_commit("abcd1234")

# List commits with filters
commits = repo.list_commits(
    branch="main",
    since=datetime(2024, 1, 1),
    until=datetime(2024, 2, 1),
    author="username",
    path="src/",
    max_results=100
)

# Search commits
results = repo.search_commits(
    query="fix bug",
    branch="main",
    path="src/",
    max_results=10
)
```

### Issues and Pull Requests

```python
# List open issues
open_issues = repo.list_issues(state="open")

# Get specific issue
issue = repo.get_issue(123)

# List pull requests
prs = repo.list_pull_requests(state="open")  # or "closed" or "all"

# Get specific pull request
pr = repo.get_pull_request(456)
```

### Releases

```python
# Get latest release
latest = repo.get_latest_release(
    include_drafts=False,
    include_prereleases=False
)

# List all releases
releases = repo.list_releases(
    include_drafts=False,
    include_prereleases=True,
    limit=10
)

# Get specific release
release = repo.get_release("v1.0.0")
```

### Repository Content

```python
# Download single file
repo.download(
    path="README.md",
    destination="local/README.md"
)

# Download directory recursively
repo.download(
    path="src/",
    destination="local/src",
    recursive=True
)

# Iterate through files
for file_path in repo.iter_files(
    path="src/",
    ref="main",
    pattern="*.py"
):
    print(file_path)
```

### CI/CD Workflows

```python
# List all workflows
workflows = repo.list_workflows()

# Get specific workflow
workflow = repo.get_workflow("workflow_id")

# Get specific workflow run
run = repo.get_workflow_run("run_id")
```

### Contributors

```python
# Get repository contributors
contributors = repo.get_contributors(
    sort_by="commits",  # or "name" or "date"
    limit=10
)
```

## Error Handling

GitHarbor provides specific exceptions for different error cases:

```python
from githarbor.exceptions import (
    RepositoryNotFoundError,
    ResourceNotFoundError,
    FeatureNotSupportedError
)

try:
    repo = create_repository("https://github.com/nonexistent/repo")
except RepositoryNotFoundError:
    print("Repository does not exist")

try:
    issue = repo.get_issue(999999)
except ResourceNotFoundError:
    print("Issue does not exist")

try:
    repo.some_unsupported_method()
except FeatureNotSupportedError:
    print("This feature is not supported by this repository provider")
```

> [!NOTE]
> Not all features are supported by all platforms. Operations that aren't supported will raise `FeatureNotSupportedError`.

> [!IMPORTANT]
> Be mindful of API rate limits when making many requests. Consider implementing retries and delays in your code.
