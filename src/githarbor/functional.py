from __future__ import annotations

import asyncio
import functools
from typing import TYPE_CHECKING, Any, Literal, ParamSpec, TypeVar

from githarbor.registry import RepoRegistry


if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime
    import os

    from githarbor.core.base import IssueState, PullRequestState
    from githarbor.core.models import (
        Branch,
        Commit,
        Issue,
        PullRequest,
        Release,
        Tag,
        User,
        Workflow,
        WorkflowRun,
    )

P = ParamSpec("P")
T = TypeVar("T")


def make_sync(async_func: Callable[P, T]) -> Callable[P, T]:
    """Convert an async function to sync using asyncio.run()."""

    @functools.wraps(async_func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        return asyncio.run(async_func(*args, **kwargs))  # type: ignore[arg-type]

    return wrapper


async def get_repo_user_async(url: str) -> User:
    """Get repository owner information.

    Args:
        url: Repository URL
    """
    repo = RepoRegistry.get(url)
    return await repo.get_repo_user_async()


async def get_branch_async(url: str, name: str) -> Branch:
    """Get information about a specific repository branch.

    Args:
        url: Repository URL
        name: Branch name
    """
    repo = RepoRegistry.get(url)
    return await repo.get_branch_async(name)


async def get_pull_request_async(url: str, number: int) -> PullRequest:
    """Get information about a specific pull request.

    Args:
        url: Repository URL
        number: Pull request number
    """
    repo = RepoRegistry.get(url)
    return await repo.get_pull_request_async(number)


async def list_pull_requests_async(
    url: str, *, state: PullRequestState = "open"
) -> list[PullRequest]:
    """List repository pull requests.

    Args:
        url: Repository URL
        state: Pull request state filter ('open', 'closed', 'all')
    """
    repo = RepoRegistry.get(url)
    return await repo.list_pull_requests_async(state)


async def get_issue_async(url: str, issue_id: int) -> Issue:
    """Get information about a specific issue.

    Args:
        url: Repository URL
        issue_id: Issue number
    """
    repo = RepoRegistry.get(url)
    return await repo.get_issue_async(issue_id)


async def list_issues_async(url: str, *, state: IssueState = "open") -> list[Issue]:
    """List repository issues.

    Args:
        url: Repository URL
        state: Issue state filter ('open', 'closed', 'all')
    """
    repo = RepoRegistry.get(url)
    return await repo.list_issues_async(state)


async def get_commit_async(url: str, sha: str) -> Commit:
    """Get information about a specific commit.

    Args:
        url: Repository URL
        sha: Commit SHA
    """
    repo = RepoRegistry.get(url)
    return await repo.get_commit_async(sha)


async def list_commits_async(
    url: str,
    *,
    branch: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    author: str | None = None,
    path: str | None = None,
    max_results: int | None = None,
) -> list[Commit]:
    """List repository commits with optional filters.

    Args:
        url: Repository URL
        branch: Filter by branch name
        since: Only show commits after this date
        until: Only show commits before this date
        author: Filter by author
        path: Filter by file path
        max_results: Maximum number of results
    """
    repo = RepoRegistry.get(url)
    return await repo.list_commits_async(
        branch=branch,
        since=since,
        until=until,
        author=author,
        path=path,
        max_results=max_results,
    )


async def get_workflow_async(url: str, workflow_id: str) -> Workflow:
    """Get information about a specific workflow.

    Args:
        url: Repository URL
        workflow_id: Workflow identifier
    """
    repo = RepoRegistry.get(url)
    return await repo.get_workflow_async(workflow_id)


async def list_workflows_async(url: str) -> list[Workflow]:
    """List repository workflows.

    Args:
        url: Repository URL
    """
    repo = RepoRegistry.get(url)
    return await repo.list_workflows_async()


async def get_workflow_run_async(url: str, run_id: str) -> WorkflowRun:
    """Get information about a specific workflow run.

    Args:
        url: Repository URL
        run_id: Workflow run identifier
    """
    repo = RepoRegistry.get(url)
    return await repo.get_workflow_run_async(run_id)


async def download_async(
    url: str,
    path: str | os.PathLike[str],
    destination: str | os.PathLike[str],
    *,
    recursive: bool = False,
) -> None:
    """Download repository content.

    Args:
        url: Repository URL
        path: Path to download
        destination: Where to save the downloaded content
        recursive: Whether to download recursively
    """
    repo = RepoRegistry.get(url)
    await repo.download_async(path, destination, recursive)


async def search_commits_async(
    url: str,
    query: str,
    *,
    branch: str | None = None,
    path: str | None = None,
    max_results: int | None = None,
) -> list[Commit]:
    """Search repository commits.

    Args:
        url: Repository URL
        query: Search query string
        branch: Filter by branch name
        path: Filter by file path
        max_results: Maximum number of results
    """
    repo = RepoRegistry.get(url)
    return await repo.search_commits_async(query, branch, path, max_results)


async def get_contributors_async(
    url: str,
    *,
    sort_by: Literal["commits", "name", "date"] = "commits",
    limit: int | None = None,
) -> list[User]:
    """Get repository contributors.

    Args:
        url: Repository URL
        sort_by: How to sort the contributors
        limit: Maximum number of contributors to return
    """
    repo = RepoRegistry.get(url)
    return await repo.get_contributors_async(sort_by, limit)


async def get_languages_async(url: str) -> dict[str, int]:
    """Get repository language statistics.

    Args:
        url: Repository URL
    """
    repo = RepoRegistry.get(url)
    return await repo.get_languages_async()


async def compare_branches_async(
    url: str,
    base: str,
    head: str,
    *,
    include_commits: bool = True,
    include_files: bool = True,
    include_stats: bool = True,
) -> dict[str, Any]:
    """Compare two branches.

    Args:
        url: Repository URL
        base: Base branch name
        head: Head branch name
        include_commits: Whether to include commit information
        include_files: Whether to include changed files
        include_stats: Whether to include statistics
    """
    repo = RepoRegistry.get(url)
    return await repo.compare_branches_async(
        base, head, include_commits, include_files, include_stats
    )


async def get_latest_release_async(
    url: str,
    *,
    include_drafts: bool = False,
    include_prereleases: bool = False,
) -> Release:
    """Get latest repository release.

    Args:
        url: Repository URL
        include_drafts: Whether to include draft releases
        include_prereleases: Whether to include pre-releases
    """
    repo = RepoRegistry.get(url)
    return await repo.get_latest_release_async(include_drafts, include_prereleases)


async def list_releases_async(
    url: str,
    *,
    include_drafts: bool = False,
    include_prereleases: bool = False,
    limit: int | None = None,
) -> list[Release]:
    """List repository releases.

    Args:
        url: Repository URL
        include_drafts: Whether to include draft releases
        include_prereleases: Whether to include pre-releases
        limit: Maximum number of releases to return
    """
    repo = RepoRegistry.get(url)
    return await repo.list_releases_async(include_drafts, include_prereleases, limit)


async def get_release_async(url: str, tag: str) -> Release:
    """Get release by tag.

    Args:
        url: Repository URL
        tag: Release tag name
    """
    repo = RepoRegistry.get(url)
    return await repo.get_release_async(tag)


async def get_tag_async(url: str, name: str) -> Tag:
    """Get tag information.

    Args:
        url: Repository URL
        name: Tag name
    """
    repo = RepoRegistry.get(url)
    return await repo.get_tag_async(name)


async def list_tags_async(url: str) -> list[Tag]:
    """List repository tags.

    Args:
        url: Repository URL
    """
    repo = RepoRegistry.get(url)
    return await repo.list_tags_async()


get_repo_user = make_sync(get_repo_user_async)
get_branch = make_sync(get_branch_async)
get_pull_request = make_sync(get_pull_request_async)
list_pull_requests = make_sync(list_pull_requests_async)
get_issue = make_sync(get_issue_async)
list_issues = make_sync(list_issues_async)
get_commit = make_sync(get_commit_async)
list_commits = make_sync(list_commits_async)
get_workflow = make_sync(get_workflow_async)
list_workflows = make_sync(list_workflows_async)
get_workflow_run = make_sync(get_workflow_run_async)
download = make_sync(download_async)
search_commits = make_sync(search_commits_async)
get_contributors = make_sync(get_contributors_async)
get_languages = make_sync(get_languages_async)
compare_branches = make_sync(compare_branches_async)
get_latest_release = make_sync(get_latest_release_async)
list_releases = make_sync(list_releases_async)
get_release = make_sync(get_release_async)
get_tag = make_sync(get_tag_async)
list_tags = make_sync(list_tags_async)


def setup_env(env: Any) -> None:
    """Used as extension point for the jinjarope environment.

    Args:
        env: The jinjarope environment to extend
    """
    funcs = {
        # Async functions
        "get_repo_user_async": get_repo_user_async,
        "get_branch_async": get_branch_async,
        "get_pull_request_async": get_pull_request_async,
        "list_pull_requests_async": list_pull_requests_async,
        "get_issue_async": get_issue_async,
        "list_issues_async": list_issues_async,
        "get_commit_async": get_commit_async,
        "list_commits_async": list_commits_async,
        "get_workflow_async": get_workflow_async,
        "list_workflows_async": list_workflows_async,
        "get_workflow_run_async": get_workflow_run_async,
        "download_from_repo_async": download_async,
        "search_commits_async": search_commits_async,
        "get_contributors_async": get_contributors_async,
        "get_languages_async": get_languages_async,
        "compare_branches_async": compare_branches_async,
        "get_latest_release_async": get_latest_release_async,
        "list_releases_async": list_releases_async,
        "get_release_async": get_release_async,
        "get_tag_async": get_tag_async,
        "list_tags_async": list_tags_async,
        # Sync functions
        "get_repo_user": get_repo_user,
        "get_branch": get_branch,
        "get_pull_request": get_pull_request,
        "list_pull_requests": list_pull_requests,
        "get_issue": get_issue,
        "list_issues": list_issues,
        "get_commit": get_commit,
        "list_commits": list_commits,
        "get_workflow": get_workflow,
        "list_workflows": list_workflows,
        "get_workflow_run": get_workflow_run,
        "download_from_repo": download,
        "search_commits": search_commits,
        "get_contributors": get_contributors,
        "get_languages": get_languages,
        "compare_branches": compare_branches,
        "get_latest_release": get_latest_release,
        "list_releases": list_releases,
        "get_release": get_release,
        "get_tag": get_tag,
        "list_tags": list_tags,
    }

    # Register as both globals and filters
    env.globals |= funcs
    env.filters |= funcs


if __name__ == "__main__":

    async def main():
        workflows = await list_workflows_async("https://github.com/phil65/githarbor")
        print(workflows)

    import asyncio

    asyncio.run(main())
