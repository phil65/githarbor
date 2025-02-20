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


async def get_repo_user_async(
    url: str,
    *,
    token: str | None = None,
    force_new: bool = False,
) -> User:
    """Get repository owner information."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.get_repo_user_async()


async def get_branch_async(
    url: str,
    name: str,
    *,
    token: str | None = None,
    force_new: bool = False,
) -> Branch:
    """Get branch information."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.get_branch_async(name)


async def get_pull_request_async(
    url: str,
    number: int,
    *,
    token: str | None = None,
    force_new: bool = False,
) -> PullRequest:
    """Get pull request by number."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.get_pull_request_async(number)


async def list_pull_requests_async(
    url: str,
    *,
    state: PullRequestState = "open",
    token: str | None = None,
    force_new: bool = False,
) -> list[PullRequest]:
    """List repository pull requests."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.list_pull_requests_async(state)


async def get_issue_async(
    url: str,
    issue_id: int,
    *,
    token: str | None = None,
    force_new: bool = False,
) -> Issue:
    """Get issue by ID."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.get_issue_async(issue_id)


async def list_issues_async(
    url: str,
    *,
    state: IssueState = "open",
    token: str | None = None,
    force_new: bool = False,
) -> list[Issue]:
    """List repository issues."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.list_issues_async(state)


async def get_commit_async(
    url: str,
    sha: str,
    *,
    token: str | None = None,
    force_new: bool = False,
) -> Commit:
    """Get commit by SHA."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
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
    token: str | None = None,
    force_new: bool = False,
) -> list[Commit]:
    """List repository commits."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.list_commits_async(
        branch=branch,
        since=since,
        until=until,
        author=author,
        path=path,
        max_results=max_results,
    )


async def get_workflow_async(
    url: str,
    workflow_id: str,
    *,
    token: str | None = None,
    force_new: bool = False,
) -> Workflow:
    """Get workflow by ID."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.get_workflow_async(workflow_id)


async def list_workflows_async(
    url: str,
    *,
    token: str | None = None,
    force_new: bool = False,
) -> list[Workflow]:
    """List repository workflows."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.list_workflows_async()


async def get_workflow_run_async(
    url: str,
    run_id: str,
    *,
    token: str | None = None,
    force_new: bool = False,
) -> WorkflowRun:
    """Get workflow run by ID."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.get_workflow_run_async(run_id)


async def download_async(
    url: str,
    path: str | os.PathLike[str],
    destination: str | os.PathLike[str],
    *,
    recursive: bool = False,
    token: str | None = None,
    force_new: bool = False,
) -> None:
    """Download repository content."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    await repo.download_async(path, destination, recursive)


async def search_commits_async(
    url: str,
    query: str,
    *,
    branch: str | None = None,
    path: str | None = None,
    max_results: int | None = None,
    token: str | None = None,
    force_new: bool = False,
) -> list[Commit]:
    """Search repository commits."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.search_commits_async(query, branch, path, max_results)


async def get_contributors_async(
    url: str,
    *,
    sort_by: Literal["commits", "name", "date"] = "commits",
    limit: int | None = None,
    token: str | None = None,
    force_new: bool = False,
) -> list[User]:
    """Get repository contributors."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.get_contributors_async(sort_by, limit)


async def get_languages_async(
    url: str,
    *,
    token: str | None = None,
    force_new: bool = False,
) -> dict[str, int]:
    """Get repository language statistics."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.get_languages_async()


async def compare_branches_async(
    url: str,
    base: str,
    head: str,
    *,
    include_commits: bool = True,
    include_files: bool = True,
    include_stats: bool = True,
    token: str | None = None,
    force_new: bool = False,
) -> dict[str, Any]:
    """Compare two branches."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.compare_branches_async(
        base, head, include_commits, include_files, include_stats
    )


async def get_latest_release_async(
    url: str,
    *,
    include_drafts: bool = False,
    include_prereleases: bool = False,
    token: str | None = None,
    force_new: bool = False,
) -> Release:
    """Get latest release."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.get_latest_release_async(include_drafts, include_prereleases)


async def list_releases_async(
    url: str,
    *,
    include_drafts: bool = False,
    include_prereleases: bool = False,
    limit: int | None = None,
    token: str | None = None,
    force_new: bool = False,
) -> list[Release]:
    """List releases."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.list_releases_async(include_drafts, include_prereleases, limit)


async def get_release_async(
    url: str,
    tag: str,
    *,
    token: str | None = None,
    force_new: bool = False,
) -> Release:
    """Get release by tag."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.get_release_async(tag)


async def get_tag_async(
    url: str,
    name: str,
    *,
    token: str | None = None,
    force_new: bool = False,
) -> Tag:
    """Get tag by name."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
    return await repo.get_tag_async(name)


async def list_tags_async(
    url: str,
    *,
    token: str | None = None,
    force_new: bool = False,
) -> list[Tag]:
    """List repository tags."""
    repo = RepoRegistry.get(url, token=token, force_new=force_new)
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


if __name__ == "__main__":

    async def main():
        workflows = await list_workflows_async("https://github.com/phil65/githarbor")
        print(workflows)

    import asyncio

    asyncio.run(main())
