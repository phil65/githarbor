"""GitHarbor MCP Server - Expose GitHarbor functionality through MCP."""

from __future__ import annotations

import argparse
import dataclasses
import functools
import os
import tempfile
from typing import Any, Literal

from fastmcp import FastMCP

from githarbor.exceptions import ResourceNotFoundError
from githarbor.functional import (
    add_pull_request_comment,
    create_branch,
    create_issue,
    create_pull_request,
    get_branch,
    get_commit,
    get_contributors,
    get_issue,
    get_languages,
    get_latest_release,
    get_pull_request,
    get_release,
    get_repo_user,
    get_tag,
    list_branches,
    list_commits,
    list_issues,
    list_pull_requests,
    list_releases,
    list_tags,
)


# Create the MCP server
mcp = FastMCP[Any]("GitHarbor")


# Expose functional tools as MCP tools
@mcp.tool()
async def gh_get_repo_user(repo_url: str) -> dict:
    """Get information about the owner of a repository."""
    user = await get_repo_user(repo_url)
    return dataclasses.asdict(user)


@mcp.tool()
async def gh_get_branch(repo_url: str, branch_name: str) -> dict:
    """Get information about a specific branch in a repository."""
    branch = await get_branch(repo_url, branch_name)
    return dataclasses.asdict(branch)


@mcp.tool()
async def gh_list_branches(repo_url: str) -> list[dict]:
    """List all branches in a repository."""
    branches = await list_branches(repo_url)
    return [dataclasses.asdict(branch) for branch in branches]


@mcp.tool()
async def gh_get_pull_request(repo_url: str, number: int) -> dict:
    """Get details about a specific pull request."""
    pr = await get_pull_request(repo_url, number)
    return dataclasses.asdict(pr)


@mcp.tool()
async def gh_list_pull_requests(
    repo_url: str,
    state: Literal["open", "closed", "all"] = "open",
) -> list[dict]:
    """List pull requests in a repository with optional state filter."""
    prs = await list_pull_requests(repo_url, state=state)
    return [dataclasses.asdict(pr) for pr in prs]


@mcp.tool()
async def gh_get_issue(repo_url: str, issue_id: int) -> dict:
    """Get details about a specific issue."""
    issue = await get_issue(repo_url, issue_id)
    return dataclasses.asdict(issue)


@mcp.tool()
async def gh_list_issues(
    repo_url: str, state: Literal["open", "closed", "all"] = "open"
) -> list[dict]:
    """List issues in a repository with optional state filter."""
    issues = await list_issues(repo_url, state=state)
    return [dataclasses.asdict(issue) for issue in issues]


@mcp.tool()
async def gh_create_issue(
    repo_url: str,
    title: str,
    body: str,
    labels: list[str] | None = None,
    assignees: list[str] | None = None,
) -> dict:
    """Create a new issue in a repository."""
    issue = await create_issue(repo_url, title, body, labels=labels, assignees=assignees)
    return dataclasses.asdict(issue)


@mcp.tool()
async def gh_get_commit(repo_url: str, sha: str) -> dict:
    """Get details about a specific commit."""
    commit = await get_commit(repo_url, sha)
    return dataclasses.asdict(commit)


@mcp.tool()
async def gh_list_commits(
    repo_url: str,
    branch: str | None = None,
    max_results: int | None = None,
) -> list[dict]:
    """List commits in a repository with optional branch and limit."""
    commits = await list_commits(repo_url, branch=branch, max_results=max_results)
    return [dataclasses.asdict(commit) for commit in commits]


@mcp.tool()
async def gh_get_languages(repo_url: str) -> dict[str, int]:
    """Get programming language statistics for a repository."""
    return await get_languages(repo_url)


@mcp.tool()
async def gh_get_contributors(
    repo_url: str,
    sort_by: Literal["commits", "name", "date"] = "commits",
    limit: int | None = None,
) -> list[dict]:
    """Get contributors to a repository."""
    contributors = await get_contributors(repo_url, sort_by=sort_by, limit=limit)
    return [dataclasses.asdict(contributor) for contributor in contributors]


@mcp.tool()
async def gh_create_branch(repo_url: str, name: str, base_commit: str) -> dict:
    """Create a new branch in a repository."""
    branch = await create_branch(repo_url, name, base_commit)
    return dataclasses.asdict(branch)


@mcp.tool()
async def gh_create_pull_request(
    repo_url: str,
    title: str,
    body: str,
    head_branch: str,
    base_branch: str,
    draft: bool = False,
) -> dict:
    """Create a new pull request in a repository."""
    pr = await create_pull_request(repo_url, title, body, head_branch, base_branch, draft)
    return dataclasses.asdict(pr)


# @mcp.tool()
# async def gh_create_pull_request_from_diff(
#     repo_url: str,
#     title: str,
#     body: str,
#     base_branch: str,
#     diff: str,
#     head_branch: str | None = None,
#     draft: bool = False,
# ) -> dict:
#     """Create a pull request from a diff string."""
#     pr = await create_pull_request_from_diff(repo_url, title, body, base_branch, diff, head_branch, draft)  # noqa: E501
#     return dataclasses.asdict(pr)


@mcp.tool()
async def gh_add_pull_request_comment(repo_url: str, number: int, body: str) -> dict:
    """Add a comment to a pull request."""
    comment = await add_pull_request_comment(repo_url, number, body)
    return dataclasses.asdict(comment)


@mcp.tool()
async def gh_get_tag(repo_url: str, name: str) -> dict:
    """Get information about a specific tag."""
    tag = await get_tag(repo_url, name)
    return dataclasses.asdict(tag)


@mcp.tool()
async def gh_list_tags(repo_url: str) -> list[dict]:
    """List all tags in a repository."""
    tags = await list_tags(repo_url)
    return [dataclasses.asdict(tag) for tag in tags]


@mcp.tool()
async def gh_get_latest_release(
    repo_url: str,
    include_drafts: bool = False,
    include_prereleases: bool = False,
) -> dict:
    """Get the latest release from a repository."""
    release = await get_latest_release(
        repo_url, include_drafts=include_drafts, include_prereleases=include_prereleases
    )
    return dataclasses.asdict(release)


@mcp.tool()
async def gh_list_releases(
    repo_url: str,
    include_drafts: bool = False,
    include_prereleases: bool = False,
    limit: int | None = None,
) -> list[dict]:
    """List releases from a repository."""
    releases = await list_releases(
        repo_url,
        include_drafts=include_drafts,
        include_prereleases=include_prereleases,
        limit=limit,
    )
    return [dataclasses.asdict(release) for release in releases]


@mcp.tool()
async def gh_get_release(repo_url: str, tag: str) -> dict:
    """Get a specific release by tag name."""
    release = await get_release(repo_url, tag)
    return dataclasses.asdict(release)


@mcp.resource("repo://{repo_url}/files/{path}")
async def get_file_content(repo_url: str, path: str) -> str:
    """Get the content of a file from a repository."""
    import upath

    from githarbor import create_repository
    from githarbor.exceptions import ResourceNotFoundError

    try:
        repo = create_repository(repo_url)

        # Create a temporary directory to download the file
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = upath.UPath(temp_dir)

            # Download the file
            repo.download(path, temp_path, recursive=False)

            # Read the file content
            file_path = temp_path / os.path.basename(path)  # noqa: PTH119
            if not file_path.exists():
                msg = f"File not found: {path}"
                raise ResourceNotFoundError(msg)  # noqa: TRY301

            return file_path.read_text(errors="replace")
    except Exception as e:  # noqa: BLE001
        msg = f"Error fetching file content: {e}"
        raise ResourceNotFoundError(msg)  # noqa: B904


# Add resources for commit history
@mcp.resource("repo://{repo_url}/commit_history")
async def get_commit_history(repo_url: str, max_results: int = 10) -> list[dict]:
    """Get the commit history for a repository."""
    try:
        commits = await list_commits(repo_url, max_results=max_results)
        return [dataclasses.asdict(commit) for commit in commits]
    except Exception as e:  # noqa: BLE001
        msg = f"Error fetching commit history: {e}"
        raise ResourceNotFoundError(msg)  # noqa: B904


# Add resources for issues list
@mcp.resource("repo://{repo_url}/issues/{state}")
async def get_issues_list(repo_url: str, state: str) -> list[dict]:  # IssueState
    """Get a list of issues for a repository filtered by state.

    State can be one of "open", "closed", or "all".
    """
    try:
        issues = await list_issues(repo_url, state=state)  # type: ignore
        return [dataclasses.asdict(issue) for issue in issues]
    except Exception as e:  # noqa: BLE001
        msg = f"Error fetching issues: {e}"
        raise ResourceNotFoundError(msg) from None


# Add resources for pull requests list
@mcp.resource("repo://{repo_url}/pull_requests/{state}")
async def get_pull_requests_list(
    repo_url: str,
    state: str,  # PullRequestState
) -> list[dict]:
    """Get a list of pull requests for a repository filtered by state."""
    try:
        prs = await list_pull_requests(repo_url, state=state)  # type: ignore
        return [dataclasses.asdict(pr) for pr in prs]
    except Exception as e:  # noqa: BLE001
        msg = f"Error fetching pull requests: {e}"
        raise ResourceNotFoundError(msg)  # noqa: B904


# Create a function to handle CLI arguments
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GitHarbor MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
        help="Transport protocol to use (default: stdio)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to use for SSE transport (default: 8000)",
    )
    parser.add_argument(
        "--repo-path", help="Optional repository URL to focus server on a specific repo"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Run the server with the specified transport
    if args.repo_path:
        # Create a repository instance for the fixed path
        from githarbor import create_repository

        # Create a repository for the specified path
        fixed_repo = create_repository(args.repo_path)
        fixed_repo_mcp = FastMCP[Any](f"GitHarbor - {args.repo_path}")

        # Get all async methods from the repository proxy
        async_methods = fixed_repo.get_async_methods()

        # Register each method as a tool
        for method in async_methods:
            # Create a wrapper that preserves metadata and serializes the result
            @functools.wraps(method)
            async def wrapper(*args, method=method, **kwargs):
                result = await method(*args, **kwargs)
                # Handle different return types
                if isinstance(result, list):
                    return [
                        dataclasses.asdict(item)
                        for item in result
                        if hasattr(item, "__dict__")
                    ]
                if hasattr(result, "__dict__"):
                    return dataclasses.asdict(result)
                return result

            # Rename the method to remove _async suffix
            wrapper.__name__ = method.__name__.replace("_async", "")

            # Add the method as a tool
            fixed_repo_mcp.add_tool(wrapper)

        # Run the fixed repository server
        if args.transport == "stdio":
            fixed_repo_mcp.run(transport="stdio")
        else:
            fixed_repo_mcp.run(transport="sse", port=args.port)
    # Run the general server
    elif args.transport == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.run(transport="sse", port=args.port)


if __name__ == "__main__":
    main()
