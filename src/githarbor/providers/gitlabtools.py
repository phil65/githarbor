"""GitLab helper functions and utilities."""

from __future__ import annotations

from datetime import datetime
import functools
import inspect
import re
import string
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

import gitlab
import gitlab.exceptions

from githarbor.core.models import (
    Commit,
    Issue,
    Label,
    PullRequest,
    Release,
    User,
    Workflow,
    WorkflowRun,
)
from githarbor.exceptions import ResourceNotFoundError


if TYPE_CHECKING:
    from collections.abc import Callable


T = TypeVar("T")
P = ParamSpec("P")


def handle_gitlab_errors(
    error_msg_template: str,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator to handle GitLab API exceptions consistently.

    Args:
        error_msg_template: Message template with format placeholders

    Example:
        @handle_gitlab_errors("Could not fetch branch {branch_name}")
        def get_branch(self, branch_name: str) -> Branch:
            ...
    """
    # Extract field names from the template string
    parser = string.Formatter()
    param_names = {
        field_name
        for _, field_name, _, _ in parser.parse(error_msg_template)
        if field_name and field_name != "error"
    }

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            # Extract parameter values from args/kwargs based on function signature
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            params = {
                name: bound_args.arguments[name]
                for name in param_names
                if name in bound_args.arguments
            }

            try:
                return func(*args, **kwargs)
            except gitlab.exceptions.GitlabError as e:
                msg = error_msg_template.format(**params, error=str(e))
                raise ResourceNotFoundError(msg) from e

        return wrapper

    return decorator


TIMESTAMP_FORMATS: list[str] = [
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%S.%f%z",  # Format with explicit timezone
]


def parse_timestamp(timestamp: str) -> datetime:
    """Parse GitLab timestamp string to datetime.

    Args:
        timestamp: Timestamp string from GitLab API
    """
    # Convert 'Z' to +00:00 for consistent parsing
    timestamp = re.sub(r"Z$", "+00:00", timestamp)

    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(timestamp, fmt)
        except ValueError:
            continue
    msg = f"Unable to parse timestamp: {timestamp}"
    raise ValueError(msg)


def create_user_model(gl_user: Any | None) -> User | None:
    """Create User model from GitLab user object."""
    if not gl_user:
        return None
    return User(
        username=gl_user.username,
        name=gl_user.name,
        email=gl_user.email,
        avatar_url=gl_user.avatar_url,
        created_at=parse_timestamp(gl_user.created_at)
        if hasattr(gl_user, "created_at")
        else None,
        state=getattr(gl_user, "state", None),
        locked=getattr(gl_user, "locked", None),
        url=gl_user.web_url,
    )


def create_label_model(gl_label: Any) -> Label:
    """Create Label model from GitLab label object."""
    return Label(
        name=gl_label.name,
        color=getattr(gl_label, "color", ""),
        description=getattr(gl_label, "description", ""),
        url=getattr(gl_label, "url", None),
    )


def create_commit_model(commit: Any) -> Commit:
    """Create Commit model from GitLab commit object."""
    return Commit(
        sha=commit.id,
        message=commit.message,
        created_at=parse_timestamp(commit.created_at),
        author=User(
            username=commit.author_name,
            email=commit.author_email,
            name=commit.author_name,
        ),
        url=commit.web_url,
        stats={
            "additions": getattr(commit.stats, "additions", 0),
            "deletions": getattr(commit.stats, "deletions", 0),
            "total": getattr(commit.stats, "total", 0),
        },
    )


def create_issue_model(issue: Any) -> Issue:
    """Create Issue model from GitLab issue object."""
    return Issue(
        number=issue.iid,
        title=issue.title,
        description=issue.description or "",
        state=issue.state,
        created_at=parse_timestamp(issue.created_at),
        updated_at=parse_timestamp(issue.updated_at) if issue.updated_at else None,
        closed_at=parse_timestamp(issue.closed_at) if issue.closed_at else None,
        closed=issue.state == "closed",
        author=User(
            username=issue.author["username"],
            name=issue.author["name"],
            avatar_url=issue.author["avatar_url"],
        )
        if issue.author
        else None,
        assignee=User(
            username=issue.assignee["username"],
            name=issue.assignee["name"],
            avatar_url=issue.assignee["avatar_url"],
        )
        if issue.assignee
        else None,
        labels=[Label(name=lbl) for lbl in issue.labels],
    )


def create_pull_request_model(mr: Any) -> PullRequest:
    """Create PullRequest model from GitLab merge request object."""
    return PullRequest(
        number=mr.iid,
        title=mr.title,
        description=mr.description or "",
        state=mr.state,
        source_branch=mr.source_branch,
        target_branch=mr.target_branch,
        created_at=parse_timestamp(mr.created_at),
        updated_at=parse_timestamp(mr.updated_at) if hasattr(mr, "updated_at") else None,
        merged_at=parse_timestamp(mr.merged_at) if hasattr(mr, "merged_at") else None,
        closed_at=parse_timestamp(mr.closed_at) if hasattr(mr, "closed_at") else None,
        author=create_user_model(getattr(mr, "author", None)),
        assignees=[
            create_user_model(a) for a in getattr(mr, "assignees", []) if a is not None
        ],
        labels=[Label(name=label) for label in getattr(mr, "labels", [])],
        review_comments_count=getattr(mr, "user_notes_count", 0),
        commits_count=getattr(mr, "commits_count", 0),
        additions=getattr(mr, "additions", 0),
        deletions=getattr(mr, "deletions", 0),
        changed_files=getattr(mr, "changes_count", 0),
        mergeable=getattr(mr, "mergeable", None),
        url=mr.web_url,
    )


def create_workflow_model(pipeline: Any) -> Workflow:
    """Create Workflow model from GitLab pipeline object."""
    return Workflow(
        id=str(pipeline.id),
        name=pipeline.ref,
        path=getattr(pipeline, "path", ""),
        state=pipeline.status,
        created_at=parse_timestamp(pipeline.created_at),
        updated_at=None,
        badge_url=getattr(pipeline, "badge_url", None),
    )


def create_workflow_run_model(job: Any) -> WorkflowRun:
    """Create WorkflowRun model from GitLab job object."""
    return WorkflowRun(
        id=str(job.id),
        name=job.name,
        workflow_id=str(job.pipeline["id"]),
        status=job.status,
        conclusion=job.status,
        branch=getattr(job, "ref", None),
        commit_sha=getattr(job.commit, "id", None),
        url=job.web_url,
        created_at=parse_timestamp(job.created_at),
        started_at=parse_timestamp(job.started_at)
        if hasattr(job, "started_at")
        else None,
        completed_at=parse_timestamp(job.finished_at)
        if hasattr(job, "finished_at")
        else None,
        logs_url=getattr(job, "artifacts_file", {}).get("filename"),
    )


def create_release_model(release: Any) -> Release:
    """Create Release model from GitLab release object."""
    return Release(
        tag_name=release.tag_name,
        name=release.name,
        description=release.description or "",
        created_at=parse_timestamp(release.created_at),
        published_at=parse_timestamp(release.released_at)
        if hasattr(release, "released_at")
        else None,
        draft=False,  # GitLab doesn't have draft releases
        prerelease=release.tag_name.startswith(("alpha", "beta", "rc")),
        author=create_user_model(getattr(release, "author", None)),
        assets=[
            {
                "name": asset["name"],
                "url": asset["url"],
                "size": asset.get("size", 0),
            }
            for asset in getattr(release, "assets", {}).get("links", [])
        ],
        url=getattr(release, "_links", {}).get("self"),
        target_commitish=getattr(release, "commit", {}).get("id"),
    )
