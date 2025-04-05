"""GitLab helper functions and utilities."""

from __future__ import annotations

from datetime import datetime
import functools
import inspect
import re
import string
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, overload

from githarbor.core.models import (
    Asset,
    Branch,
    Comment,
    Commit,
    Issue,
    Label,
    PullRequest,
    Release,
    Tag,
    User,
    Workflow,
    WorkflowRun,
)
from githarbor.exceptions import ResourceNotFoundError


if TYPE_CHECKING:
    from collections.abc import Callable

    from gitlab.base import RESTObject
    from gitlab.v4.objects import ProjectBranch


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
            import gitlab.exceptions

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


def create_branch_model(branch: ProjectBranch) -> Branch:
    """Create Branch model from GitLab ProjectBranch object."""
    return Branch(
        name=branch.name,
        sha=branch.commit["id"],
        protected=branch.protected,
        default=False,  # GitLab doesn't provide this in branch object
        created_at=None,  # GitLab doesn't provide this
        updated_at=None,  # GitLab doesn't provide this
        last_commit_date=parse_timestamp(branch.commit["created_at"]),
        last_commit_message=branch.commit["message"],
        last_commit_author=User(
            username=branch.commit["author_name"],
            name=branch.commit["author_name"],
            email=branch.commit["author_email"],
        ),
        protection_rules={
            "can_push": branch.developers_can_push,
            "can_merge": branch.developers_can_merge,
        }
        if branch.protected
        else None,
    )


@overload
def create_user_model(gl_user: None) -> None: ...


@overload
def create_user_model(gl_user: RESTObject) -> User: ...


def create_user_model(gl_user: RESTObject | None) -> User | None:
    """Create User model from GitLab user object.

    Args:
        gl_user: GitLab user object or None

    Returns:
        User model instance or None if input is None
    """
    if not gl_user:
        return None

    return User(
        username=gl_user.username,
        name=gl_user.name,
        email=getattr(gl_user, "email", None),
        avatar_url=gl_user.avatar_url,
        created_at=parse_timestamp(gl_user.created_at)
        if hasattr(gl_user, "created_at")
        else None,
        bio=getattr(gl_user, "bio", None),
        location=getattr(gl_user, "location", None),
        company=getattr(gl_user, "organization", None),
        url=gl_user.web_url,
        followers=getattr(gl_user, "followers_count", 0),
        following=getattr(gl_user, "following_count", 0),
        public_repos=getattr(gl_user, "projects_limit", 0),
        blog=getattr(gl_user, "website_url", None),
        twitter_username=getattr(gl_user, "twitter", None),
        hireable=None,  # GitLab doesn't have this field
        gravatar_id=None,  # GitLab doesn't use gravatar_id
        state=getattr(gl_user, "state", None),
        locked=getattr(gl_user, "locked", None),
        is_admin=getattr(gl_user, "is_admin", False),
        last_activity_on=parse_timestamp(gl_user.last_activity_on)
        if hasattr(gl_user, "last_activity_on")
        else None,
        linkedin=getattr(gl_user, "linkedin", None),
        skype=getattr(gl_user, "skype", None),
    )


def create_label_model(gl_label: RESTObject) -> Label:
    """Create Label model from GitLab label object."""
    return Label(
        name=gl_label.name,
        color=getattr(gl_label, "color", ""),
        description=getattr(gl_label, "description", ""),
        url=getattr(gl_label, "url", None),
    )


def create_commit_model(commit: RESTObject) -> Commit:
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
    asset_list = getattr(release, "assets", {}).get("links", [])
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
        assets=[create_asset_model(a) for a in asset_list],
        url=getattr(release, "_links", {}).get("self"),
        target_commitish=getattr(release, "commit", {}).get("id"),
    )


def create_comment_model(gl_comment: Any) -> Comment:
    """Create Comment model from GitLab comment/note object."""
    return Comment(
        id=str(gl_comment.id),
        body=gl_comment.body,
        author=create_user_model(gl_comment.author),
        created_at=parse_timestamp(gl_comment.created_at),
        updated_at=parse_timestamp(gl_comment.updated_at)
        if gl_comment.updated_at
        else None,
        url=gl_comment.url if hasattr(gl_comment, "url") else None,
    )


def create_tag_model(tag: Any) -> Tag:
    """Create Tag model from GitLab tag object."""
    return Tag(
        name=tag.name,
        sha=tag.commit["id"],
        message=tag.message or "",
        created_at=parse_timestamp(tag.commit["created_at"]),
        author=create_user_model(tag.commit["author"]),
        url=f"{tag._project.web_url}/-/tags/{tag.name}",  # type: ignore
        verified=bool(getattr(tag, "verified", False)),
    )


def create_asset_model(gl_asset: Any) -> Asset:
    """Create Asset model from GitLab asset object."""
    return Asset(
        name=gl_asset["name"],
        url=gl_asset["url"],
        size=gl_asset.get("size", 0),
        download_count=gl_asset.get("download_count", 0),
    )
