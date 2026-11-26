"""GitLab helper functions and utilities."""

import annotationlib
from datetime import datetime
import functools
import inspect
import string
from typing import TYPE_CHECKING, Any, overload

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


def handle_gitlab_errors[T, **P](
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
            sig = inspect.signature(func, annotation_format=annotationlib.Format.FORWARDREF)
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


def parse_timestamp(timestamp: str) -> datetime:
    """Parse GitLab timestamp string to datetime.

    Args:
        timestamp: Timestamp string from GitLab API
    """
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00"))


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
def create_user_model(gl_user: RESTObject | dict[str, Any]) -> User: ...


def create_user_model(gl_user: RESTObject | dict[str, Any] | None) -> User | None:
    """Create User model from GitLab user object or dict.

    Args:
        gl_user: GitLab user object, dict, or None

    Returns:
        User model instance or None if input is None
    """
    if not gl_user:
        return None

    def _get(key: str, default: Any = None) -> Any:
        if isinstance(gl_user, dict):
            return gl_user.get(key, default)
        return getattr(gl_user, key, default)

    created_at_raw = _get("created_at")
    last_activity_raw = _get("last_activity_on")
    return User(
        username=_get("username", ""),
        name=_get("name"),
        email=_get("email"),
        avatar_url=_get("avatar_url"),
        created_at=parse_timestamp(created_at_raw) if created_at_raw else None,
        bio=_get("bio"),
        location=_get("location"),
        company=_get("organization"),
        url=_get("web_url"),
        followers=_get("followers_count", 0),
        following=_get("following_count", 0),
        public_repos=_get("projects_limit", 0),
        blog=_get("website_url"),
        twitter_username=_get("twitter"),
        hireable=None,  # GitLab doesn't have this field
        gravatar_id=None,  # GitLab doesn't use gravatar_id
        state=_get("state"),
        locked=_get("locked"),
        is_admin=_get("is_admin", False),
        last_activity_on=parse_timestamp(last_activity_raw) if last_activity_raw else None,
        linkedin=_get("linkedin"),
        skype=_get("skype"),
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
    raw_stats = getattr(commit, "stats", None)
    if isinstance(raw_stats, dict):
        stats = {
            "additions": raw_stats.get("additions", 0),
            "deletions": raw_stats.get("deletions", 0),
            "total": raw_stats.get("total", 0),
        }
    else:
        stats = {
            "additions": getattr(raw_stats, "additions", 0),
            "deletions": getattr(raw_stats, "deletions", 0),
            "total": getattr(raw_stats, "total", 0),
        }
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
        stats=stats,
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
    updated_at = getattr(mr, "updated_at", None)
    merged_at = getattr(mr, "merged_at", None)
    closed_at = getattr(mr, "closed_at", None)
    return PullRequest(
        number=mr.iid,
        title=mr.title,
        description=mr.description or "",
        state=mr.state,
        source_branch=mr.source_branch,
        target_branch=mr.target_branch,
        created_at=parse_timestamp(mr.created_at),
        updated_at=parse_timestamp(updated_at) if updated_at else None,
        merged_at=parse_timestamp(merged_at) if merged_at else None,
        closed_at=parse_timestamp(closed_at) if closed_at else None,
        author=create_user_model(getattr(mr, "author", None)),
        assignees=[create_user_model(a) for a in (getattr(mr, "assignees", None) or []) if a],
        labels=[Label(name=label) for label in (getattr(mr, "labels", None) or [])],
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
    commit = getattr(job, "commit", None)
    commit_sha = commit.get("id") if isinstance(commit, dict) else getattr(commit, "id", None)
    started_at = getattr(job, "started_at", None)
    finished_at = getattr(job, "finished_at", None)
    artifacts_file = getattr(job, "artifacts_file", None) or {}
    return WorkflowRun(
        id=str(job.id),
        name=job.name,
        workflow_id=str(job.pipeline["id"]),
        status=job.status,
        conclusion=job.status,
        branch=getattr(job, "ref", None),
        commit_sha=commit_sha,
        url=job.web_url,
        created_at=parse_timestamp(job.created_at),
        started_at=parse_timestamp(started_at) if started_at else None,
        completed_at=parse_timestamp(finished_at) if finished_at else None,
        logs_url=artifacts_file.get("filename"),
    )


def create_release_model(release: Any) -> Release:
    """Create Release model from GitLab release object."""
    asset_list = (getattr(release, "assets", None) or {}).get("links", [])
    released_at = getattr(release, "released_at", None)
    return Release(
        tag_name=release.tag_name,
        name=release.name,
        description=release.description or "",
        created_at=parse_timestamp(release.created_at),
        published_at=parse_timestamp(released_at) if released_at else None,
        draft=False,  # GitLab doesn't have draft releases
        prerelease=release.tag_name.startswith(("alpha", "beta", "rc")),
        author=create_user_model(getattr(release, "author", None)),
        assets=[create_asset_model(a) for a in asset_list],
        url=(getattr(release, "_links", None) or {}).get("self"),
        target_commitish=(getattr(release, "commit", None) or {}).get("id"),
    )


def create_comment_model(gl_comment: Any) -> Comment:
    """Create Comment model from GitLab comment/note object or dict."""
    if isinstance(gl_comment, dict):
        comment_id = str(gl_comment["id"])
        body = gl_comment["body"]
        author = create_user_model(gl_comment.get("author"))
        created_at = parse_timestamp(gl_comment["created_at"])
        updated_at_raw = gl_comment.get("updated_at")
        url = gl_comment.get("url")
    else:
        comment_id = str(gl_comment.id)
        body = gl_comment.body
        author = create_user_model(gl_comment.author)
        created_at = parse_timestamp(gl_comment.created_at)
        updated_at_raw = getattr(gl_comment, "updated_at", None)
        url = getattr(gl_comment, "url", None)

    return Comment(
        id=comment_id,
        body=body,
        author=author or User(username="unknown"),
        created_at=created_at,
        updated_at=parse_timestamp(updated_at_raw) if updated_at_raw else None,
        url=url,
    )


def create_tag_model(tag: Any) -> Tag:
    """Create Tag model from GitLab tag object."""
    commit = tag.commit
    author_name = commit.get("author_name")
    author = (
        User(
            username=author_name,
            name=author_name,
            email=commit.get("author_email"),
        )
        if author_name
        else None
    )
    project = getattr(tag, "_project", None)
    url = (
        f"{project.web_url}/-/tags/{tag.name}" if project and hasattr(project, "web_url") else None
    )
    return Tag(
        name=tag.name,
        sha=commit["id"],
        message=tag.message or "",
        created_at=parse_timestamp(commit["created_at"]) if commit.get("created_at") else None,
        author=author,
        url=url,
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
