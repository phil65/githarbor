"""Utilities for PyGitea provider."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from functools import wraps
import logging
from typing import TYPE_CHECKING, Any, TypeVar


if TYPE_CHECKING:
    from collections.abc import Iterator

    from githarbor.core.models import (
        Branch,
        Commit,
        Issue,
        PullRequest,
        Release,
        Tag,
        User,
    )

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def handle_api_errors(error_msg: str) -> Callable[[F], F]:
    """Decorator to handle PyGitea API errors."""

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.exception("%s", error_msg)
                # Import here to avoid circular imports
                from githarbor.exceptions import (
                    AuthenticationError,
                    GitHarborError,
                    ResourceNotFoundError,
                )

                error_str = str(e).lower()
                if "not found" in error_str or "404" in error_str:
                    msg = f"{error_msg}: {e}"
                    raise ResourceNotFoundError(msg) from e
                if "unauthorized" in error_str or "401" in error_str or "403" in error_str:
                    msg = f"{error_msg}: {e}"
                    raise AuthenticationError(msg) from e
                msg = f"{error_msg}: {e}"
                raise GitHarborError(msg) from e

        return wrapper  # type: ignore[return-value]

    return decorator


def create_user_model(user: Any) -> User:
    """Convert PyGitea User to GitHarbor User model."""
    from githarbor.core.models import User

    return User(
        username=user.username,
        name=user.full_name or user.username,
        email=user.email,
        avatar_url=user.avatar_url,
    )


def create_branch_model(branch: Any) -> Branch:
    """Convert PyGitea Branch to GitHarbor Branch model."""
    from githarbor.core.models import Branch

    sha = branch.commit["id"] if branch.commit else ""
    return Branch(
        name=branch.name,
        sha=sha,
        protected=branch.protected if hasattr(branch, "protected") else False,
    )


def create_commit_model(commit: Any) -> Commit:
    """Convert PyGitea Commit to GitHarbor Commit model."""
    from githarbor.core.models import Commit, User

    # Handle commit structure based on py-gitea documentation
    commit_data = commit.inner_commit if hasattr(commit, "inner_commit") else {}
    author_info = commit_data.get("author", {})
    committer_info = commit_data.get("committer", {})

    author = None
    if author_info.get("name"):
        author = User(
            username=author_info.get("name", ""),
            name=author_info.get("name"),
            email=author_info.get("email"),
        )

    committer = None
    if committer_info.get("name"):
        committer = User(
            username=committer_info.get("name", ""),
            name=committer_info.get("name"),
            email=committer_info.get("email"),
        )

    return Commit(
        sha=commit.sha,
        message=commit_data.get("message", ""),
        author=author,
        created_at=_parse_datetime(author_info.get("date")),
        committer=committer,
        url=commit.html_url if hasattr(commit, "html_url") else None,
    )


def create_issue_model(issue: Any) -> Issue:
    """Convert PyGitea Issue to GitHarbor Issue model."""
    from githarbor.core.models import Issue

    return Issue(
        number=issue.number if hasattr(issue, "number") else issue.id,
        title=issue.title,
        description=issue.body if hasattr(issue, "body") else "",
        state=issue.state,
        author=create_user_model(issue.user) if issue.user else None,
        assignee=create_user_model(issue.assignee) if issue.assignee else None,
        labels=[],
        created_at=_parse_datetime(issue.created_at),
        updated_at=_parse_datetime(issue.updated_at),
        closed_at=_parse_datetime(issue.closed_at) if hasattr(issue, "closed_at") else None,
        closed=issue.state == "closed",
    )


def create_pull_request_model(pr: Any) -> PullRequest:
    """Convert PyGitea Pull Request to GitHarbor PullRequest model."""
    from githarbor.core.models import PullRequest

    return PullRequest(
        number=pr.number,
        title=pr.title,
        source_branch=pr.head.ref if pr.head else "",
        target_branch=pr.base.ref if pr.base else "",
        description=pr.body if hasattr(pr, "body") else "",
        state=pr.state,
        author=create_user_model(pr.user) if pr.user else None,
        created_at=_parse_datetime(pr.created_at),
        updated_at=_parse_datetime(pr.updated_at),
        merged_at=_parse_datetime(pr.merged_at) if hasattr(pr, "merged_at") else None,
        merged_by=create_user_model(pr.merged_by)
        if hasattr(pr, "merged_by") and pr.merged_by
        else None,
        mergeable=pr.mergeable if hasattr(pr, "mergeable") else None,
        url=pr.html_url if hasattr(pr, "html_url") else None,
    )


def create_release_model(release: Any) -> Release:
    """Convert PyGitea Release to GitHarbor Release model."""
    from githarbor.core.models import Release

    return Release(
        tag_name=release.tag_name,
        name=release.name,
        description=release.body if hasattr(release, "body") else "",
        draft=release.draft,
        prerelease=release.prerelease,
        created_at=_parse_datetime(release.created_at),
        published_at=_parse_datetime(release.published_at),
        author=create_user_model(release.author) if release.author else None,
        assets=[],
        url=release.html_url if hasattr(release, "html_url") else None,
    )


def create_tag_model(tag: Any) -> Tag:
    """Convert PyGitea Tag to GitHarbor Tag model."""
    from githarbor.core.models import Tag

    sha = tag.commit["id"] if tag.commit else ""
    return Tag(
        name=tag.name,
        sha=sha,
        message=tag.message if hasattr(tag, "message") else None,
    )


def _parse_datetime(dt_str: str | None) -> datetime | None:
    """Parse datetime string to datetime object."""
    if not dt_str:
        return None

    try:
        # Handle different datetime formats that Gitea might return
        if dt_str.endswith("Z"):
            # ISO format with Z suffix
            return datetime.fromisoformat(dt_str.rstrip("Z")).replace(tzinfo=None)
        if "+" in dt_str or dt_str.count("-") > 2:  # noqa: PLR2004
            # ISO format with timezone
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        # Fallback to basic ISO format
        return datetime.fromisoformat(dt_str)
    except (ValueError, AttributeError):
        logger.warning("Failed to parse datetime: %s", dt_str)
        return None


def iter_paginated(func: Callable[..., list[Any]], *args: Any, **kwargs: Any) -> Iterator[Any]:
    """Iterate through paginated API results."""
    page = 1
    while True:
        try:
            results = func(*args, page=page, **kwargs)
            if not results:
                break
            yield from results
            page += 1
        except Exception:  # noqa: BLE001
            # If pagination fails, just return what we got
            break
