from __future__ import annotations

import functools
from typing import TYPE_CHECKING, ParamSpec, TypeVar, overload

from githarbor.core.models import (
    Asset,
    Branch,
    Commit,
    Issue,
    Label,
    PullRequest,
    Release,
    Tag,
    User,
)
from githarbor.exceptions import ResourceNotFoundError


if TYPE_CHECKING:
    from collections.abc import Callable

    from giteapy.models import (
        Asset as GiteaAsset,
        Branch as GiteaBranch,
        Commit as GiteaCommit,
        Issue as GiteaIssue,
        Label as GiteaLabel,
        PullRequest as GiteaPullRequest,
        Release as GiteaRelease,
        Tag as GiteaTag,
        User as GiteaUser,
    )

T = TypeVar("T")
P = ParamSpec("P")


def handle_api_errors(error_msg: str) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator to handle Gitea API exceptions consistently.

    Args:
        error_msg: Base error message to use in exception
    """

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            from giteapy.rest import ApiException

            try:
                return func(*args, **kwargs)
            except ApiException as e:
                msg = f"{error_msg}: {e!s}"
                raise ResourceNotFoundError(msg) from e

        return wrapper

    return decorator


@overload
def create_user_model(gitea_user: None) -> None: ...


@overload
def create_user_model(gitea_user: GiteaUser) -> User: ...


def create_user_model(gitea_user: GiteaUser | None) -> User | None:
    """Create User model from Gitea user object.

    Args:
        gitea_user: Gitea user object to convert

    Returns:
        Converted User model or None if input is None
    """
    if not gitea_user:
        return None

    return User(
        username=gitea_user.login,
        name=gitea_user.full_name,
        email=gitea_user.email,
        avatar_url=gitea_user.avatar_url,
        created_at=gitea_user.created,
        bio=gitea_user.description,
        location=gitea_user.location,
        is_admin=gitea_user.is_admin,
        last_activity_on=gitea_user.last_login,
        blog=gitea_user.website,
        url=gitea_user.html_url,
        followers=getattr(gitea_user, "followers_count", None),
        following=getattr(gitea_user, "following_count", None),
        public_repos=getattr(gitea_user, "public_repos", None),
        linkedin=getattr(gitea_user, "linkedin", None),
        skype=getattr(gitea_user, "skype", None),
    )


def create_label_model(gitea_label: GiteaLabel) -> Label:
    """Create Label model from Gitea label object."""
    return Label(
        name=gitea_label.name,
        color=gitea_label.color,
        description=gitea_label.description or "",
        url=gitea_label.url,
    )


def create_pull_request_model(pr: GiteaPullRequest) -> PullRequest:
    """Create PullRequest model from Gitea PR object."""
    return PullRequest(
        number=pr.number,
        title=pr.title,
        description=pr.body or "",
        state=pr.state,
        source_branch=pr.head.ref,
        target_branch=pr.base.ref,
        created_at=pr.created_at,
        updated_at=pr.updated_at,
        merged_at=pr.merged_at,
        closed_at=pr.closed_at,
        author=create_user_model(pr.user),
        labels=[create_label_model(label) for label in (pr.labels or [])],
        url=pr.html_url,
    )


def create_issue_model(issue: GiteaIssue) -> Issue:
    """Create Issue model from Gitea issue object."""
    return Issue(
        number=issue.number,
        title=issue.title,
        description=issue.body or "",
        state=issue.state,
        created_at=issue.created_at,
        updated_at=issue.updated_at,
        closed_at=issue.closed_at,
        closed=issue.state == "closed",
        author=create_user_model(issue.user),
        assignee=create_user_model(issue.assignee),
        labels=[create_label_model(label) for label in (issue.labels or [])],
        url=issue.html_url,
    )


def create_commit_model(commit: GiteaCommit) -> Commit:
    """Create Commit model from Gitea commit object."""
    return Commit(
        sha=commit.sha,
        message=commit.commit.message,
        created_at=commit.commit.author._date,
        author=User(
            username=commit.commit.author.name,
            email=commit.commit.author.email,
            name=commit.commit.author.name,
        ),
        url=commit.html_url,
    )


def create_release_model(release: GiteaRelease) -> Release:
    """Create Release model from Gitea release object."""
    return Release(
        tag_name=release.tag_name,
        name=release.name,
        description=release.body or "",
        created_at=release.created_at,
        published_at=release.published_at,
        draft=release.draft,
        prerelease=release.prerelease,
        author=create_user_model(release.author),
        assets=[create_asset_model(asset) for asset in release.assets],
        url=release.url,
        target_commitish=release.target_commitish,
    )


def create_tag_model(gitea_tag: GiteaTag) -> Tag:
    """Create Tag model from Gitea tag object."""
    return Tag(
        name=gitea_tag.name,
        sha=gitea_tag.id,  # Gitea uses 'id' for the SHA
        message=gitea_tag.message,
        created_at=getattr(gitea_tag, "created_at", None),
        author=create_user_model(gitea_tag.tagger)
        if hasattr(gitea_tag, "tagger")
        else None,
        url=getattr(gitea_tag, "url", None),
        verified=bool(getattr(gitea_tag, "verification", {}).get("verified", False)),
    )


def create_branch_model(gitea_branch: GiteaBranch) -> Branch:
    """Create Branch model from Gitea branch object."""
    return Branch(
        name=gitea_branch.name,
        sha=gitea_branch.commit.id,
        protected=gitea_branch.protected,
        created_at=None,  # Gitea API doesn't provide this
        updated_at=None,  # Gitea API doesn't provide this
    )


def create_asset_model(gitea_asset: GiteaAsset) -> Asset:
    """Create Asset model from Gitea asset object."""
    return Asset(
        name=gitea_asset.name,
        url=gitea_asset.browser_download_url,
        size=gitea_asset.size,
        download_count=gitea_asset.download_count,
        # Gitea API doesn't provide these timestamps
        created_at=None,
        updated_at=None,
    )
