"""Tools for converting aiogithubapi models to githarbor models."""

from __future__ import annotations

from datetime import datetime
import logging
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, overload

from aiogithubapi.exceptions import GitHubException
import aiohttp
import upath

from githarbor.core.models import (
    Asset,
    Issue,
    Label,
    PullRequest,
    Release,
    Tag,
    User,
)
from githarbor.exceptions import ResourceNotFoundError


if TYPE_CHECKING:
    import os

    from aiogithubapi import GitHubLabelModel
    from aiogithubapi.models.issue import GitHubIssueModel
    from aiogithubapi.models.pull_request import GitHubPullRequestModel
    from aiogithubapi.models.release import GitHubReleaseModel
    from aiogithubapi.models.tag import GitHubTagModel
    from aiogithubapi.models.user import GitHubBaseUserModel, GitHubUserModel


logger = logging.getLogger(__name__)
T = TypeVar("T")
P = ParamSpec("P")


def parse_datetime(date_str: str | None) -> datetime | None:
    """Parse GitHub date string to datetime."""
    if not date_str:
        return None
    return datetime.fromisoformat(date_str.replace("Z", "+00:00"))


@overload
def create_user_model(user: None) -> None: ...


@overload
def create_user_model(user: GitHubBaseUserModel | GitHubUserModel) -> User: ...


def create_user_model(user: GitHubBaseUserModel | GitHubUserModel | None) -> User | None:
    """Convert aiogithubapi user model to GitHarbor user model."""
    if not user:
        return None

    data = {
        "username": user.login or "",
        "name": getattr(user, "name", None),
        "email": getattr(user, "email", None),
        "avatar_url": user.avatar_url,
        "url": user.html_url,
        "created_at": parse_datetime(getattr(user, "created_at", None)),
        "bio": getattr(user, "bio", None),
        "location": getattr(user, "location", None),
        "company": getattr(user, "company", None),
        "blog": getattr(user, "blog", None),
        "twitter_username": getattr(user, "twitter_username", None),
        "hireable": getattr(user, "hireable", None),
        "gravatar_id": user.gravatar_id,
        "followers": getattr(user, "followers", None),
        "following": getattr(user, "following", None),
        "public_repos": getattr(user, "public_repos", None),
    }
    return User(**data)  # pyright: ignore


def create_label_model(label: GitHubLabelModel) -> Label:
    """Convert aiogithubapi label model to GitHarbor label model."""
    return Label(
        name=label.name or "",
        color=label.color or "",
        description=label.description or "",
        url=label.url,
    )


def create_issue_model(issue: GitHubIssueModel) -> Issue:
    """Convert aiogithubapi issue model to GitHarbor issue model."""
    return Issue(
        number=int(issue.number or 0),
        title=str(issue.title or ""),
        description=str(issue.body or ""),
        state=str(issue.state or ""),
        created_at=parse_datetime(issue.created_at),
        updated_at=parse_datetime(issue.updated_at),
        closed_at=parse_datetime(issue.closed_at),
        closed=issue.state == "closed",
        author=create_user_model(issue.user),
        assignee=create_user_model(issue.assignee),
        labels=[create_label_model(lbl) for lbl in (issue.labels or [])],
        comments_count=int(issue.comments or 0),
        url=str(issue.html_url or ""),
        milestone=issue.milestone.title if issue.milestone else None,
    )


def create_pull_request_model(pr: GitHubPullRequestModel) -> PullRequest:
    """Convert aiogithubapi pull request model to GitHarbor pull request model."""
    return PullRequest(
        number=int(pr.number or 0),
        title=str(pr.title or ""),
        description=str(pr.body or ""),
        state=str(pr.state or ""),
        source_branch=str(pr.head.get("ref", "") if pr.head else ""),
        target_branch=str(pr.base.get("ref", "") if pr.base else ""),
        created_at=parse_datetime(pr.created_at),
        updated_at=parse_datetime(pr.updated_at),
        merged_at=parse_datetime(pr.merged_at),
        closed_at=parse_datetime(pr.closed_at),
        author=create_user_model(pr.user),
        assignees=[create_user_model(a) for a in (pr.assignees or [])],
        labels=[create_label_model(lbl) for lbl in (pr.labels or [])],
        merged_by=None,  # Not available
        review_comments_count=0,  # Not available
        commits_count=0,  # Not available
        additions=0,  # Not available
        deletions=0,  # Not available
        changed_files=0,  # Not available
        url=str(pr.html_url or ""),
    )


def create_release_model(release: GitHubReleaseModel) -> Release:
    """Convert aiogithubapi release model to GitHarbor release model."""
    return Release(
        tag_name=str(release.tag_name or ""),
        name=str(release.name or release.tag_name or ""),
        description=str(release.body or ""),
        created_at=parse_datetime(release.created_at),
        published_at=parse_datetime(release.published_at),
        draft=bool(release.draft),
        prerelease=bool(release.prerelease),
        author=create_user_model(release.author),
        assets=[create_asset_model(asset) for asset in (release.assets or [])],
        url=str(release.html_url or ""),
        target_commitish=str(release.target_commitish or ""),
    )


def create_asset_model(asset: Any) -> Asset:
    """Create Asset model from AioGitHubAPI asset object."""
    return Asset(
        name=str(asset["name"]),
        url=str(asset["browser_download_url"]),
        size=int(asset["size"]),
        download_count=int(asset["download_count"]),
        created_at=parse_datetime(asset["created_at"])
        if asset.get("created_at")
        else None,
        updated_at=parse_datetime(asset["updated_at"])
        if asset.get("updated_at")
        else None,
    )


def create_tag_model(tag: GitHubTagModel) -> Tag:
    """Convert aiogithubapi tag model to GitHarbor tag model."""
    return Tag(
        name=str(tag.name or ""),
        sha=str(tag.commit.sha if tag.commit else ""),
        message=None,  # Not available
        created_at=None,  # Not available
        author=None,  # Not available
        url=None,  # Not available
    )


async def download_from_github(
    repository: str,
    path: str | os.PathLike[str],
    destination: str | os.PathLike[str],
    token: str | None = None,
    recursive: bool = False,
) -> None:
    """Download files from GitHub repository."""
    owner, repo = repository.split("/")

    dest = upath.UPath(destination)
    dest.mkdir(exist_ok=True, parents=True)

    url = f"https://raw.githubusercontent.com/{owner}/{repo}/main/{path}"
    headers = {"Authorization": f"token {token}"} if token else {}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 404:  # noqa: PLR2004
                    msg = f"Path not found: {path}"
                    raise ResourceNotFoundError(msg)
                content = await response.read()
                file_dest = dest / upath.UPath(path).name
                file_dest.write_bytes(content)
        except aiohttp.ClientError as e:
            msg = f"Failed to download {path}: {e}"
            raise GitHubException(msg) from e
