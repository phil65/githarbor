"""GitHub repository implementation using aiogithubapi."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, ClassVar

from aiogithubapi import GitHubAPI, GitHubRequestKwarg
from aiogithubapi.exceptions import (
    GitHubAuthenticationException,
    GitHubException,
    GitHubNotFoundException,
)

from githarbor.core.base import BaseRepository, IssueState, PullRequestState
from githarbor.exceptions import AuthenticationError, ResourceNotFoundError
from githarbor.providers.aiogithubapi_provider import utils as aiogithubapitools


if TYPE_CHECKING:
    from githarbor.core.models import (
        Issue,
        PullRequest,
        Release,
        Tag,
        User,
    )


class AioGitHubRepository(BaseRepository):
    """GitHub repository implementation using aiogithubapi."""

    url_patterns: ClassVar[list[str]] = ["github.com"]

    def __init__(
        self,
        owner: str,
        name: str,
        token: str | None = None,
    ) -> None:
        """Initialize GitHub API client.

        Args:
            owner: Repository owner
            name: Repository name
            token: GitHub access token
        """
        try:
            t = token or os.getenv("GITHUB_TOKEN")
            assert t, "GitHub token is required"
            self._gh = GitHubAPI(token=t)
            self._token = t
            self._owner = owner
            self._name = name
            self._repo = None

        except GitHubAuthenticationException as e:
            msg = f"GitHub authentication failed: {e}"
            raise AuthenticationError(msg) from e

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> AioGitHubRepository:
        """Create instance from URL."""
        # Remove .git suffix and trailing slashes
        url = url.removesuffix(".git").rstrip("/")
        path = url.split("github.com/", 1)[1]
        owner, name = path.split("/")
        return cls(owner=owner, name=name, token=kwargs.get("token"))

    async def _ensure_repo(self) -> None:
        """Ensure repository info is loaded."""
        if self._repo is None:
            try:
                response = await self._gh.repos.get(f"{self._owner}/{self._name}")
                self._repo = response.data
            except GitHubNotFoundException as e:
                msg = f"Repository {self._owner}/{self._name} not found"
                raise ResourceNotFoundError(msg) from e

    @property
    def default_branch(self) -> str:
        """Return default branch name."""
        return (
            self._repo.default_branch
            if self._repo and self._repo.default_branch
            else "main"
        )

    async def get_repo_user_async(self) -> User:
        """Get repository owner information."""
        try:
            response = await self._gh.users.get(self._owner)
            assert response.data, f"User {self._owner} not found"
            return aiogithubapitools.create_user_model(response.data)
        except GitHubNotFoundException as e:
            msg = f"User {self._owner} not found"
            raise ResourceNotFoundError(msg) from e

    async def get_issue_async(self, issue_id: int) -> Issue:
        """Get issue by number."""
        try:
            repo = f"{self._owner}/{self._name}"
            response = await self._gh.repos.issues.get(repo, issue_id)
            assert response.data, f"Issue #{issue_id} not found"
            return aiogithubapitools.create_issue_model(response.data)
        except GitHubNotFoundException as e:
            msg = f"Issue #{issue_id} not found"
            raise ResourceNotFoundError(msg) from e

    async def list_issues_async(self, state: IssueState = "open") -> list[Issue]:
        """List repository issues."""
        repo = f"{self._owner}/{self._name}"
        params = {GitHubRequestKwarg.QUERY: {"state": state}}
        response = await self._gh.repos.issues.list(repo, params=params)
        return [aiogithubapitools.create_issue_model(i) for i in response.data or []]

    async def create_issue_async(
        self,
        title: str,
        body: str,
        labels: list[str] | None = None,
        assignees: list[str] | None = None,
    ) -> Issue:
        """Create a new issue."""
        from aiogithubapi import GitHubRequestKwarg
        from aiogithubapi.github import HttpMethod

        data = {
            "title": title,
            "body": body,
            "labels": labels or [],
            "assignees": assignees or [],
        }
        kwargs = {GitHubRequestKwarg.METHOD: HttpMethod.POST}
        endpoint = f"/repos/{self._owner}/{self._name}/issues"
        response = await self._gh.generic(endpoint=endpoint, data=data, **kwargs)  # type: ignore
        assert response.data
        return aiogithubapitools.create_issue_model(response.data)

    async def get_pull_request_async(self, number: int) -> PullRequest:
        """Get pull request by number."""
        try:
            repo = f"{self._owner}/{self._name}"
            params = {GitHubRequestKwarg.QUERY: {"number": number}}
            response = await self._gh.repos.pulls.list(repo, params=params)
            if not response.data:
                msg = f"Pull request #{number} not found"
                raise ResourceNotFoundError(msg)
            return aiogithubapitools.create_pull_request_model(response.data[0])
        except GitHubException as e:
            msg = f"Failed to get pull request #{number}: {e}"
            raise ResourceNotFoundError(msg) from e

    async def list_pull_requests_async(
        self, state: PullRequestState = "open"
    ) -> list[PullRequest]:
        """List pull requests."""
        repo = f"{self._owner}/{self._name}"
        params = {GitHubRequestKwarg.QUERY: {"state": state}}
        response = await self._gh.repos.pulls.list(repo, params=params)
        return [
            aiogithubapitools.create_pull_request_model(pr) for pr in response.data or []
        ]

    async def get_latest_release_async(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:
        """Get latest release."""
        try:
            response = await self._gh.repos.releases.latest(f"{self._owner}/{self._name}")
            assert response.data, f"Release for {self._owner}/{self._name} not found"
            release = aiogithubapitools.create_release_model(response.data)
            if (not include_drafts and release.draft) or (
                not include_prereleases and release.prerelease
            ):
                msg = "Latest release is draft/prerelease"
                raise ResourceNotFoundError(msg)
        except GitHubNotFoundException as e:
            msg = "No releases found"
            raise ResourceNotFoundError(msg) from e
        else:
            return release

    async def list_releases_async(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        """List releases."""
        response = await self._gh.repos.releases.list(f"{self._owner}/{self._name}")
        releases = []
        for release in response.data or []:
            if not include_drafts and release.draft:
                continue
            if not include_prereleases and release.prerelease:
                continue
            releases.append(aiogithubapitools.create_release_model(release))
            if limit and len(releases) >= limit:
                break
        return releases

    async def list_tags_async(self) -> list[Tag]:
        """List repository tags."""
        await self._ensure_repo()
        response = await self._gh.repos.list_tags(f"{self._owner}/{self._name}")
        return [aiogithubapitools.create_tag_model(tag) for tag in response.data or []]

    async def download_async(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ) -> None:
        """Download repository content."""
        await aiogithubapitools.download_from_github(
            repository=f"{self._owner}/{self._name}",
            path=path,
            destination=destination,
            token=self._token,
            recursive=recursive,
        )
