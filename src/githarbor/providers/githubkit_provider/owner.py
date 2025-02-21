"""Module containing GitHubKit owner implementation."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any, ClassVar

from githubkit import GitHub
from githubkit.exception import GitHubException

from githarbor.core.base import BaseOwner, BaseRepository
from githarbor.exceptions import AuthenticationError, ResourceNotFoundError
from githarbor.providers.githubkit_provider import utils as githubkittools
from githarbor.repositories import create_repository


if TYPE_CHECKING:
    from githarbor.core.models import User

logger = logging.getLogger(__name__)


class GitHubKitOwner(BaseOwner):
    """Owner implementation using GitHubKit."""

    url_patterns: ClassVar[list[str]] = ["github.com"]
    is_async = True

    def __init__(self, username: str, token: str | None = None) -> None:
        """Initialize GitHub API client.

        Args:
            username: Username of the owner
            token: GitHub access token
        """
        try:
            t = token or os.getenv("GITHUB_TOKEN")
            if not t:
                msg = "GitHub token is required"
                raise ValueError(msg)
            self._token = t
            self._gh = GitHub(t)
            self._name = username
            self._user = self._gh.rest.users.get_authenticated().parsed_data

        except GitHubException as e:
            msg = f"GitHub authentication failed: {e!s}"
            raise AuthenticationError(msg) from e

    @property
    def name(self) -> str:
        """The name of the owner."""
        return self._name

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> GitHubKitOwner:
        """Create instance from URL.

        Args:
            url: URL like "https://github.com/octocat"
            **kwargs: Additional arguments including token
        """
        # Remove trailing slashes and handle both user & org URLs
        url = url.rstrip("/")
        username = url.split("/")[-1]
        return cls(username=username, token=kwargs.get("token"))

    async def list_repos_owned_by_user(
        self, headers: dict[str, str] | None = None
    ) -> list[dict[str, Any]]:
        """List repositories owned by user.

        Args:
            headers: Optional additional headers to pass

        Returns:
            List of repository data
        """
        import aiohttp

        base_headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"Bearer {self._token}",
        }
        if headers:
            base_headers.update(headers)

        url = f"https://api.github.com/users/{self._name}/repos"

        async with aiohttp.ClientSession() as session:  # noqa: SIM117
            async with session.get(url, headers=base_headers) as response:
                if response.status == 200:  # noqa: PLR2004
                    return await response.json()
                msg = f"Failed to list repositories: {response.status}"
                raise ResourceNotFoundError(msg)

    async def list_repositories_async(self) -> list[BaseRepository]:
        """List all repositories owned by this user."""
        try:
            repos = await self.list_repos_owned_by_user()
            return [
                create_repository(repo["html_url"])
                for repo in repos
                if repo.get("html_url")
            ]
        except Exception as e:
            msg = f"Failed to list repositories: {e!s}"
            raise ResourceNotFoundError(msg) from e

    async def create_repository_async(
        self,
        name: str,
        description: str = "",
        private: bool = False,
    ) -> BaseRepository:
        """Create a new repository.

        Args:
            name: Repository name
            description: Repository description
            private: Whether to create a private repository

        Returns:
            Newly created repository
        """
        try:
            resp = await self._gh.rest.repos.async_create_for_authenticated_user(
                name=name,
                description=description,
                private=private,
            )
            return create_repository(resp.parsed_data.html_url)
        except GitHubException as e:
            msg = f"Failed to create repository: {e!s}"
            raise ResourceNotFoundError(msg) from e

    async def get_user_async(self) -> User:
        """Get user information."""
        try:
            resp = await self._gh.rest.users.async_get_by_username(self._name)
            return githubkittools.create_user_model(resp.parsed_data)
        except GitHubException as e:
            msg = f"Failed to get user info: {e!s}"
            raise ResourceNotFoundError(msg) from e

    async def delete_repository_async(self, name: str) -> None:
        """Delete a repository.

        Args:
            name: Name of repository to delete
        """
        try:
            await self._gh.rest.repos.async_delete(self._name, name)
        except GitHubException as e:
            msg = f"Failed to delete repository: {e!s}"
            raise ResourceNotFoundError(msg) from e
