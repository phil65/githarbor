from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any, ClassVar

from githarbor.core.base import BaseOwner, BaseRepository
from githarbor.exceptions import AuthenticationError, ResourceNotFoundError
from githarbor.providers.aiogithubapi_provider import utils as aiogithubapitools
from githarbor.repositories import create_repository


if TYPE_CHECKING:
    from githarbor.core.models import User

logger = logging.getLogger(__name__)


class AioGitHubOwner(BaseOwner):
    """Owner implementation using aiogithubapi."""

    url_patterns: ClassVar[list[str]] = ["github.com"]
    is_async = True

    def __init__(self, username: str, token: str | None = None) -> None:
        """Initialize GitHub API client.

        Args:
            username: Username of the owner
            token: GitHub access token
        """
        from aiogithubapi import GitHubAPI
        from aiogithubapi.exceptions import GitHubAuthenticationException

        try:
            token = token or os.getenv("GITHUB_TOKEN")
            if not token:
                msg = "GitHub token is required"
                raise ValueError(msg)
            self._gh = GitHubAPI(token=token)
            self._name = username

        except GitHubAuthenticationException as e:
            msg = f"GitHub authentication failed: {e}"
            raise AuthenticationError(msg) from e

    @property
    def name(self) -> str:
        """The name of the owner."""
        return self._name

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> AioGitHubOwner:
        """Create instance from URL.

        Args:
            url: URL like "https://github.com/octocat"
            **kwargs: Additional arguments including token
        """
        # Remove trailing slashes and handle both user & org URLs
        url = url.rstrip("/")
        username = url.split("/")[-1]
        return cls(username=username, token=kwargs.get("token"))

    async def list_repositories_async(self) -> list[BaseRepository]:
        """List all repositories owned by this user."""
        from aiogithubapi.exceptions import GitHubAuthenticationException

        try:
            response = await self._gh.user.repos()
            assert response.data
            return [create_repository(r.html_url) for r in response.data if r.html_url]
        except GitHubAuthenticationException as e:
            msg = f"Failed to list repositories: {e}"
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
        from aiogithubapi import GitHubRequestKwarg
        from aiogithubapi.exceptions import GitHubAuthenticationException
        from aiogithubapi.github import HttpMethod

        try:
            data = {"name": name, "description": description, "private": private}
            kwargs: dict[Any, Any] = {GitHubRequestKwarg.METHOD: HttpMethod.POST}
            ep = "/user/repos"
            response = await self._gh.generic(endpoint=ep, data=data, **kwargs)
            assert response.data
            return create_repository(response.data["html_url"])
        except GitHubAuthenticationException as e:
            msg = f"Failed to create repository: {e}"
            raise ResourceNotFoundError(msg) from e

    async def get_user_async(self) -> User:
        """Get user information."""
        from aiogithubapi.exceptions import GitHubAuthenticationException

        try:
            response = await self._gh.users.get(self._name)
            assert response.data
            return aiogithubapitools.create_user_model(response.data)
        except GitHubAuthenticationException as e:
            msg = f"Failed to get user info: {e}"
            raise ResourceNotFoundError(msg) from e

    async def delete_repository_async(self, name: str) -> None:
        """Delete a repository.

        Args:
            name: Name of repository to delete
        """
        from aiogithubapi import GitHubRequestKwarg
        from aiogithubapi.exceptions import GitHubAuthenticationException
        from aiogithubapi.github import HttpMethod

        try:
            kwargs = {GitHubRequestKwarg.METHOD: HttpMethod.DELETE}
            ep = f"/repos/{self._name}/{name}"
            await self._gh.generic(endpoint=ep, **kwargs)  # type: ignore
        except GitHubAuthenticationException as e:
            msg = f"Failed to delete repository: {e}"
            raise ResourceNotFoundError(msg) from e
