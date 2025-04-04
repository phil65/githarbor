from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import urlparse

from githarbor.core.base import BaseOwner, BaseRepository
from githarbor.exceptions import AuthenticationError, ResourceNotFoundError
from githarbor.providers.github_provider import utils as githubtools


if TYPE_CHECKING:
    from githarbor.core.models import User


logger = logging.getLogger(__name__)
HTML_ERROR_CODE = 404
TOKEN = os.getenv("GITHUB_TOKEN")


class GitHubOwner(BaseOwner):
    url_patterns: ClassVar = ["github.com"]
    is_async = False

    def __init__(self, username: str, token: str | None = None):
        from github import Auth, Github, GithubException
        from github.AuthenticatedUser import AuthenticatedUser

        try:
            t = token or TOKEN
            if t is None:
                logger.info("No GitHub token provided. Stricter rate limit.")
            auth = Auth.Token(t) if t else None
            self._gh = Github(auth=auth)
            self._name = username
            self._user = self._gh.get_user()
            assert isinstance(self._user, AuthenticatedUser)
        except GithubException as e:
            msg = f"GitHub authentication failed: {e!s}"
            raise AuthenticationError(msg) from e

    @property
    def name(self) -> str:
        """The name of the owner."""
        return self._name

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> GitHubOwner:
        # Handle URLs like "https://github.com/phil65"
        parsed = urlparse(url)
        parts = parsed.path.strip("/").split("/")
        return cls(username=parts[0], token=kwargs.get("token"))

    @githubtools.handle_github_errors("Failed to list repositories")
    def list_repositories(self) -> list[BaseRepository]:
        """List all repositories owned by this user."""
        from githarbor.repositories import create_repository

        return [create_repository(repo.html_url) for repo in self._user.get_repos()]

    @githubtools.handle_github_errors("Failed to create repository {name}")
    def create_repository(
        self,
        name: str,
        description: str = "",
        private: bool = False,
    ) -> BaseRepository:
        """Create a new repository."""
        from github.AuthenticatedUser import AuthenticatedUser

        from githarbor.repositories import create_repository

        assert isinstance(self._user, AuthenticatedUser)
        repo = self._user.create_repo(name=name, description=description, private=private)
        return create_repository(repo.html_url)

    @githubtools.handle_github_errors("Failed to get user information")
    def get_user(self) -> User:
        """Get user information."""
        return githubtools.create_user_model(self._user)

    @githubtools.handle_github_errors("Failed to delete repository {name}")
    def delete_repository(self, name: str) -> None:
        """Delete a repository."""
        from github import GithubException

        try:
            repo = self._gh.get_repo(f"{self._user.login}/{name}")
            repo.delete()
        except GithubException as e:
            if e.status == HTML_ERROR_CODE:
                msg = f"Repository {name} not found"
                raise ResourceNotFoundError(msg) from e
            raise


if __name__ == "__main__":

    async def main():
        provider = GitHubOwner("phil65")
        repos = provider.list_repositories()
        print(repos)

    import asyncio

    asyncio.run(main())
