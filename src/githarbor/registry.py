"""Module containing the repository registry."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from githarbor.core.proxy import Repository
from githarbor.exceptions import RepositoryNotFoundError


if TYPE_CHECKING:
    from githarbor.core.base import BaseRepository


class RepoRegistry:
    """Registry for repository implementations."""

    _repos: ClassVar[dict[str, type[BaseRepository]]] = {}

    @classmethod
    def register(cls, name: str):
        """Decorator to register a repository class.

        Args:
            name: Name to register the repository under.
        """

        def decorator(repo_class: type[BaseRepository]) -> type[BaseRepository]:
            cls._repos[name] = repo_class
            return repo_class

        return decorator

    @classmethod
    def create(cls, name: str, **kwargs: Any) -> Repository:
        """Create a proxy-wrapped repository instance by name.

        Args:
            name: Name of the repository type.
            **kwargs: Repository-specific configuration.

        Returns:
            Proxy-wrapped repository instance.

        Raises:
            RepositoryNotFoundError: If repository type is not found or creation fails.
        """
        if not (repo_class := cls._repos.get(name)):
            msg = f"Repository type {name} not found"
            raise RepositoryNotFoundError(msg)
        return Repository(repo_class(**kwargs))

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> Repository:
        """Create a proxy-wrapped repository instance from a URL.

        Args:
            url: Repository URL.
            **kwargs: Repository-specific configuration.

        Returns:
            Proxy-wrapped repository instance.

        Raises:
            RepositoryNotFoundError: If no implementation supports the URL.
        """
        for repo_class in cls._repos.values():
            if repo_class.supports_url(url):
                return Repository(repo_class.from_url(url, **kwargs))

        msg = f"No repository implementation found for URL: {url}"
        raise RepositoryNotFoundError(msg)

    @classmethod
    def get_repo_class_for_url(cls, url: str) -> type[BaseRepository] | None:
        """Get the repository class that can handle the given URL.

        Args:
            url: Repository URL.

        Returns:
            Repository class or None if no implementation supports the URL.
        """
        return next(
            (
                repo_class
                for repo_class in cls._repos.values()
                if repo_class.supports_url(url)
            ),
            None,
        )

    @classmethod
    def get_registered_repos(cls) -> list[str]:
        """Get a list of all registered repository types.

        Returns:
            List of registered repository type names.
        """
        return list(cls._repos)
