from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from githarbor.exceptions import RepositoryNotFoundError


if TYPE_CHECKING:
    from githarbor.core.base import Repository


class RepoRegistry:
    _repos: ClassVar[dict[str, type[Repository]]] = {}

    @classmethod
    def register(cls, name: str):
        """Decorator to register a repository class."""

        def decorator(repo_class: type[Repository]) -> type[Repository]:
            cls._repos[name] = repo_class
            return repo_class

        return decorator

    @classmethod
    def create(cls, name: str, **kwargs: Any) -> Repository:
        """Create a repository instance by name."""
        try:
            repo_class = cls._repos.get(name)
            if not repo_class:
                msg = f"Repository type {name} not found"
                raise RepositoryNotFoundError(msg)  # noqa: TRY301
            return repo_class(**kwargs)
        except Exception as e:
            msg = f"Failed to create repository {name}: {e!s}"
            raise RepositoryNotFoundError(msg) from e

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> Repository:
        """Create a repository instance from a URL."""
        for repo_class in cls._repos.values():
            if repo_class.supports_url(url):
                return repo_class.from_url(url, **kwargs)

        msg = f"No repository implementation found for URL: {url}"
        raise RepositoryNotFoundError(msg)

    @classmethod
    def get_repo_class_for_url(cls, url: str) -> type[Repository] | None:
        """Get the repository class that can handle the given URL."""
        for repo_class in cls._repos.values():
            if repo_class.supports_url(url):
                return repo_class
        return None

    @classmethod
    def get_registered_repos(cls) -> list[str]:
        """Get a list of all registered repository types."""
        return list(cls._repos.keys())


if __name__ == "__main__":
    # Using registry directly
    github = RepoRegistry.from_url("https://github.com/phil65/mknodes")
    print(github)
    # # Check which provider handles a URL
    # provider_class = RepoRegistry.get_provider_for_url(
    #     "https://github.com/owner/repo"
    # )
    # if provider_class:
    #     provider = provider_class.from_url(
    #         "https://github.com/owner/repo", token="my-token"
    #     )
