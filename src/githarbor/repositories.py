from __future__ import annotations

import importlib.util
from typing import TYPE_CHECKING, Any

from githarbor.exceptions import RepositoryNotFoundError
from githarbor.registry import RepoRegistry


if TYPE_CHECKING:
    from githarbor.core.proxy import Repository


if importlib.util.find_spec("github"):
    from githarbor.providers.github_provider.repository import GitHubRepository

    RepoRegistry.register("github")(GitHubRepository)

if importlib.util.find_spec("aiogithubapi"):
    from githarbor.providers.aiogithubapi_provider.repository import AioGitHubRepository

    RepoRegistry.register("aiogithubapi")(AioGitHubRepository)

if importlib.util.find_spec("gitlab"):
    from githarbor.providers.gitlab_provider.repository import GitLabRepository

    RepoRegistry.register("gitlab")(GitLabRepository)

if importlib.util.find_spec("giteapy"):
    from githarbor.providers.gitea_provider.repository import GiteaRepository

    RepoRegistry.register("gitea")(GiteaRepository)

if importlib.util.find_spec("azure"):
    from githarbor.providers.azure_provider.repository import AzureRepository

    RepoRegistry.register("azure")(AzureRepository)

if importlib.util.find_spec("githubkit"):
    from githarbor.providers.githubkit_provider.repository import GitHubKitRepository

    RepoRegistry.register("githubkit")(GitHubKitRepository)

# if importlib.util.find_spec("atlassian"):
#     from githarbor.providers.bitbucket_provider.repository import BitbucketRepository

#     RepoRegistry.register("bitbucket")(BitbucketRepository)


if importlib.util.find_spec("git"):
    # registered last so that it's the fallback, since we allow upaths this would
    # also pick up all other URLs.
    from githarbor.providers.local_provider.repository import LocalRepository

    RepoRegistry.register("local")(LocalRepository)


def create_repository(url: str, **kwargs: Any) -> Repository:
    """Create a proxy-wrapped repository instance from a URL.

    Args:
        url: The repository URL (e.g. 'https://github.com/owner/repo')
        **kwargs: Repository-specific configuration (tokens, credentials, etc.)

    Returns:
        Repository: Proxy-wrapped repository instance

    Raises:
        RepositoryNotFoundError: If the URL isn't supported or no repository found

    Example:
        >>> repo = create_repository('https://github.com/owner/repo', token='my-token')
        >>> issues = repo.list_issues()
    """
    try:
        return RepoRegistry.from_url(url, **kwargs)
    except Exception as e:
        msg = f"Failed to create repository from {url}: {e!s}"
        raise RepositoryNotFoundError(msg) from e


if __name__ == "__main__":
    repo = create_repository(".")
    print(repo._repository)
