from __future__ import annotations

import importlib.util
from typing import TYPE_CHECKING, Any

from githarbor.exceptions import RepositoryNotFoundError
from githarbor.registry import RepoRegistry


if importlib.util.find_spec("github"):
    from githarbor.providers.githubrepository import GitHubRepository

    RepoRegistry.register("github")(GitHubRepository)

if importlib.util.find_spec("gitlab"):
    from githarbor.providers.gitlabrepository import GitLabRepository

    RepoRegistry.register("gitlab")(GitLabRepository)

if importlib.util.find_spec("giteapy"):
    from githarbor.providers.gitearepository import GiteaRepository

    RepoRegistry.register("gitea")(GiteaRepository)

if importlib.util.find_spec("azure"):
    from githarbor.providers.azurerepository import AzureRepository

    RepoRegistry.register("azure")(AzureRepository)

# if importlib.util.find_spec("atlassian"):
#     from githarbor.providers.bitbucketrepository import BitbucketRepository

#     RepoRegistry.register("bitbucket")(BitbucketRepository)

if TYPE_CHECKING:
    from githarbor.core.base import Repository


def create_repository(url: str, **kwargs: Any) -> Repository:
    """Create a repository instance from a URL.

    Args:
        url: The repository URL (e.g. 'https://github.com/owner/repo')
        **kwargs: Repository-specific configuration (tokens, credentials, etc.)

    Returns:
        Repository: Configured repository instance

    Raises:
        RepositoryNotFoundError: If the repository cannot be created or URL isnt supported

    Example:
        >>> repo = create_repository('https://github.com/owner/repo', token='my-token')
        >>> issues = repo.list_issues()
    """
    try:
        return RepoRegistry.from_url(url, **kwargs)
    except Exception as e:
        msg = f"Failed to create repository from {url}: {e!s}"
        raise RepositoryNotFoundError(msg) from e
