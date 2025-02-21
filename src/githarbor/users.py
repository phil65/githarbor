from __future__ import annotations

import importlib.util
from typing import TYPE_CHECKING, Any

from githarbor.exceptions import OwnerNotFoundError
from githarbor.registry import RepoRegistry


if TYPE_CHECKING:
    from githarbor.core.user_proxy import Owner


if importlib.util.find_spec("github"):
    from githarbor.providers.github_provider.owner import GitHubOwner

    RepoRegistry.register_owner("github")(GitHubOwner)

if importlib.util.find_spec("aiogithubapi"):
    from githarbor.providers.aiogithubapi_provider.owner import AioGitHubOwner

    RepoRegistry.register_owner("aiogithubapi")(AioGitHubOwner)

if importlib.util.find_spec("githubkit"):
    from githarbor.providers.githubkit_provider.owner import GitHubKitOwner

    RepoRegistry.register_owner("githubkit")(GitHubKitOwner)


def create_owner(url: str, **kwargs: Any) -> Owner:
    """Create a proxy-wrapped owner instance from a URL.

    Args:
        url: The owner URL (e.g. 'https://github.com/phil65')
        **kwargs: Provider-specific configuration (tokens, etc.)

    Returns:
        Owner: Proxy-wrapped owner instance

    Raises:
        OwnerNotFoundError: If the URL isn't supported or no owner found
    """
    try:
        return RepoRegistry.owner_from_url(url, **kwargs)
    except Exception as e:
        msg = f"Failed to create owner from {url}: {e!s}"
        raise OwnerNotFoundError(msg) from e


if __name__ == "__main__":
    owner = create_owner("https://github.com/phil65")
    print(owner)
