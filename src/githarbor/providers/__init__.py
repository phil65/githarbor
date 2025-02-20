"""Githarbor providers package."""

from __future__ import annotations

import importlib.util

if importlib.util.find_spec("github"):
    from githarbor.providers.github_provider import GitHubRepository

if importlib.util.find_spec("gitlab"):
    from githarbor.providers.gitlab_provider import GitLabRepository

if importlib.util.find_spec("giteapy"):
    from githarbor.providers.gitea_provider import GiteaRepository

if importlib.util.find_spec("azure"):
    from githarbor.providers.azure_provider import AzureRepository

if importlib.util.find_spec("aiogithubapi"):
    from githarbor.providers.aiogithubapi_provider import AioGitHubRepository

if importlib.util.find_spec("githubkit"):
    from githarbor.providers.githubkit_provider import GitHubKitRepository

if importlib.util.find_spec("gitpython"):
    from githarbor.providers.local_provider import LocalRepository


# if importlib.util.find_spec("atlassian"):
#     from githarbor.providers.bitbucketrepository import BitbucketRepository

__all__ = [
    "AioGitHubRepository",
    "AzureRepository",
    "GitHubKitRepository",
    "GitHubRepository",
    "GitLabRepository",
    "GiteaRepository",
    "LocalRepository",
    # "BitbucketRepository",
]
