from __future__ import annotations

import importlib.util

if importlib.util.find_spec("github"):
    from githarbor.providers.githubrepository import GitHubRepository

if importlib.util.find_spec("gitlab"):
    from githarbor.providers.gitlabrepository import GitLabRepository

if importlib.util.find_spec("giteapy"):
    from githarbor.providers.gitearepository import GiteaRepository

if importlib.util.find_spec("azure"):
    from githarbor.providers.azurerepository import AzureRepository


# if importlib.util.find_spec("atlassian"):
#     from githarbor.providers.bitbucketrepository import BitbucketRepository

__all__ = [
    "GitHubRepository",
    "GitLabRepository",
    "GiteaRepository",
    "AzureRepository",
    # "BitbucketRepository",
]
