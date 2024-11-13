from __future__ import annotations

from githarbor.providers.testrepository import DummyRepository
import importlib.util

if importlib.util.find_spec("github"):
    from githarbor.providers.githubrepository import GitHubRepository
if importlib.util.find_spec("gitlab"):
    from githarbor.providers.gitlabrepository import GitLabRepository
# if importlib.util.find_spec("atlassian"):
#     from githarbor.providers.bitbucketrepository import BitbucketRepository

__all__ = [
    "GitHubRepository",
    "GitLabRepository",
    # "BitbucketRepository",
    "DummyRepository",
]
