from __future__ import annotations

import os

import pytest

from githarbor.exceptions import RepositoryNotFoundError
from githarbor.providers.github_provider import GitHubRepository
from githarbor.providers.gitlab_provider import GitLabRepository
from githarbor.repositories import create_repository


BITBUCKET_TOKEN = os.getenv("BITBUCKET_TOKEN")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")


def test_create_github_repository():
    repo = create_repository("https://github.com/phil65/mknodes")
    assert isinstance(repo._repository, GitHubRepository)
    assert repo.name == "mknodes"


def test_create_gitlab_repository():
    repo = create_repository("https://gitlab.com/phil65/test")
    assert isinstance(repo._repository, GitLabRepository)
    assert repo.name == "test"


# def test_create_bitbucket_repository():
#     repo = create_repository("https://bitbucket.org/phil__65/testrepo")
#     assert isinstance(repo._repository, BitbucketRepository)
#     assert repo.name == "testrepo"


def test_invalid_url():
    with pytest.raises(RepositoryNotFoundError):
        create_repository("https://invalid.com/owner/repo")
