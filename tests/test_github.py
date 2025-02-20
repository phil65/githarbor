from __future__ import annotations

import os
from unittest.mock import Mock, patch

import pytest

from githarbor.exceptions import ResourceNotFoundError
from githarbor.providers.github_provider import GitHubRepository


TOKEN = os.getenv("GITHUB_TOKEN")
OWNER = "phil65"
REPO = "mknodes"
BRANCH = "main"


@pytest.fixture
def mock_github():
    with patch("github.Github") as mock:
        yield mock


@pytest.fixture
def github_repo(mock_github):
    # Setup mock repository
    mock_repo = Mock()
    mock_repo.name = REPO
    mock_repo.default_branch = BRANCH

    mock_github.return_value.get_repo.return_value = mock_repo

    return GitHubRepository(OWNER, REPO, token=TOKEN)


def test_github_repository_init(mock_github):
    repo = GitHubRepository(OWNER, REPO, token=TOKEN)
    assert repo.name == "mknodes"
    mock_github.assert_called_once()


def test_github_repository_from_url(mock_github):
    repo = GitHubRepository.from_url(f"https://github.com/{OWNER}/{REPO}", token=TOKEN)
    assert repo.name == "mknodes"


def test_get_branch(github_repo):
    # Setup mock branch
    mock_branch = Mock()
    mock_branch.name = "main"
    mock_branch.commit.sha = "abc123"
    mock_branch.protected = False

    github_repo._repo.get_branch.return_value = mock_branch

    branch = github_repo.get_branch("main")
    assert branch.name == "main"
    assert branch.sha == "abc123"
    assert branch.protected is False


def test_branch_not_found(github_repo):
    from github.GithubException import GithubException

    github_repo._repo.get_branch.side_effect = GithubException(404, "Not Found")

    with pytest.raises(ResourceNotFoundError):
        github_repo.get_branch("nonexistent")
