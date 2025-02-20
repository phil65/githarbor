from __future__ import annotations

import os
from unittest.mock import Mock, patch

import pytest

from githarbor.providers.gitlab_provider import GitLabRepository


TOKEN = os.getenv("GITHUB_TOKEN")
OWNER = "phil65"
REPO = "test"
BRANCH = "master"


@pytest.fixture
def mock_gitlab_client():
    return Mock()


@pytest.fixture
def gitlab_repo(mock_gitlab_client) -> GitLabRepository:
    with patch("gitlab.Gitlab") as mock_gitlab:
        mock_gitlab.return_value = mock_gitlab_client
        provider = GitLabRepository("phil65", "test")
        provider._gl = mock_gitlab_client
        return provider


def supports_url():
    assert GitLabRepository.supports_url("https://gitlab.com/owner/repo")
    assert not GitLabRepository.supports_url("https://github.com/owner/repo")


def test_initialization():
    """Test basic initialization of GitLabRepository."""
    repo = GitLabRepository(OWNER, REPO)
    assert repo._owner == OWNER
    assert repo._name == REPO


def test_from_url():
    """Test creating repository from URL."""
    url = "https://gitlab.com/phil65/test"
    repo = GitLabRepository.from_url(url)
    assert repo._owner == "phil65"
    assert repo._name == "test"


def test_invalid_url():
    """Test creating repository from invalid URL."""
    import gitlab.exceptions

    with pytest.raises(gitlab.exceptions.GitlabParsingError):
        GitLabRepository.from_url("https://invalid-url.com/owner/repo")


def test_get_default_branch(gitlab_repo: GitLabRepository, mock_gitlab_client):
    """Test getting default branch."""
    mock_project = Mock()
    mock_project.default_branch = "main"
    mock_gitlab_client.projects.get.return_value = mock_project
    # branch = gitlab_repo.get_default_branch()
    # assert branch == "main"


# def test_get_branches(gitlab_repo, mock_gitlab_client):
#     """Test getting all branches"""
#     mock_project = Mock()
#     mock_project.branches.list.return_value = [
#         Mock(name="main"),
#         Mock(name="develop")
#     ]
#     mock_gitlab_client.projects.get.return_value = mock_project

#     branches = gitlab_repo.get_branches()
#     assert len(branches) == 2
#     assert "main" in branches
#     assert "develop" in branches


# def test_get_file_content(gitlab_repo, mock_gitlab_client):
#     """Test getting file content"""
#     mock_project = Mock()
#     mock_file = Mock()
#     mock_file.decode.return_value = "file content"
#     mock_project.files.get.return_value = mock_file
#     mock_gitlab_client.projects.get.return_value = mock_project

#     content = gitlab_repo.get_file_content("path/to/file.txt", "main")
#     assert content == "file content"


def test_supports_url_variations():
    """Test URL support with different variations."""
    assert GitLabRepository.supports_url("https://gitlab.com/owner/repo")
    assert GitLabRepository.supports_url("http://gitlab.com/owner/repo")
    assert GitLabRepository.supports_url("gitlab.com/owner/repo")
    assert not GitLabRepository.supports_url("https://github.com/owner/repo")
    assert not GitLabRepository.supports_url("https://bitbucket.org/owner/repo")


# def test_get_repo_url(gitlab_repo):
#     """Test getting repository URL"""
#     expected_url = f"https://gitlab.com/{OWNER}/{REPO}"
#     assert gitlab_repo.get_repo_url() == expected_url

# @pytest.mark.parametrize("invalid_input", [
#     ("", "repo"),
#     ("owner", ""),
#     (None, "repo"),
#     ("owner", None)
# ])
# def test_invalid_initialization(invalid_input):
#     """Test initialization with invalid inputs"""
#     owner, repo = invalid_input
#     with pytest.raises(ValueError):
#         GitLabRepository(owner, repo)
