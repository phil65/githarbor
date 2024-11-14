from typing import Any, ClassVar

import pytest

from githarbor.core import base
from githarbor.exceptions import RepositoryNotFoundError
from githarbor.registry import RepoRegistry


class DummyRepository(base.BaseRepository):
    url_patterns: ClassVar[list[str]] = ["test.com"]

    def __init__(self, owner, repo, **kwargs: Any):
        self.kwargs = kwargs

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> base.BaseRepository:
        return cls("test", "test")


def test_register_repo():
    # Clear existing registrations
    RepoRegistry._repos = {}  # Changed from _repos to _repos

    @RepoRegistry.register("test")
    class DummyRepositoryRegistered(DummyRepository):
        pass

    assert "test" in RepoRegistry._repos
    assert RepoRegistry._repos["test"] == DummyRepositoryRegistered


def test_create_repo():
    # Register test repo
    RepoRegistry._repos = {"test": DummyRepository}

    # Create by name
    repo = RepoRegistry.create("test", owner="test", repo="test", token="test-token")
    assert isinstance(repo._repository, DummyRepository)
    assert repo._repository.kwargs["token"] == "test-token"

    # Invalid repo
    with pytest.raises(RepositoryNotFoundError):
        RepoRegistry.create("invalid")


def test_from_url():
    # Register test repo
    RepoRegistry._repos = {"test": DummyRepository}

    # Create from URL
    repo = RepoRegistry.from_url("https://test.com/owner/repo", token="test-token")
    assert isinstance(repo._repository, DummyRepository)

    # Invalid URL
    with pytest.raises(RepositoryNotFoundError):
        RepoRegistry.from_url("https://invalid.com/owner/repo")


def test_get_repo_class_for_url():
    # Register test repo
    RepoRegistry._repos = {"test": DummyRepository}

    # Get repo class
    repo_class = RepoRegistry.get_repo_class_for_url("https://test.com/owner/repo")
    assert repo_class == DummyRepository

    # Invalid URL
    repo_class = RepoRegistry.get_repo_class_for_url("https://invalid.com/owner/repo")
    assert repo_class is None


def test_get_registered_repos():
    # Register test repos
    RepoRegistry._repos = {"test1": DummyRepository, "test2": DummyRepository}

    repos = RepoRegistry.get_registered_repos()
    assert set(repos) == {"test1", "test2"}
