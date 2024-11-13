from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar


if TYPE_CHECKING:
    from datetime import datetime
    import os

    from githarbor.core.models import (
        Branch,
        Commit,
        Issue,
        PullRequest,
        Workflow,
        WorkflowRun,
    )


class Repository(ABC):
    """Base class representing a specific Git repository."""

    _name: str
    _owner: str
    url_patterns: ClassVar[list[str]] = []

    def __repr__(self):
        return f"{type(self).__name__}(name={self._name}, owner={self._owner})"

    @classmethod
    def supports_url(cls, url: str) -> bool:
        """Check if this provider can handle the given URL."""
        return any(pattern in url for pattern in cls.url_patterns)

    @classmethod
    @abstractmethod
    def from_url(cls, url: str, **kwargs: Any) -> Repository:
        """Create a repository instance from a URL."""
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        """Repository name."""
        raise NotImplementedError

    @property
    @abstractmethod
    def default_branch(self) -> str:
        """Default branch name."""
        raise NotImplementedError

    @abstractmethod
    def get_branch(self, name: str) -> Branch:
        """Get a specific branch."""
        raise NotImplementedError

    @abstractmethod
    def get_pull_request(self, number: int) -> PullRequest:
        """Get a specific pull request."""
        raise NotImplementedError

    @abstractmethod
    def list_pull_requests(self, state: str = "open") -> list[PullRequest]:
        """List pull requests."""
        raise NotImplementedError

    @abstractmethod
    def get_issue(self, issue_id: int) -> Issue:
        """Get a specific issue."""
        raise NotImplementedError

    @abstractmethod
    def list_issues(self, state: str = "open") -> list[Issue]:
        """List issues."""
        raise NotImplementedError

    @abstractmethod
    def get_commit(self, sha: str) -> Commit:
        """Get a specific commit."""
        raise NotImplementedError

    @abstractmethod
    def list_commits(
        self,
        branch: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        author: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        """List commits."""
        raise NotImplementedError

    @abstractmethod
    def get_workflow(self, workflow_id: str) -> Workflow:
        """Get a specific workflow."""
        raise NotImplementedError

    @abstractmethod
    def list_workflows(self) -> list[Workflow]:
        """List workflows."""
        raise NotImplementedError

    @abstractmethod
    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        """Get a specific workflow run."""
        raise NotImplementedError

    def download(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ):
        """Download a file or directory from this GitLab repository.

        Args:
            path: Path to the file or directory we want to download.
            destination: Path where file/directory should be saved.
            recursive: Download all files from a folder (and subfolders).
        """
        raise NotImplementedError
