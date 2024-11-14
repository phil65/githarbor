from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

from githarbor.exceptions import FeatureNotSupportedError


if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime
    import os

    from githarbor.core.models import (
        Branch,
        Commit,
        Issue,
        PullRequest,
        Release,
        Tag,
        User,
        Workflow,
        WorkflowRun,
    )


class BaseRepository:
    """Base repository class. All methods raise FeatureNotSupportedError by default."""

    url_patterns: ClassVar[list[str]] = []

    @property
    def name(self):
        """The name of the repository."""
        raise NotImplementedError

    @property
    def default_branch(self):
        """The default branch of this repository."""
        raise NotImplementedError

    @classmethod
    def supports_url(cls, url: str) -> bool:
        return any(pattern in url for pattern in cls.url_patterns)

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> BaseRepository:
        msg = f"{cls.__name__} does not implement from_url"
        raise FeatureNotSupportedError(msg)

    def get_branch(self, name: str) -> Branch:
        msg = f"{self.__class__.__name__} does not implement get_branch"
        raise FeatureNotSupportedError(msg)

    def list_branches(self) -> list[Branch]:
        msg = f"{self.__class__.__name__} does not implement list_branches"
        raise FeatureNotSupportedError(msg)

    def get_pull_request(self, number: int) -> PullRequest:
        msg = f"{self.__class__.__name__} does not implement get_pull_request"
        raise FeatureNotSupportedError(msg)

    def list_pull_requests(self, state: str = "open") -> list[PullRequest]:
        msg = f"{self.__class__.__name__} does not implement list_pull_requests"
        raise FeatureNotSupportedError(msg)

    def get_issue(self, issue_id: int) -> Issue:
        msg = f"{self.__class__.__name__} does not implement get_issue"
        raise FeatureNotSupportedError(msg)

    def list_issues(self, state: str = "open") -> list[Issue]:
        msg = f"{self.__class__.__name__} does not implement list_issues"
        raise FeatureNotSupportedError(msg)

    def get_commit(self, sha: str) -> Commit:
        msg = f"{self.__class__.__name__} does not implement get_commit"
        raise FeatureNotSupportedError(msg)

    def list_commits(
        self,
        branch: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        author: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        msg = f"{self.__class__.__name__} does not implement list_commits"
        raise FeatureNotSupportedError(msg)

    def get_workflow(self, workflow_id: str) -> Workflow:
        msg = f"{self.__class__.__name__} does not implement get_workflow"
        raise FeatureNotSupportedError(msg)

    def list_workflows(self) -> list[Workflow]:
        msg = f"{self.__class__.__name__} does not implement list_workflows"
        raise FeatureNotSupportedError(msg)

    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        msg = f"{self.__class__.__name__} does not implement get_workflow_run"
        raise FeatureNotSupportedError(msg)

    def download(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ) -> None:
        msg = f"{self.__class__.__name__} does not implement download"
        raise FeatureNotSupportedError(msg)

    def search_commits(
        self,
        query: str,
        branch: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        msg = f"{self.__class__.__name__} does not implement search_commits"
        raise FeatureNotSupportedError(msg)

    def iter_files(
        self,
        path: str = "",
        ref: str | None = None,
        pattern: str | None = None,
    ) -> Iterator[str]:
        msg = f"{self.__class__.__name__} does not implement iter_files"
        raise FeatureNotSupportedError(msg)

    def get_contributors(
        self,
        sort_by: Literal["commits", "name", "date"] = "commits",
        limit: int | None = None,
    ) -> list[User]:
        msg = f"{self.__class__.__name__} does not implement get_contributors"
        raise FeatureNotSupportedError(msg)

    def get_languages(self) -> dict[str, int]:
        msg = f"{self.__class__.__name__} does not implement get_languages"
        raise FeatureNotSupportedError(msg)

    def compare_branches(
        self,
        base: str,
        head: str,
        include_commits: bool = True,
        include_files: bool = True,
        include_stats: bool = True,
    ) -> dict[str, Any]:
        msg = f"{self.__class__.__name__} does not implement compare_branches"
        raise FeatureNotSupportedError(msg)

    def get_latest_release(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:
        msg = f"{self.__class__.__name__} does not implement get_latest_release"
        raise FeatureNotSupportedError(msg)

    def list_releases(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        msg = f"{self.__class__.__name__} does not implement list_releases"
        raise FeatureNotSupportedError(msg)

    def get_release(self, tag: str) -> Release:
        msg = f"{self.__class__.__name__} does not implement get_release"
        raise FeatureNotSupportedError(msg)

    def get_tag(self, name: str) -> Tag:
        msg = f"{self.__class__.__name__} does not implement get_tag"
        raise FeatureNotSupportedError(msg)

    def list_tags(self) -> list[Tag]:
        msg = f"{self.__class__.__name__} does not implement list_tags"
        raise FeatureNotSupportedError(msg)
