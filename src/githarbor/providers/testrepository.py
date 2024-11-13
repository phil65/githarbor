from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

from githarbor.core.base import Repository
from githarbor.core.models import (
    Branch,
    Commit,
    Issue,
    PullRequest,
    Release,
    User,
    Workflow,
    WorkflowRun,
)


if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime
    import os


class DummyRepository(Repository):
    url_patterns: ClassVar[list[str]] = ["test.com"]

    @classmethod
    def supports_url(cls, url: str) -> bool:
        return super().supports_url(url)

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> DummyRepository:
        return cls(**kwargs)

    def __init__(self, **kwargs: Any):
        self._name = kwargs.get("name", "test-repo")
        self._owner = kwargs.get("owner", "test-owner")
        self.kwargs = kwargs

    @property
    def name(self) -> str:
        return self._name

    @property
    def default_branch(self) -> str:
        return "main"

    def get_branch(self, name: str) -> Branch:
        return Branch(
            name=name,
            sha="test-sha",
            protected=False,
            default=(name == self.default_branch),
        )

    def get_pull_request(self, number: int) -> PullRequest:
        return PullRequest(
            title=f"Test PR #{number}",
            source_branch="feature",
            target_branch=self.default_branch,
            number=number,
        )

    def list_pull_requests(self, state: str = "open") -> list[PullRequest]:
        return [self.get_pull_request(1), self.get_pull_request(2)]

    def get_issue(self, number: int) -> Issue:
        return Issue(number=number, title=f"Test Issue #{number}")

    def list_issues(self, state: str = "open") -> list[Issue]:
        return [self.get_issue(1), self.get_issue(2)]

    def get_commit(self, sha: str) -> Commit:
        from datetime import datetime

        from githarbor.core.models import User

        return Commit(
            sha=sha,
            message="Test commit",
            author=User(username="test-user"),
            created_at=datetime.now(),
        )

    def list_commits(
        self,
        branch: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        author: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        return [self.get_commit("test-sha-1"), self.get_commit("test-sha-2")]

    def get_workflow(self, workflow_id: str) -> Workflow:
        """Get a specific workflow."""
        from datetime import datetime

        return Workflow(
            id=workflow_id,
            name=f"Test Workflow {id}",
            path=".github/workflows/test.yml",
            state="active",
            created_at=datetime.now(),
        )

    def list_workflows(self) -> list[Workflow]:
        """List workflows."""
        return [self.get_workflow("1"), self.get_workflow("2")]

    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        """Get a specific workflow run."""
        from datetime import datetime

        return WorkflowRun(
            id=run_id,
            name=f"Test Workflow Run {id}",
            workflow_id="1",
            status="completed",
            conclusion="success",
            created_at=datetime.now(),
        )

    def search_commits(
        self,
        query: str,
        branch: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        raise NotImplementedError

    def get_recent_activity(
        self,
        days: int = 30,
        include_commits: bool = True,
        include_prs: bool = True,
        include_issues: bool = True,
    ) -> dict[str, int]:
        raise NotImplementedError

    def iter_files(
        self,
        path: str = "",
        ref: str | None = None,
        pattern: str | None = None,
    ) -> Iterator[str]:
        raise NotImplementedError

    def get_contributors(
        self,
        sort_by: Literal["commits", "name", "date"] = "commits",
        limit: int | None = None,
    ) -> list[User]:
        raise NotImplementedError

    def get_latest_release(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:
        raise NotImplementedError

    def get_languages(self) -> dict[str, int]:
        raise NotImplementedError

    def compare_branches(
        self,
        base: str,
        head: str,
        include_commits: bool = True,
        include_files: bool = True,
        include_stats: bool = True,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def download(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ) -> None:
        raise NotImplementedError

    def get_release(self, tag: str) -> Release:
        """Get specific release by tag."""
        raise NotImplementedError

    def list_releases(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        """List releases."""
        raise NotImplementedError
