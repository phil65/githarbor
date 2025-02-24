"""Base repository class. All methods raise FeatureNotSupportedError by default."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

from githarbor.exceptions import FeatureNotSupportedError


if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime
    import os

    from githarbor.core.models import (
        Branch,
        Comment,
        Commit,
        Issue,
        PullRequest,
        Release,
        Tag,
        User,
        Workflow,
        WorkflowRun,
    )

IssueState = Literal["open", "closed", "all"]
PullRequestState = Literal["open", "closed", "all"]


class BaseRepository:
    """Base repository class. All methods raise FeatureNotSupportedError by default."""

    is_async: ClassVar[bool] = False
    url_patterns: ClassVar[list[str]] = []
    _owner: str = ""
    _name: str = ""

    @property
    def name(self) -> str:
        """The name of the repository."""
        return self._name

    @property
    def owner(self) -> str:
        """The owner of the repository."""
        return self._owner

    @property
    def default_branch(self) -> str:
        """The default branch of this repository."""
        raise NotImplementedError

    @property
    def edit_base_uri(self) -> str | None:
        """The edit uri prefix of a repository."""
        return None

    @classmethod
    def supports_url(cls, url: str) -> bool:
        return any(pattern in url for pattern in cls.url_patterns)

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> BaseRepository:
        msg = f"{cls.__name__} does not implement from_url"
        raise FeatureNotSupportedError(msg)

    def get_repo_user(self) -> User:
        msg = f"{self.__class__.__name__} does not implement get_repo_user"
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

    def list_pull_requests(self, state: PullRequestState = "open") -> list[PullRequest]:
        msg = f"{self.__class__.__name__} does not implement list_pull_requests"
        raise FeatureNotSupportedError(msg)

    def get_issue(self, issue_id: int) -> Issue:
        msg = f"{self.__class__.__name__} does not implement get_issue"
        raise FeatureNotSupportedError(msg)

    def list_issues(self, state: IssueState = "open") -> list[Issue]:
        msg = f"{self.__class__.__name__} does not implement list_issues"
        raise FeatureNotSupportedError(msg)

    def create_issue(
        self,
        title: str,
        body: str,
        labels: list[str] | None = None,
        assignees: list[str] | None = None,
    ) -> Issue:
        """Create a new issue."""
        msg = f"{self.__class__.__name__} does not implement create_issue"
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

    def create_pull_request(
        self,
        title: str,
        body: str,
        head_branch: str,
        base_branch: str,
        draft: bool = False,
    ) -> PullRequest:
        msg = f"{self.__class__.__name__} does not implement create_pull_request"
        raise FeatureNotSupportedError(msg)

    def create_branch(
        self,
        name: str,
        base_commit: str,
    ) -> Branch:
        """Create a new branch at the specified commit."""
        msg = f"{self.__class__.__name__} does not implement create_branch"
        raise FeatureNotSupportedError(msg)

    def create_pull_request_from_diff(
        self,
        title: str,
        body: str,
        base_branch: str,
        diff: str,
        head_branch: str | None = None,
        draft: bool = False,
    ) -> PullRequest:
        """Create a pull request from a diff string."""
        msg = (
            f"{self.__class__.__name__} does not implement create_pull_request_from_diff"
        )
        raise FeatureNotSupportedError(msg)

    def add_pull_request_comment(
        self,
        number: int,
        body: str,
    ) -> Comment:
        msg = f"{self.__class__.__name__} does not implement add_pull_request_comment"
        raise FeatureNotSupportedError(msg)

    def add_pull_request_review_comment(
        self,
        number: int,
        body: str,
        commit_id: str,
        path: str,
        position: int,
    ) -> Comment:
        fn_name = "add_pull_request_review_comment"
        msg = f"{self.__class__.__name__} does not implement {fn_name}"
        raise FeatureNotSupportedError(msg)

    async def get_repo_user_async(self) -> User:
        """Get repository owner information asynchronously."""
        msg = f"{self.__class__.__name__} does not implement get_repo_user_async"
        raise FeatureNotSupportedError(msg)

    async def get_branch_async(self, name: str) -> Branch:
        """Get branch information asynchronously."""
        msg = f"{self.__class__.__name__} does not implement get_branch_async"
        raise FeatureNotSupportedError(msg)

    async def get_pull_request_async(self, number: int) -> PullRequest:
        """Get pull request information asynchronously."""
        msg = f"{self.__class__.__name__} does not implement get_pull_request_async"
        raise FeatureNotSupportedError(msg)

    async def list_pull_requests_async(
        self,
        state: PullRequestState = "open",
    ) -> list[PullRequest]:
        """List pull requests asynchronously."""
        msg = f"{self.__class__.__name__} does not implement list_pull_requests_async"
        raise FeatureNotSupportedError(msg)

    async def get_issue_async(self, issue_id: int) -> Issue:
        """Get issue information asynchronously."""
        msg = f"{self.__class__.__name__} does not implement get_issue_async"
        raise FeatureNotSupportedError(msg)

    async def list_issues_async(self, state: IssueState = "open") -> list[Issue]:
        """List issues asynchronously."""
        msg = f"{self.__class__.__name__} does not implement list_issues_async"
        raise FeatureNotSupportedError(msg)

    async def create_issue_async(
        self,
        title: str,
        body: str,
        labels: list[str] | None = None,
        assignees: list[str] | None = None,
    ) -> Issue:
        """Create a new issue asynchronously."""
        msg = f"{self.__class__.__name__} does not implement create_issue_async"
        raise FeatureNotSupportedError(msg)

    async def get_commit_async(self, sha: str) -> Commit:
        """Get commit information asynchronously."""
        msg = f"{self.__class__.__name__} does not implement get_commit_async"
        raise FeatureNotSupportedError(msg)

    async def list_commits_async(
        self,
        branch: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        author: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        """List commits asynchronously."""
        msg = f"{self.__class__.__name__} does not implement list_commits_async"
        raise FeatureNotSupportedError(msg)

    async def get_workflow_async(self, workflow_id: str) -> Workflow:
        """Get workflow information asynchronously."""
        msg = f"{self.__class__.__name__} does not implement get_workflow_async"
        raise FeatureNotSupportedError(msg)

    async def list_workflows_async(self) -> list[Workflow]:
        """List workflows asynchronously."""
        msg = f"{self.__class__.__name__} does not implement list_workflows_async"
        raise FeatureNotSupportedError(msg)

    async def get_workflow_run_async(self, run_id: str) -> WorkflowRun:
        """Get workflow run information asynchronously."""
        msg = f"{self.__class__.__name__} does not implement get_workflow_run_async"
        raise FeatureNotSupportedError(msg)

    async def download_async(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ) -> None:
        """Download repository content asynchronously."""
        msg = f"{self.__class__.__name__} does not implement download_async"
        raise FeatureNotSupportedError(msg)

    async def search_commits_async(
        self,
        query: str,
        branch: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        """Search commits asynchronously."""
        msg = f"{self.__class__.__name__} does not implement search_commits_async"
        raise FeatureNotSupportedError(msg)

    async def get_contributors_async(
        self,
        sort_by: Literal["commits", "name", "date"] = "commits",
        limit: int | None = None,
    ) -> list[User]:
        """Get repository contributors asynchronously."""
        msg = f"{self.__class__.__name__} does not implement get_contributors_async"
        raise FeatureNotSupportedError(msg)

    async def get_languages_async(self) -> dict[str, int]:
        """Get repository language statistics asynchronously."""
        msg = f"{self.__class__.__name__} does not implement get_languages_async"
        raise FeatureNotSupportedError(msg)

    async def compare_branches_async(
        self,
        base: str,
        head: str,
        include_commits: bool = True,
        include_files: bool = True,
        include_stats: bool = True,
    ) -> dict[str, Any]:
        """Compare two branches asynchronously."""
        msg = f"{self.__class__.__name__} does not implement compare_branches_async"
        raise FeatureNotSupportedError(msg)

    async def get_latest_release_async(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:
        """Get latest release asynchronously."""
        msg = f"{self.__class__.__name__} does not implement get_latest_release_async"
        raise FeatureNotSupportedError(msg)

    async def list_releases_async(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        """List releases asynchronously."""
        msg = f"{self.__class__.__name__} does not implement list_releases_async"
        raise FeatureNotSupportedError(msg)

    async def get_release_async(self, tag: str) -> Release:
        """Get release by tag asynchronously."""
        msg = f"{self.__class__.__name__} does not implement get_release_async"
        raise FeatureNotSupportedError(msg)

    async def get_tag_async(self, name: str) -> Tag:
        """Get tag information asynchronously."""
        msg = f"{self.__class__.__name__} does not implement get_tag_async"
        raise FeatureNotSupportedError(msg)

    async def list_tags_async(self) -> list[Tag]:
        """List all tags asynchronously."""
        msg = f"{self.__class__.__name__} does not implement list_tags_async"
        raise FeatureNotSupportedError(msg)

    async def create_pull_request_async(
        self,
        title: str,
        body: str,
        head_branch: str,
        base_branch: str,
        draft: bool = False,
    ) -> PullRequest:
        msg = f"{self.__class__.__name__} does not implement create_pull_request_async"
        raise FeatureNotSupportedError(msg)

    async def create_branch_async(
        self,
        name: str,
        base_commit: str,
    ) -> Branch:
        """Create a new branch at the specified commit asynchronously."""
        msg = f"{self.__class__.__name__} does not implement create_branch_async"
        raise FeatureNotSupportedError(msg)

    async def add_pull_request_comment_async(
        self,
        number: int,
        body: str,
    ) -> Comment:
        msg = (
            f"{self.__class__.__name__} does not implement add_pull_request_comment_async"
        )
        raise FeatureNotSupportedError(msg)

    async def add_pull_request_review_comment_async(
        self,
        number: int,
        body: str,
        commit_id: str,
        path: str,
        position: int,
    ) -> Comment:
        fn_name = "add_pull_request_review_comment_async"
        msg = f"{self.__class__.__name__} does not implement {fn_name}"
        raise FeatureNotSupportedError(msg)

    async def create_pull_request_from_diff_async(
        self,
        title: str,
        body: str,
        base_branch: str,
        diff: str,
        head_branch: str | None = None,
        draft: bool = False,
    ) -> PullRequest:
        """Create a pull request from a diff string asynchronously."""
        fn_name = "create_pull_request_from_diff_async"
        msg = f"{self.__class__.__name__} does not implement {fn_name}"
        raise FeatureNotSupportedError(msg)


class BaseOwner:
    """Base class for repository owners."""

    is_async: ClassVar[bool] = False
    url_patterns: ClassVar[list[str]] = []
    _name: str = ""

    def list_repositories(self) -> list[BaseRepository]:
        msg = f"{self.__class__.__name__} does not implement list_repositories"
        raise FeatureNotSupportedError(msg)

    @property
    def name(self) -> str:
        """The name of the repository."""
        return self._name

    def create_repository(
        self,
        name: str,
        description: str = "",
        private: bool = False,
    ) -> BaseRepository:
        msg = f"{self.__class__.__name__} does not implement create_repository"
        raise FeatureNotSupportedError(msg)

    def get_user(self) -> User:
        msg = f"{self.__class__.__name__} does not implement get_user"
        raise FeatureNotSupportedError(msg)

    def delete_repository(self, name: str) -> None:
        msg = f"{self.__class__.__name__} does not implement delete_repository"
        raise FeatureNotSupportedError(msg)

    async def list_repositories_async(self) -> list[BaseRepository]:
        """List repositories asynchronously."""
        msg = f"{self.__class__.__name__} does not implement list_repositories_async"
        raise FeatureNotSupportedError(msg)

    async def create_repository_async(
        self,
        name: str,
        description: str = "",
        private: bool = False,
    ) -> BaseRepository:
        """Create repository asynchronously."""
        msg = f"{self.__class__.__name__} does not implement create_repository_async"
        raise FeatureNotSupportedError(msg)

    async def get_user_async(self) -> User:
        """Get user information asynchronously."""
        msg = f"{self.__class__.__name__} does not implement get_user_async"
        raise FeatureNotSupportedError(msg)

    async def delete_repository_async(self, name: str) -> None:
        """Delete repository asynchronously."""
        msg = f"{self.__class__.__name__} does not implement delete_repository_async"
        raise FeatureNotSupportedError(msg)

    @classmethod
    def supports_url(cls, url: str) -> bool:
        return any(pattern in url for pattern in cls.url_patterns)

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> BaseOwner:
        msg = f"{cls.__name__} does not implement from_url"
        raise FeatureNotSupportedError(msg)
