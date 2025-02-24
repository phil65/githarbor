"""Module containing repository proxy implementation."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Literal

from githarbor.core.base import BaseRepository, IssueState, PullRequestState
from githarbor.core.datatypes import NiceReprList
from githarbor.exceptions import ResourceNotFoundError


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
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


class Repository(BaseRepository):
    """Proxy class that forwards all method calls to a repository instance."""

    def __init__(self, repository: BaseRepository) -> None:
        """Initialize proxy with repository instance.

        Args:
            repository: Repository instance to forward calls to.
        """
        self._repository = repository
        self.repository_type = type(repository).__name__.removesuffix("Repository")

    def __repr__(self):
        return f"<{self.repository_type} {self.owner}/{self.name}>"

    @property
    def owner(self):
        return self._repository.owner

    @property
    def name(self) -> str:
        """Return repository name.

        Returns:
            Name of the repository.
        """
        return self._repository.name

    @property
    def edit_base_uri(self):
        return self._repository.edit_base_uri

    @property
    def repository(self):
        return self._repository

    @property
    def default_branch(self) -> str:
        """Return default branch name.

        Returns:
            Name of the default branch.
        """
        return self._repository.default_branch

    def get_repo_user(self) -> User:
        """Get information about the repository user.

        Returns:
            User information.
        """
        if self._repository.is_async:
            return asyncio.run(self._repository.get_repo_user_async())
        return self._repository.get_repo_user()

    def get_branch(self, name: str) -> Branch:
        """Get information about a specific branch.

        Args:
            name: Name of the branch.

        Returns:
            Branch information.
        """
        if self._repository.is_async:
            return asyncio.run(self._repository.get_branch_async(name))
        return self._repository.get_branch(name)

    def get_pull_request(self, number: int) -> PullRequest:
        """Get information about a specific pull request.

        Args:
            number: PR number.

        Returns:
            Pull request information.
        """
        if self._repository.is_async:
            return asyncio.run(self._repository.get_pull_request_async(number))
        return self._repository.get_pull_request(number)

    def list_pull_requests(self, state: PullRequestState = "open") -> list[PullRequest]:
        """List pull requests.

        Args:
            state: State filter ('open', 'closed', 'all').

        Returns:
            List of pull requests.
        """
        if self._repository.is_async:
            return asyncio.run(self._repository.list_pull_requests_async(state))
        return self._repository.list_pull_requests(state)

    def get_issue(self, issue_id: int) -> Issue:
        """Get information about a specific issue.

        Args:
            issue_id: Issue number.

        Returns:
            Issue information.
        """
        if self._repository.is_async:
            return asyncio.run(self._repository.get_issue_async(issue_id))
        return self._repository.get_issue(issue_id)

    def list_issues(self, state: IssueState = "open") -> list[Issue]:
        """List issues.

        Args:
            state: State filter ('open', 'closed', 'all').

        Returns:
            List of issues.
        """
        if self._repository.is_async:
            return asyncio.run(self._repository.list_issues_async(state))
        return self._repository.list_issues(state)

    def create_issue(
        self,
        title: str,
        body: str,
        labels: list[str] | None = None,
        assignees: list[str] | None = None,
    ) -> Issue:
        """Create an issue.

        Args:
            title: Issue title
            body: Issue description/content
            labels: List of label names to apply
            assignees: List of usernames to assign

        Returns:
            Created issue
        """
        if self._repository.is_async:
            return asyncio.run(
                self._repository.create_issue_async(
                    title=title,
                    body=body,
                    labels=labels,
                    assignees=assignees,
                )
            )
        return self._repository.create_issue(
            title=title,
            body=body,
            labels=labels,
            assignees=assignees,
        )

    def get_commit(self, sha: str) -> Commit:
        """Get information about a specific commit.

        Args:
            sha: Commit SHA.

        Returns:
            Commit information.
        """
        if self._repository.is_async:
            return asyncio.run(self._repository.get_commit_async(sha))
        return self._repository.get_commit(sha)

    def list_commits(
        self,
        branch: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        author: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> NiceReprList[Commit]:
        """List commits with optional filters.

        Args:
            branch: Branch to list commits from.
            since: Only show commits after this date.
            until: Only show commits before this date.
            author: Filter by author.
            path: Filter by file path.
            max_results: Maximum number of results to return.

        Returns:
            List of commits.
        """
        if self._repository.is_async:
            return NiceReprList(
                asyncio.run(
                    self._repository.list_commits_async(
                        branch=branch,
                        since=since,
                        until=until,
                        author=author,
                        path=path,
                        max_results=max_results,
                    )
                )
            )
        return NiceReprList(
            self._repository.list_commits(
                branch=branch,
                since=since,
                until=until,
                author=author,
                path=path,
                max_results=max_results,
            )
        )

    def get_workflow(self, workflow_id: str) -> Workflow:
        """Get information about a specific workflow.

        Args:
            workflow_id: Workflow ID.

        Returns:
            Workflow information.
        """
        if self._repository.is_async:
            return asyncio.run(self._repository.get_workflow_async(workflow_id))
        return self._repository.get_workflow(workflow_id)

    def list_workflows(self) -> list[Workflow]:
        """List all workflows.

        Returns:
            List of workflows.
        """
        if self._repository.is_async:
            return asyncio.run(self._repository.list_workflows_async())
        return self._repository.list_workflows()

    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        """Get information about a specific workflow run.

        Args:
            run_id: Workflow run ID.

        Returns:
            Workflow run information.
        """
        if self._repository.is_async:
            return asyncio.run(self._repository.get_workflow_run_async(run_id))
        return self._repository.get_workflow_run(run_id)

    def download(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ) -> None:
        """Download repository content.

        Args:
            path: Path to download.
            destination: Where to save the downloaded content.
            recursive: Whether to download recursively.
        """
        if self._repository.is_async:
            return asyncio.run(
                self._repository.download_async(path, destination, recursive)
            )
        return self._repository.download(path, destination, recursive)

    def search_commits(
        self,
        query: str,
        branch: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        """Search commits.

        Default implementation that filters commits based on message content.

        Args:
            query: Search query string
            branch: Branch to search in
            path: Filter by file path
            max_results: Maximum number of results to return

        Returns:
            List of matching commits
        """
        if self._repository.is_async:
            return NiceReprList(
                asyncio.run(
                    self._repository.search_commits_async(
                        query=query,
                        branch=branch,
                        path=path,
                        max_results=max_results,
                    )
                )
            )
        return NiceReprList(
            self._repository.search_commits(
                query=query,
                branch=branch,
                path=path,
                max_results=max_results,
            )
        )

    def iter_files(
        self,
        path: str = "",
        ref: str | None = None,
        pattern: str | None = None,
    ) -> Iterator[str]:
        """Iterate over repository files.

        Default implementation using recursive directory traversal.

        Args:
            path: Base path to start from
            ref: Git reference (branch/tag/commit)
            pattern: File pattern to match

        Yields:
            File paths
        """
        yield from self._repository.iter_files(path, ref, pattern)
        # try:
        #     yield from self._repository.iter_files(path, ref, pattern)
        # except NotImplementedError:

        #     def _should_include(file_path: str) -> bool:
        #         return not pattern or fnmatch.fnmatch(file_path, pattern)

        #     # This assumes repository provides some basic file listing capability
        #     try:
        #         contents = self._repository.list_directory(path, ref=ref)
        #         for item in contents:
        #             if isinstance(item, dict):
        #                 # Assuming item has 'path' and 'type' keys
        #                 if item["type"] == "dir":
        #                     yield from self.iter_files(
        #                         item["path"], ref=ref, pattern=pattern
        #                     )
        #                 elif _should_include(item["path"]):
        #                     yield item["path"]
        #             # Assuming item is a path string
        #             elif _should_include(str(item)):
        #                 yield str(item)
        #     except (NotImplementedError, AttributeError):
        #         # If even basic file listing is not available, raise NotImplementedError
        #         msg = "Repository does not support file iteration"
        #         raise NotImplementedError(msg)

    def get_contributors(
        self,
        sort_by: Literal["commits", "name", "date"] = "commits",
        limit: int | None = None,
    ) -> list[User]:
        """Get repository contributors.

        Args:
            sort_by: How to sort contributors.
            limit: Maximum number of contributors to return.

        Returns:
            List of contributors.
        """
        if self._repository.is_async:
            return asyncio.run(self._repository.get_contributors_async(sort_by, limit))
        return self._repository.get_contributors(sort_by, limit)

    def get_languages(self) -> dict[str, int]:
        """Get repository language statistics.

        Returns:
            Dictionary mapping language names to byte counts.
        """
        if self._repository.is_async:
            return asyncio.run(self._repository.get_languages_async())
        return self._repository.get_languages()

    def compare_branches(
        self,
        base: str,
        head: str,
        include_commits: bool = True,
        include_files: bool = True,
        include_stats: bool = True,
    ) -> dict[str, Any]:
        """Compare two branches.

        Default implementation using commit history comparison.

        Args:
            base: Base branch name
            head: Head branch name
            include_commits: Whether to include commit information
            include_files: Whether to include changed files
            include_stats: Whether to include statistics

        Returns:
            Dictionary containing comparison information
        """
        try:
            if self._repository.is_async:
                return asyncio.run(
                    self._repository.compare_branches_async(
                        base, head, include_commits, include_files, include_stats
                    )
                )
            return self._repository.compare_branches(
                base, head, include_commits, include_files, include_stats
            )
        except NotImplementedError:
            # Get commits from both branches
            base_commits = {c.sha for c in self.list_commits(branch=base)}
            head_commits = self.list_commits(branch=head)

            # Find commits in head that aren't in base
            unique_commits = [c for c in head_commits if c.sha not in base_commits]

            result: dict[str, Any] = {"ahead_by": len(unique_commits)}

            if include_commits:
                result["commits"] = unique_commits

            if include_files or include_stats:
                # Get changed files by comparing each commit
                all_changes: set[str] = set()
                total_additions = 0
                total_deletions = 0

                for commit in unique_commits:
                    # This assumes repository provides file change info in commits
                    if hasattr(commit, "changed_files"):
                        all_changes.update(commit.changed_files)  # pyright: ignore
                    if hasattr(commit, "stats"):
                        total_additions += commit.stats.get("additions", 0)
                        total_deletions += commit.stats.get("deletions", 0)

                if include_files:
                    result["files"] = sorted(all_changes)

                if include_stats:
                    result["stats"] = {
                        "additions": total_additions,
                        "deletions": total_deletions,
                        "changes": len(all_changes),
                    }

            return result

    def get_latest_release(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:
        """Get latest release.

        Default implementation using list_releases.

        Args:
            include_drafts: Whether to include draft releases
            include_prereleases: Whether to include pre-releases

        Returns:
            Latest release information

        Raises:
            ResourceNotFoundError: If no matching releases are found
        """
        try:
            if self._repository.is_async:
                return asyncio.run(
                    self._repository.get_latest_release_async(
                        include_drafts, include_prereleases
                    )
                )
            return self._repository.get_latest_release(
                include_drafts, include_prereleases
            )
        except NotImplementedError as e:
            releases = self.list_releases(
                include_drafts=include_drafts,
                include_prereleases=include_prereleases,
                limit=1,
            )
            if not releases:
                msg = "No matching releases found"
                raise ResourceNotFoundError(msg) from e
            return releases[0]

    def list_releases(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        """List releases.

        Args:
            include_drafts: Whether to include draft releases.
            include_prereleases: Whether to include pre-releases.
            limit: Maximum number of releases to return.

        Returns:
            List of releases.
        """
        if self._repository.is_async:
            return asyncio.run(
                self._repository.list_releases_async(
                    include_drafts, include_prereleases, limit
                )
            )
        return self._repository.list_releases(include_drafts, include_prereleases, limit)

    def get_release(self, tag: str) -> Release:
        """Get release by tag.

        Args:
            tag: Release tag name.

        Returns:
            Release information.
        """
        if self._repository.is_async:
            return asyncio.run(self._repository.get_release_async(tag))
        return self._repository.get_release(tag)

    def get_tag(self, name: str) -> Tag:
        """Get tag information.

        Args:
            name: Tag name.

        Returns:
            Tag information.
        """
        if self._repository.is_async:
            return asyncio.run(self._repository.get_tag_async(name))
        return self._repository.get_tag(name)

    def create_pull_request(
        self,
        title: str,
        body: str,
        head_branch: str,
        base_branch: str,
        draft: bool = False,
    ) -> PullRequest:
        """Create a new pull request.

        Args:
            title: Pull request title
            body: Pull request description
            head_branch: Source branch containing the changes
            base_branch: Target branch for the changes
            draft: Whether to create a draft pull request

        Returns:
            Newly created pull request
        """
        if self._repository.is_async:
            return asyncio.run(
                self._repository.create_pull_request_async(
                    title=title,
                    body=body,
                    head_branch=head_branch,
                    base_branch=base_branch,
                    draft=draft,
                )
            )
        return self._repository.create_pull_request(
            title=title,
            body=body,
            head_branch=head_branch,
            base_branch=base_branch,
            draft=draft,
        )

    def list_tags(self) -> list[Tag]:
        """List all tags.

        Returns:
            List of tags.
        """
        if self._repository.is_async:
            return asyncio.run(self._repository.list_tags_async())
        return self._repository.list_tags()

    def create_branch(
        self,
        name: str,
        base_commit: str,
    ) -> Branch:
        """Create a new branch at the specified commit.

        Args:
            name: Name of the branch to create
            base_commit: SHA of the commit to base the branch on

        Returns:
            Created branch
        """
        if self._repository.is_async:
            return asyncio.run(
                self._repository.create_branch_async(
                    name=name,
                    base_commit=base_commit,
                )
            )
        return self._repository.create_branch(
            name=name,
            base_commit=base_commit,
        )

    def add_pull_request_comment(
        self,
        number: int,
        body: str,
    ) -> Comment:
        """Add a general comment to a pull request.

        Args:
            number: Pull request number
            body: Comment text

        Returns:
            Created comment
        """
        if self._repository.is_async:
            return asyncio.run(
                self._repository.add_pull_request_comment_async(number, body)
            )
        return self._repository.add_pull_request_comment(number, body)

    def add_pull_request_review_comment(
        self,
        number: int,
        body: str,
        commit_id: str,
        path: str,
        position: int,
    ) -> Comment:
        """Add a review comment to specific line in a pull request.

        Args:
            number: Pull request number
            body: Comment text
            commit_id: The SHA of the commit to comment on
            path: The relative path to the file to comment on
            position: Line number in the file to comment on

        Returns:
            Created comment
        """
        if self._repository.is_async:
            return asyncio.run(
                self._repository.add_pull_request_review_comment_async(
                    number, body, commit_id, path, position
                )
            )
        return self._repository.add_pull_request_review_comment(
            number, body, commit_id, path, position
        )

    def create_pull_request_from_diff(
        self,
        title: str,
        body: str,
        base_branch: str,
        diff: str,
        head_branch: str | None = None,
        draft: bool = False,
    ) -> PullRequest:
        """Create a pull request from a diff string.

        Args:
            title: Pull request title
            body: Pull request description
            base_branch: Target branch for the changes
            diff: Git diff string
            head_branch: Name of the branch to create. Auto-generated if not provided.
            draft: Whether to create a draft pull request

        Returns:
            Created pull request
        """
        if self._repository.is_async:
            return asyncio.run(
                self._repository.create_pull_request_from_diff_async(
                    title=title,
                    body=body,
                    base_branch=base_branch,
                    diff=diff,
                    head_branch=head_branch,
                    draft=draft,
                )
            )
        return self._repository.create_pull_request_from_diff(
            title=title,
            body=body,
            base_branch=base_branch,
            diff=diff,
            head_branch=head_branch,
            draft=draft,
        )

    def get_recent_activity(
        self,
        days: int = 30,
        include_commits: bool = True,
        include_prs: bool = True,
        include_issues: bool = True,
    ) -> dict[str, int]:
        """Get recent repository activity.

        This is a default implementation that composes results from other API calls.
        Repository implementations can override this if they have a more efficient way.

        Args:
            days: Number of days to look back.
            include_commits: Whether to include commit counts.
            include_prs: Whether to include PR counts.
            include_issues: Whether to include issue counts.

        Returns:
            Activity statistics with keys for 'commits', 'pull_requests', and 'issues'.
        """
        try:
            # First try the repository's native implementation
            return self._repository.get_recent_activity(  # type: ignore[attr-defined]
                days, include_commits, include_prs, include_issues
            )
        except (NotImplementedError, AttributeError):
            # Fall back to our composite implementation
            from datetime import UTC, datetime, timedelta

            since = datetime.now(UTC) - timedelta(days=days)
            activity: dict[str, int] = {}

            if include_commits:
                commits = self.list_commits(since=since)
                activity["commits"] = len(commits)

            if include_prs:
                # Get all PRs and filter by update date
                prs = self.list_pull_requests(state="all")
                activity["pull_requests"] = sum(
                    1 for pr in prs if pr.updated_at and pr.updated_at >= since
                )

            if include_issues:
                # Get all issues and filter by update date
                issues = self.list_issues(state="all")
                activity["issues"] = sum(
                    1
                    for issue in issues
                    if issue.updated_at
                    and issue.updated_at >= since
                    and not hasattr(issue, "pull_request")  # Exclude PRs
                )

            return activity

    async def get_repo_user_async(self) -> User:
        """See get_repo_user."""
        if self._repository.is_async:
            return await self._repository.get_repo_user_async()  # type: ignore
        return await asyncio.to_thread(self._repository.get_repo_user)

    async def get_branch_async(self, name: str) -> Branch:
        """See get_branch."""
        if self._repository.is_async:
            return await self._repository.get_branch_async(name)  # type: ignore
        return await asyncio.to_thread(self._repository.get_branch, name)

    async def get_pull_request_async(self, number: int) -> PullRequest:
        """See get_pull_request."""
        if self._repository.is_async:
            return await self._repository.get_pull_request_async(number)  # type: ignore
        return await asyncio.to_thread(self._repository.get_pull_request, number)

    async def list_pull_requests_async(
        self,
        state: PullRequestState = "open",
    ) -> list[PullRequest]:
        """See list_pull_requests."""
        if self._repository.is_async:
            return await self._repository.list_pull_requests_async(state)  # type: ignore
        return await asyncio.to_thread(self._repository.list_pull_requests, state)

    async def get_issue_async(self, issue_id: int) -> Issue:
        """See get_issue."""
        if self._repository.is_async:
            return await self._repository.get_issue_async(issue_id)  # type: ignore
        return await asyncio.to_thread(self._repository.get_issue, issue_id)

    async def list_issues_async(self, state: IssueState = "open") -> list[Issue]:
        """See list_issues."""
        if self._repository.is_async:
            return await self._repository.list_issues_async(state)  # type: ignore
        return await asyncio.to_thread(self._repository.list_issues, state)

    async def create_issue_async(
        self,
        title: str,
        body: str,
        labels: list[str] | None = None,
        assignees: list[str] | None = None,
    ) -> Issue:
        """See create_issue."""
        if self._repository.is_async:
            return await self._repository.create_issue_async(
                title=title,
                body=body,
                labels=labels,
                assignees=assignees,
            )
        return await asyncio.to_thread(
            self._repository.create_issue,
            title=title,
            body=body,
            labels=labels,
            assignees=assignees,
        )

    async def get_commit_async(self, sha: str) -> Commit:
        """See get_commit."""
        if self._repository.is_async:
            return await self._repository.get_commit_async(sha)  # type: ignore
        return await asyncio.to_thread(self._repository.get_commit, sha)

    async def list_commits_async(
        self,
        branch: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        author: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        """See list_commits."""
        if self._repository.is_async:
            return await self._repository.list_commits_async(  # type: ignore
                branch, since, until, author, path, max_results
            )
        return await asyncio.to_thread(
            self._repository.list_commits,
            branch,
            since,
            until,
            author,
            path,
            max_results,
        )

    async def get_workflow_async(self, workflow_id: str) -> Workflow:
        """See get_workflow."""
        if self._repository.is_async:
            return await self._repository.get_workflow_async(workflow_id)  # type: ignore
        return await asyncio.to_thread(self._repository.get_workflow, workflow_id)

    async def list_workflows_async(self) -> list[Workflow]:
        """See list_workflows."""
        if self._repository.is_async:
            return await self._repository.list_workflows_async()  # type: ignore
        return await asyncio.to_thread(self._repository.list_workflows)

    async def get_workflow_run_async(self, run_id: str) -> WorkflowRun:
        """See get_workflow_run."""
        if self._repository.is_async:
            return await self._repository.get_workflow_run_async(run_id)  # type: ignore
        return await asyncio.to_thread(self._repository.get_workflow_run, run_id)

    async def download_async(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ) -> None:
        """See download."""
        if self._repository.is_async:
            await self._repository.download(path, destination, recursive)  # type: ignore
        await asyncio.to_thread(self._repository.download, path, destination, recursive)

    async def search_commits_async(
        self,
        query: str,
        branch: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        """See search_commits."""
        if self._repository.is_async:
            return await self._repository.search_commits_async(  # type: ignore
                query, branch, path, max_results
            )
        return await asyncio.to_thread(
            self._repository.search_commits, query, branch, path, max_results
        )

    async def get_contributors_async(
        self,
        sort_by: Literal["commits", "name", "date"] = "commits",
        limit: int | None = None,
    ) -> list[User]:
        """See get_contributors."""
        if self._repository.is_async:
            return await self._repository.get_contributors_async(sort_by, limit)  # type: ignore
        return await asyncio.to_thread(self._repository.get_contributors, sort_by, limit)

    async def get_languages_async(self) -> dict[str, int]:
        """See get_languages."""
        if self._repository.is_async:
            return await self._repository.get_languages_async()  # type: ignore
        return await asyncio.to_thread(self._repository.get_languages)

    async def compare_branches_async(
        self,
        base: str,
        head: str,
        include_commits: bool = True,
        include_files: bool = True,
        include_stats: bool = True,
    ) -> dict[str, Any]:
        """See compare_branches."""
        if self._repository.is_async:
            return await self._repository.compare_branches_async(  # type: ignore
                base, head, include_commits, include_files, include_stats
            )
        return await asyncio.to_thread(
            self._repository.compare_branches,
            base,
            head,
            include_commits,
            include_files,
            include_stats,
        )

    async def get_latest_release_async(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:
        """See get_latest_release."""
        if self._repository.is_async:
            return await self._repository.get_latest_release_async(  # type: ignore
                include_drafts, include_prereleases
            )
        return await asyncio.to_thread(
            self._repository.get_latest_release, include_drafts, include_prereleases
        )

    async def list_releases_async(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        """See list_releases."""
        if self._repository.is_async:
            return await self._repository.list_releases_async(  # type: ignore
                include_drafts, include_prereleases, limit
            )
        return await asyncio.to_thread(
            self._repository.list_releases, include_drafts, include_prereleases, limit
        )

    async def get_release_async(self, tag: str) -> Release:
        """See get_release."""
        if self._repository.is_async:
            return await self._repository.get_release_async(tag)  # type: ignore
        return await asyncio.to_thread(self._repository.get_release, tag)

    async def get_tag_async(self, name: str) -> Tag:
        """See get_tag."""
        if self._repository.is_async:
            return await self._repository.get_tag_async(name)  # type: ignore
        return await asyncio.to_thread(self._repository.get_tag, name)

    async def create_pull_request_async(
        self,
        title: str,
        body: str,
        head_branch: str,
        base_branch: str,
        draft: bool = False,
    ) -> PullRequest:
        """See create_pull_request."""
        if self._repository.is_async:
            return await self._repository.create_pull_request_async(
                title=title,
                body=body,
                head_branch=head_branch,
                base_branch=base_branch,
                draft=draft,
            )
        return await asyncio.to_thread(
            self._repository.create_pull_request,
            title=title,
            body=body,
            head_branch=head_branch,
            base_branch=base_branch,
            draft=draft,
        )

    async def create_branch_async(
        self,
        name: str,
        base_commit: str,
    ) -> Branch:
        """Create a new branch at the specified commit asynchronously.

        Args:
            name: Name of the branch to create
            base_commit: SHA of the commit to base the branch on

        Returns:
            Created branch
        """
        if self._repository.is_async:
            return await self._repository.create_branch_async(
                name=name,
                base_commit=base_commit,
            )
        return await asyncio.to_thread(
            self._repository.create_branch,
            name=name,
            base_commit=base_commit,
        )

    async def list_tags_async(self) -> list[Tag]:
        """See list_tags."""
        if self._repository.is_async:
            return await self._repository.list_tags_async()  # type: ignore
        return await asyncio.to_thread(self._repository.list_tags)

    async def create_pull_request_from_diff_async(
        self,
        title: str,
        body: str,
        base_branch: str,
        diff: str,
        head_branch: str | None = None,
        draft: bool = False,
    ) -> PullRequest:
        """Create a pull request from a diff string asynchronously.

        Args:
            title: Pull request title
            body: Pull request description
            base_branch: Target branch for the changes
            diff: Git diff string
            head_branch: Name of the branch to create. Auto-generated if not provided.
            draft: Whether to create a draft pull request

        Returns:
            Created pull request
        """
        if self._repository.is_async:
            return await self._repository.create_pull_request_from_diff_async(
                title=title,
                body=body,
                base_branch=base_branch,
                diff=diff,
                head_branch=head_branch,
                draft=draft,
            )
        return await asyncio.to_thread(
            self._repository.create_pull_request_from_diff,
            title=title,
            body=body,
            base_branch=base_branch,
            diff=diff,
            head_branch=head_branch,
            draft=draft,
        )

    async def add_pull_request_comment_async(
        self,
        number: int,
        body: str,
    ) -> Comment:
        """See add_pull_request_comment."""
        if self._repository.is_async:
            return await self._repository.add_pull_request_comment_async(number, body)
        return await asyncio.to_thread(
            self._repository.add_pull_request_comment, number, body
        )

    async def add_pull_request_review_comment_async(
        self,
        number: int,
        body: str,
        commit_id: str,
        path: str,
        position: int,
    ) -> Comment:
        """See add_pull_request_review_comment."""
        if self._repository.is_async:
            return await self._repository.add_pull_request_review_comment_async(
                number, body, commit_id, path, position
            )
        return await asyncio.to_thread(
            self._repository.add_pull_request_review_comment,
            number,
            body,
            commit_id,
            path,
            position,
        )

    def get_sync_methods(self) -> list[Callable]:
        """Return list of all synchronous methods."""
        return [
            self.get_repo_user,
            self.create_branch,
            self.get_branch,
            self.create_pull_request,
            self.create_pull_request_from_diff,
            self.get_pull_request,
            self.list_pull_requests,
            self.get_issue,
            self.list_issues,
            self.create_issue,
            self.get_commit,
            self.list_commits,
            self.get_workflow,
            self.list_workflows,
            self.get_workflow_run,
            self.download,
            self.search_commits,
            self.get_contributors,
            self.add_pull_request_comment,
            self.add_pull_request_review_comment,
            self.get_languages,
            self.compare_branches,
            self.get_latest_release,
            self.list_releases,
            self.get_release,
            self.get_tag,
            self.list_tags,
        ]

    def get_async_methods(self) -> list[Callable]:
        """Return list of all asynchronous methods."""
        return [
            self.get_repo_user_async,
            self.create_branch_async,
            self.get_branch_async,
            self.create_pull_request_async,
            self.create_pull_request_from_diff_async,
            self.get_pull_request_async,
            self.list_pull_requests_async,
            self.get_issue_async,
            self.list_issues_async,
            self.get_commit_async,
            self.create_issue_async,
            self.list_commits_async,
            self.get_workflow_async,
            self.list_workflows_async,
            self.get_workflow_run_async,
            self.download_async,
            self.search_commits_async,
            self.get_contributors_async,
            self.add_pull_request_comment_async,
            self.add_pull_request_review_comment_async,
            self.get_languages_async,
            self.compare_branches_async,
            self.get_latest_release_async,
            self.list_releases_async,
            self.get_release_async,
            self.get_tag_async,
            self.list_tags_async,
        ]


for method_name, method in Repository.__dict__.items():
    if method_name.endswith("_async"):
        sync_name = method_name[:-6]  # Remove '_async' suffix
        if (sync_method := getattr(Repository, sync_name, None)) and sync_method.__doc__:
            method.__doc__ = sync_method.__doc__
