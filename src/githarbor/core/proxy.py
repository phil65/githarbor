"""Module containing repository proxy implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from githarbor.core.base import BaseRepository
from githarbor.exceptions import ResourceNotFoundError


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


class Repository(BaseRepository):
    """Proxy class that forwards all method calls to a repository instance."""

    def __init__(self, repository: BaseRepository) -> None:
        """Initialize proxy with repository instance.

        Args:
            repository: Repository instance to forward calls to.
        """
        self._repository = repository

    @property
    def name(self) -> str:
        """Return repository name.

        Returns:
            Name of the repository.
        """
        return self._repository.name

    @property
    def default_branch(self) -> str:
        """Return default branch name.

        Returns:
            Name of the default branch.
        """
        return self._repository.default_branch

    def get_branch(self, name: str) -> Branch:
        """Get information about a specific branch.

        Args:
            name: Name of the branch.

        Returns:
            Branch information.
        """
        return self._repository.get_branch(name)

    def get_pull_request(self, number: int) -> PullRequest:
        """Get information about a specific pull request.

        Args:
            number: PR number.

        Returns:
            Pull request information.
        """
        return self._repository.get_pull_request(number)

    def list_pull_requests(self, state: str = "open") -> list[PullRequest]:
        """List pull requests.

        Args:
            state: State filter ('open', 'closed', 'all').

        Returns:
            List of pull requests.
        """
        return self._repository.list_pull_requests(state)

    def get_issue(self, issue_id: int) -> Issue:
        """Get information about a specific issue.

        Args:
            issue_id: Issue number.

        Returns:
            Issue information.
        """
        return self._repository.get_issue(issue_id)

    def list_issues(self, state: str = "open") -> list[Issue]:
        """List issues.

        Args:
            state: State filter ('open', 'closed', 'all').

        Returns:
            List of issues.
        """
        return self._repository.list_issues(state)

    def get_commit(self, sha: str) -> Commit:
        """Get information about a specific commit.

        Args:
            sha: Commit SHA.

        Returns:
            Commit information.
        """
        return self._repository.get_commit(sha)

    def list_commits(
        self,
        branch: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        author: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
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
        return self._repository.list_commits(
            branch=branch,
            since=since,
            until=until,
            author=author,
            path=path,
            max_results=max_results,
        )

    def get_workflow(self, workflow_id: str) -> Workflow:
        """Get information about a specific workflow.

        Args:
            workflow_id: Workflow ID.

        Returns:
            Workflow information.
        """
        return self._repository.get_workflow(workflow_id)

    def list_workflows(self) -> list[Workflow]:
        """List all workflows.

        Returns:
            List of workflows.
        """
        return self._repository.list_workflows()

    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        """Get information about a specific workflow run.

        Args:
            run_id: Workflow run ID.

        Returns:
            Workflow run information.
        """
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
        self._repository.download(path, destination, recursive)

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
        try:
            return self._repository.search_commits(query, branch, path, max_results)
        except NotImplementedError:
            # Get all commits and filter manually
            commits = self.list_commits(branch=branch, path=path)
            matches = [
                commit for commit in commits if query.lower() in commit.message.lower()
            ]
            if max_results:
                matches = matches[:max_results]
            return matches

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
        return self._repository.get_contributors(sort_by, limit)

    def get_languages(self) -> dict[str, int]:
        """Get repository language statistics.

        Returns:
            Dictionary mapping language names to byte counts.
        """
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
                        all_changes.update(commit.changed_files)
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
        return self._repository.list_releases(include_drafts, include_prereleases, limit)

    def get_release(self, tag: str) -> Release:
        """Get release by tag.

        Args:
            tag: Release tag name.

        Returns:
            Release information.
        """
        return self._repository.get_release(tag)

    def get_tag(self, name: str) -> Tag:
        """Get tag information.

        Args:
            name: Tag name.

        Returns:
            Tag information.
        """
        return self._repository.get_tag(name)

    def list_tags(self) -> list[Tag]:
        """List all tags.

        Returns:
            List of tags.
        """
        return self._repository.list_tags()

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
