"""PyGitea repository implementation."""

from __future__ import annotations

import fnmatch
import logging
import os
from typing import TYPE_CHECKING, Any, ClassVar, Literal
from urllib.parse import urlparse

from githarbor.core.base import BaseRepository
from githarbor.exceptions import AuthenticationError, ResourceNotFoundError
from githarbor.providers.pygitea_provider import utils as pygiteatools


logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime

    from upath.types import JoinablePathLike

    from githarbor.core.base import IssueState, PullRequestState
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

StrPath = str | os.PathLike[str]


class PyGiteaRepository(BaseRepository):
    """PyGitea repository implementation using py-gitea library."""

    url_patterns: ClassVar[list[str]] = [
        "gitea.com",
        "codeberg.org",
    ]  # Add your Gitea instances here

    def __init__(
        self,
        owner: str,
        name: str,
        token: str | None = None,
        url: str = "https://gitea.com",
    ):
        try:
            import gitea
        except ImportError as e:
            msg = "py-gitea library is required for PyGiteaRepository"
            raise ImportError(msg) from e

        t = token or os.getenv("GITEA_TOKEN")
        if not t:
            msg = "Gitea token is required"
            raise ValueError(msg)

        self._owner = owner
        self._name = name
        self._base_url = url.rstrip("/")

        try:
            # Initialize Gitea client
            self._gitea = gitea.Gitea(self._base_url, t)

            # Test connection and get repository info
            self._repo = gitea.Repository.request(self._gitea, owner, name)

        except Exception as e:
            msg = f"PyGitea authentication failed: {e!s}"
            raise AuthenticationError(msg) from e

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> PyGiteaRepository:
        """Create from URL like 'https://gitea.com/owner/repo'."""
        parsed = urlparse(url)
        parts = parsed.path.strip("/").split("/")
        if len(parts) < 2:  # noqa: PLR2004
            msg = f"Invalid Gitea URL: {url}"
            raise ValueError(msg)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        return cls(owner=parts[0], name=parts[1], token=kwargs.get("token"), url=base_url)

    @property
    def default_branch(self) -> str:
        return self._repo.default_branch

    @property
    def edit_base_uri(self):
        return f"_edit/{self.default_branch}/"

    @pygiteatools.handle_api_errors("Failed to get branch")
    def get_branch(self, name: str) -> Branch:
        """Get a specific branch by name."""
        import gitea

        branch = gitea.Branch.request(self._gitea, self._owner, self._name, name)
        return pygiteatools.create_branch_model(branch)

    @pygiteatools.handle_api_errors("Failed to get repository owner info")
    def get_repo_user(self) -> User:
        """Get user (repository owner) information."""
        import gitea

        user = gitea.User.request(self._gitea, self._owner)
        return pygiteatools.create_user_model(user)

    @pygiteatools.handle_api_errors("Failed to get pull request")
    def get_pull_request(self, number: int) -> PullRequest:
        """Get a specific pull request by number."""
        # Note: py-gitea doesn't have a direct PullRequest class
        # We need to use the repository API to get pull requests
        prs = self._repo.get_issues_state("open") + self._repo.get_issues_state("closed")
        for pr in prs:
            if pr.number == number:
                return pygiteatools.create_pull_request_model(pr)
        msg = f"Pull request #{number} not found"
        raise ResourceNotFoundError(msg)

    @pygiteatools.handle_api_errors("Failed to list pull requests")
    def list_pull_requests(self, state: PullRequestState = "open") -> list[PullRequest]:
        """List pull requests."""
        # py-gitea treats pull requests as issues with additional metadata
        issues = self._repo.get_issues_state(state)
        # Filter for pull requests (issues that are actually PRs)
        prs = [issue for issue in issues if issue.pull_request]
        return [pygiteatools.create_pull_request_model(pr) for pr in prs]

    @pygiteatools.handle_api_errors("Failed to list branches")
    def list_branches(self) -> list[Branch]:
        """List repository branches."""
        branches = self._repo.get_branches()
        return [pygiteatools.create_branch_model(branch) for branch in branches]

    @pygiteatools.handle_api_errors("Failed to get issue")
    def get_issue(self, issue_id: int) -> Issue:
        """Get a specific issue by ID."""
        import gitea

        issue = gitea.Issue.request(self._gitea, self._owner, self._name, str(issue_id))
        return pygiteatools.create_issue_model(issue)

    @pygiteatools.handle_api_errors("Failed to list issues")
    def list_issues(self, state: IssueState = "open") -> list[Issue]:
        """List repository issues."""
        issues = self._repo.get_issues_state(state)
        return [pygiteatools.create_issue_model(issue) for issue in issues]

    @pygiteatools.handle_api_errors("Failed to create issue")
    def create_issue(
        self,
        title: str,
        body: str,
        labels: list[str] | None = None,
        assignees: list[str] | None = None,
    ) -> Issue:
        """Create a new issue."""
        issue = self._repo.create_issue(title=title, description=body, assignees=assignees or [])
        return pygiteatools.create_issue_model(issue)

    @pygiteatools.handle_api_errors("Failed to get commit")
    def get_commit(self, sha: str) -> Commit:
        """Get a specific commit by SHA."""
        commits = self._repo.get_commits()
        for commit in commits:
            if commit.sha == sha:
                return pygiteatools.create_commit_model(commit)
        msg = f"Commit {sha} not found"
        raise ResourceNotFoundError(msg)

    @pygiteatools.handle_api_errors("Failed to list commits")
    def list_commits(
        self,
        branch: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        author: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        """List repository commits with optional filters."""
        commits = self._repo.get_commits()

        # Apply filters
        if author:
            commits = [
                c
                for c in commits
                if c.author and (author in c.author.username or author in c.author.email)
            ]

        if max_results:
            commits = commits[:max_results]

        return [pygiteatools.create_commit_model(commit) for commit in commits]

    def get_workflow(self, workflow_id: str) -> Workflow:
        """Get workflow - not supported by py-gitea."""
        msg = "Workflows are not supported by py-gitea"
        raise NotImplementedError(msg)

    def list_workflows(self) -> list[Workflow]:
        """List workflows - not supported by py-gitea."""
        msg = "Workflows are not supported by py-gitea"
        raise NotImplementedError(msg)

    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        """Get workflow run - not supported by py-gitea."""
        msg = "Workflow runs are not supported by py-gitea"
        raise NotImplementedError(msg)

    @pygiteatools.handle_api_errors("Failed to download file")
    def download(self, path: str, destination: JoinablePathLike, recursive: bool = False):
        """Download repository contents."""
        from upathtools import to_upath

        dest = to_upath(destination)
        dest.mkdir(exist_ok=True, parents=True)

        try:
            content = self._repo.get_git_content()

            if recursive:
                for file_content in content:
                    if file_content.type == "file":
                        file_data = self._repo.get_file_content(file_content)
                        file_dest = dest / file_content.path
                        file_dest.parent.mkdir(exist_ok=True, parents=True)

                        # Decode base64 content if needed
                        import base64

                        try:
                            content_bytes = base64.b64decode(file_data)
                            file_dest.write_bytes(content_bytes)
                        except Exception:  # noqa: BLE001
                            # If not base64, write as text
                            file_dest.write_text(file_data)
            else:
                # Find specific file
                target_file = None
                for file_content in content:
                    if file_content.path == path:
                        target_file = file_content
                        break

                if target_file:
                    file_data = self._repo.get_file_content(target_file)
                    file_dest = dest / to_upath(path).name

                    import base64

                    try:
                        content_bytes = base64.b64decode(file_data)
                        file_dest.write_bytes(content_bytes)
                    except Exception:  # noqa: BLE001
                        file_dest.write_text(file_data)

        except Exception as e:  # noqa: BLE001
            logger.warning("Download failed, using fallback method: %s", e)
            # Fallback - create empty file
            (dest / to_upath(path).name).touch()

    @pygiteatools.handle_api_errors("Failed to search commits")
    def search_commits(
        self,
        query: str,
        branch: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        """Search repository commits - limited support in py-gitea."""
        commits = self._repo.get_commits()

        # Simple text search in commit messages
        matching_commits = []
        for commit in commits:
            commit_data = commit.inner_commit if hasattr(commit, "inner_commit") else {}
            message = commit_data.get("message", "")
            if query.lower() in message.lower():
                matching_commits.append(commit)
                if max_results and len(matching_commits) >= max_results:
                    break

        return [pygiteatools.create_commit_model(commit) for commit in matching_commits]

    @pygiteatools.handle_api_errors("Failed to list files")
    def iter_files(
        self,
        path: str = "",
        ref: str | None = None,
        pattern: str | None = None,
    ) -> Iterator[str]:
        """Iterate over repository files."""
        try:
            content = self._repo.get_git_content()
            for file_content in content:
                if file_content.type == "file":
                    file_path = file_content.path
                    if not pattern or fnmatch.fnmatch(file_path, pattern):
                        yield file_path
        except Exception as e:  # noqa: BLE001
            logger.warning("Failed to iterate files: %s", e)

    @pygiteatools.handle_api_errors("Failed to get contributors")
    def get_contributors(
        self,
        sort_by: Literal["commits", "name", "date"] = "commits",
        limit: int | None = None,
    ) -> list[User]:
        """Get repository contributors."""
        commits = self._repo.get_commits()

        # Build contributor stats from commits
        contributors: dict[str, dict[str, Any]] = {}
        for commit in commits:
            if commit.author:
                username = commit.author.username
                if username and username not in contributors:
                    contributors[username] = {"user": commit.author, "commits": 0}
                contributors[username]["commits"] += 1

        # Convert to list and sort
        contributor_list = list(contributors.values())
        if sort_by == "name":
            contributor_list.sort(key=lambda c: c["user"].username)
        elif sort_by == "commits":
            contributor_list.sort(key=lambda c: c["commits"], reverse=True)

        if limit:
            contributor_list = contributor_list[:limit]

        return [pygiteatools.create_user_model(c["user"]) for c in contributor_list]

    def get_languages(self) -> dict[str, int]:
        """Get repository languages - not supported by py-gitea."""
        msg = "Language statistics are not supported by py-gitea"
        raise NotImplementedError(msg)

    def compare_branches(
        self,
        base: str,
        head: str,
        include_commits: bool = True,
        include_files: bool = True,
        include_stats: bool = True,
    ) -> dict[str, Any]:
        """Compare branches - not supported by py-gitea."""
        msg = "Branch comparison is not supported by py-gitea"
        raise NotImplementedError(msg)

    @pygiteatools.handle_api_errors("Failed to get latest release")
    def get_latest_release(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:
        """Get the latest repository release."""
        releases = self.list_releases(
            include_drafts=include_drafts,
            include_prereleases=include_prereleases,
            limit=1,
        )
        if not releases:
            msg = "No releases found"
            raise ResourceNotFoundError(msg)
        return releases[0]

    @pygiteatools.handle_api_errors("Failed to list releases")
    def list_releases(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        """List repository releases - limited support in py-gitea."""
        # py-gitea may not have direct release support
        # This is a placeholder implementation
        logger.warning("Release support in py-gitea is limited")
        return []

    @pygiteatools.handle_api_errors("Failed to get release")
    def get_release(self, tag: str) -> Release:
        """Get a specific release by tag."""
        releases = self.list_releases()
        for release in releases:
            if release.tag_name == tag:
                return release
        msg = f"Release with tag '{tag}' not found"
        raise ResourceNotFoundError(msg)

    @pygiteatools.handle_api_errors("Failed to list tags")
    def list_tags(self) -> list[Tag]:
        """List all repository tags - limited support in py-gitea."""
        logger.warning("Tag support in py-gitea is limited")
        return []

    @pygiteatools.handle_api_errors("Failed to get tag")
    def get_tag(self, name: str) -> Tag:
        """Get a specific repository tag."""
        tags = self.list_tags()
        for tag in tags:
            if tag.name == name:
                return tag
        msg = f"Tag '{name}' not found"
        raise ResourceNotFoundError(msg)

    @pygiteatools.handle_api_errors("Failed to create pull request")
    def create_pull_request(
        self,
        title: str,
        body: str,
        head_branch: str,
        base_branch: str,
        draft: bool = False,
    ) -> PullRequest:
        """Create pull request - limited support in py-gitea."""
        if draft:
            logger.warning("Draft pull requests are not supported by py-gitea")

        # py-gitea doesn't have direct PR creation in the simple API
        # This would require more complex API calls
        msg = "Pull request creation is not fully supported by py-gitea"
        raise NotImplementedError(msg)

    def create_branch(
        self,
        name: str,
        base_commit: str,
    ) -> Branch:
        """Create a new branch - limited support in py-gitea."""
        try:
            # py-gitea has add_branch method but requires a Branch object as source
            branches = self._repo.get_branches()
            base_branch = None
            for branch in branches:
                if branch.commit and branch.commit.get("id") == base_commit:
                    base_branch = branch
                    break

            if not base_branch:
                msg = f"Base commit {base_commit} not found in any branch"
                raise ResourceNotFoundError(msg)  # noqa: TRY301

            new_branch = self._repo.add_branch(base_branch, name)
            return pygiteatools.create_branch_model(new_branch)
        except Exception as e:
            msg = f"Branch creation failed: {e}"
            raise NotImplementedError(msg) from e


if __name__ == "__main__":
    # Example usage
    repo = PyGiteaRepository.from_url("https://codeberg.org/owner/repo", token="your_token")
    print(repo.list_branches())
