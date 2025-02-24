"""GitHub repository implementation using GitHubKit."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, ClassVar

from githubkit import GitHub
from githubkit.exception import GitHubException

from githarbor.core.base import BaseRepository, IssueState, PullRequestState
from githarbor.exceptions import ResourceNotFoundError
from githarbor.providers.githubkit_provider import utils as githubkittools


if TYPE_CHECKING:
    from datetime import datetime

    from githubkit.versions.latest.models import FullRepository

    from githarbor.core.models import (
        Branch,
        Comment,
        Commit,
        Issue,
        PullRequest,
        Release,
        Tag,
        User,
    )


class GitHubKitRepository(BaseRepository):
    """GitHub repository implementation using GitHubKit."""

    url_patterns: ClassVar[list[str]] = ["github.com"]
    is_async: ClassVar[bool] = True

    def __init__(
        self,
        owner: str,
        name: str,
        token: str | None = None,
    ) -> None:
        """Initialize GitHub API client.

        Args:
            owner: Repository owner
            name: Repository name
            token: GitHub access token

        Raises:
            AuthenticationError: If authentication fails
        """
        t = token or os.getenv("GITHUB_TOKEN")
        if not t:
            msg = "GitHub token is required"
            raise ValueError(msg)
        self._gh = GitHub(t)
        self._owner = owner
        self._name = name
        self._repo: FullRepository | None = None

    async def _ensure_repo(self) -> None:
        """Ensure repository info is loaded."""
        if self._repo is None:
            try:
                resp = await self._gh.rest.repos.async_get(self._owner, self._name)
                self._repo = resp.parsed_data
            except GitHubException as e:
                msg = f"Repository {self._owner}/{self._name} not found"
                raise ResourceNotFoundError(msg) from e

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> GitHubKitRepository:
        """Create instance from URL."""
        # Remove .git suffix and trailing slashes
        url = url.removesuffix(".git").rstrip("/")
        path = url.split("github.com/", 1)[1]
        owner, name = path.split("/")
        return cls(owner=owner, name=name, token=kwargs.get("token"))

    @property
    def default_branch(self) -> str:
        """Return default branch name."""
        if not self._repo:
            return "main"
        return self._repo.default_branch

    @property
    def edit_base_uri(self) -> str:
        """Return base URI for editing files."""
        return f"edit/{self.default_branch}/"

    @githubkittools.handle_githubkit_errors("Failed to get user info")
    async def get_repo_user_async(self) -> User:
        """Get repository owner information."""
        resp = await self._gh.rest.users.async_get_by_username(self._owner)
        return githubkittools.create_user_model(resp.parsed_data)

    @githubkittools.handle_githubkit_errors("Failed to get branch {name}")
    async def get_branch_async(self, name: str) -> Branch:
        """Get branch information."""
        resp = await self._gh.rest.repos.async_get_branch(self._owner, self._name, name)
        branch = githubkittools.create_branch_model(resp.parsed_data)
        branch.default = name == self.default_branch
        return branch

    @githubkittools.handle_githubkit_errors("Failed to get pull request {number}")
    async def get_pull_request_async(self, number: int) -> PullRequest:
        """Get pull request by number."""
        resp = await self._gh.rest.pulls.async_get(self._owner, self._name, number)
        return githubkittools.create_pull_request_model(resp.parsed_data)

    @githubkittools.handle_githubkit_errors("Failed to list pull requests")
    async def list_pull_requests_async(
        self, state: PullRequestState = "open"
    ) -> list[PullRequest]:
        """List pull requests."""
        resp = await self._gh.rest.pulls.async_list(self._owner, self._name, state=state)
        data = resp.parsed_data
        return [githubkittools.create_pull_request_model(pr) for pr in data]  # type: ignore

    @githubkittools.handle_githubkit_errors("Failed to get issue {issue_id}")
    async def get_issue_async(self, issue_id: int) -> Issue:
        """Get issue by ID."""
        resp = await self._gh.rest.issues.async_get(self._owner, self._name, issue_id)
        return githubkittools.create_issue_model(resp.parsed_data)

    @githubkittools.handle_githubkit_errors("Failed to list issues")
    async def list_issues_async(self, state: IssueState = "open") -> list[Issue]:
        """List repository issues."""
        resp = await self._gh.rest.issues.async_list_for_repo(
            self._owner,
            self._name,
            state=state,
        )
        data = resp.parsed_data
        return [githubkittools.create_issue_model(issue) for issue in data]  # type: ignore

    @githubkittools.handle_githubkit_errors("Failed to create issue")
    async def create_issue_async(
        self,
        title: str,
        body: str,
        labels: list[str] | None = None,
        assignees: list[str] | None = None,
    ) -> Issue:
        """Create a new issue."""
        response = await self._gh.rest.issues.async_create(
            owner=self._owner,
            repo=self._name,
            title=title,
            body=body,
            labels=labels or [],  # type: ignore
            assignees=assignees,
        )
        return githubkittools.create_issue_model(response.parsed_data)

    @githubkittools.handle_githubkit_errors("Failed to get commit {sha}")
    async def get_commit_async(self, sha: str) -> Commit:
        """Get commit by SHA."""
        resp = await self._gh.rest.repos.async_get_commit(self._owner, self._name, sha)
        return githubkittools.create_commit_model(resp.parsed_data)

    @githubkittools.handle_githubkit_errors("Failed to list commits")
    async def list_commits_async(
        self,
        branch: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        author: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        """List repository commits."""
        kwargs: dict[str, Any] = {"sha": branch} if branch else {}
        if since:
            kwargs["since"] = since.isoformat()
        if until:
            kwargs["until"] = until.isoformat()
        if author:
            kwargs["author"] = author
        if path:
            kwargs["path"] = path
        if max_results:
            kwargs["per_page"] = max_results

        resp = await self._gh.rest.repos.async_list_commits(
            self._owner, self._name, **kwargs
        )
        return [githubkittools.create_commit_model(c) for c in resp.parsed_data]

    @githubkittools.handle_githubkit_errors("Failed to download {path}")
    async def download_async(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ) -> None:
        """Download repository content."""
        import upath

        dest = upath.UPath(destination)
        dest.mkdir(exist_ok=True, parents=True)

        resp = await self._gh.rest.repos.async_get_content(
            self._owner,
            self._name,
            str(path),
            headers={"Accept": "application/vnd.github.raw"},
        )
        file_dest = dest / upath.UPath(path).name
        file_dest.write_bytes(resp.content)

    @githubkittools.handle_githubkit_errors("Failed to search commits")
    async def search_commits_async(
        self,
        query: str,
        branch: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        """Search repository commits."""
        search_query = f"repo:{self._owner}/{self._name} {query}"
        if branch:
            search_query += f" ref:{branch}"
        if path:
            search_query += f" path:{path}"
        if max_results:
            search_query += f" per_page:{max_results}"

        resp = await self._gh.rest.search.async_commits(q=search_query)
        return [
            githubkittools.create_commit_model(c.commit) for c in resp.parsed_data.items
        ]

    @githubkittools.handle_githubkit_errors("Failed to get languages")
    async def get_languages_async(self) -> dict[str, int]:
        """Get repository language statistics."""
        resp = await self._gh.rest.repos.async_list_languages(self._owner, self._name)
        return resp.parsed_data.model_dump()

    @githubkittools.handle_githubkit_errors("Failed to compare branches")
    async def compare_branches_async(
        self,
        base: str,
        head: str,
        include_commits: bool = True,
        include_files: bool = True,
        include_stats: bool = True,
    ) -> dict[str, Any]:
        """Compare two branches."""
        # Create the basehead string in the format "base...head"
        basehead = f"{base}...{head}"

        resp = await self._gh.rest.repos.async_compare_commits(
            self._owner,
            self._name,
            basehead,  # Single parameter combining base and head
        )
        comparison = resp.parsed_data
        result: dict[str, Any] = {
            "ahead_by": comparison.ahead_by,
            "behind_by": comparison.behind_by,
        }

        if include_commits:
            result["commits"] = [
                githubkittools.create_commit_model(c) for c in comparison.commits
            ]
        files = comparison.files or []
        if include_files:
            result["files"] = [f.filename for f in files]
        if include_stats:
            result["stats"] = {
                "additions": sum(f.additions for f in files),
                "deletions": sum(f.deletions for f in files),
                "changes": len(files),
            }
        return result

    @githubkittools.handle_githubkit_errors("Failed to get latest release")
    async def get_latest_release_async(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:
        """Get latest release."""
        try:
            resp = await self._gh.rest.repos.async_get_latest_release(
                self._owner, self._name
            )
            release = resp.parsed_data
            if (not include_drafts and release.draft) or (
                not include_prereleases and release.prerelease
            ):
                msg = "Latest release is draft/prerelease"
                raise ResourceNotFoundError(msg)
            return githubkittools.create_release_model(release)
        except GitHubException as e:
            msg = "No releases found"
            raise ResourceNotFoundError(msg) from e

    @githubkittools.handle_githubkit_errors("Failed to list releases")
    async def list_releases_async(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        """List releases."""
        resp = await self._gh.rest.repos.async_list_releases(self._owner, self._name)
        releases = []
        for release in resp.parsed_data:
            if not include_drafts and release.draft:
                continue
            if not include_prereleases and release.prerelease:
                continue
            releases.append(githubkittools.create_release_model(release))
            if limit and len(releases) >= limit:
                break
        return releases

    @githubkittools.handle_githubkit_errors("Failed to get release {tag}")
    async def get_release_async(self, tag: str) -> Release:
        """Get release by tag."""
        resp = await self._gh.rest.repos.async_get_release_by_tag(
            self._owner, self._name, tag
        )
        return githubkittools.create_release_model(resp.parsed_data)

    @githubkittools.handle_githubkit_errors("Failed to get tag {name}")
    async def get_tag_async(self, name: str) -> Tag:
        """Get tag by name."""
        resp = await self._gh.rest.git.async_get_tag(self._owner, self._name, name)
        data = resp.parsed_data
        return githubkittools.create_tag_model(data)

    @githubkittools.handle_githubkit_errors("Failed to list tags")
    async def list_tags_async(self) -> list[Tag]:
        """List repository tags."""
        resp = await self._gh.rest.repos.async_list_tags(self._owner, self._name)
        return [githubkittools.create_tag_model(tag) for tag in resp.parsed_data]

    @githubkittools.handle_githubkit_errors("Failed to create pull request")
    async def create_pull_request_async(
        self,
        title: str,
        body: str,
        head_branch: str,
        base_branch: str,
        draft: bool = False,
    ) -> PullRequest:
        response = await self._gh.rest.pulls.async_create(
            owner=self._owner,
            repo=self._name,
            title=title,
            body=body,
            head=head_branch,
            base=base_branch,
            draft=draft,
        )
        return githubkittools.create_pull_request_model(response.parsed_data)

    @githubkittools.handle_githubkit_errors("Failed to create branch")
    async def create_branch_async(
        self,
        name: str,
        base_commit: str,
    ) -> Branch:
        """Create a new branch at the specified commit."""
        await self._gh.rest.git.async_create_ref(
            owner=self._owner,
            repo=self._name,
            ref=f"refs/heads/{name}",
            sha=base_commit,
        )
        # Get the branch to return proper Branch model
        response = await self._gh.rest.repos.async_get_branch(
            owner=self._owner,
            repo=self._name,
            branch=name,
        )
        return githubkittools.create_branch_model(response.parsed_data)

    @githubkittools.handle_githubkit_errors("Failed to add pull request comment")
    async def add_pull_request_comment_async(
        self,
        number: int,
        body: str,
    ) -> Comment:
        response = await self._gh.rest.issues.async_create_comment(
            owner=self._owner,
            repo=self._name,
            issue_number=number,
            body=body,
        )
        return githubkittools.create_comment_model(response.parsed_data)

    @githubkittools.handle_githubkit_errors("Failed to add pull request review comment")
    async def add_pull_request_review_comment_async(
        self,
        number: int,
        body: str,
        commit_id: str,
        path: str,
        position: int,
    ) -> Comment:
        response = await self._gh.rest.pulls.async_create_review_comment(
            owner=self._owner,
            repo=self._name,
            pull_number=number,
            body=body,
            commit_id=commit_id,
            path=path,
            line=position,  # Using line instead of position as per API docs
        )
        return githubkittools.create_comment_model(response.parsed_data)


if __name__ == "__main__":

    async def main():
        provider = GitHubKitRepository("phil65", "llmling-agent")
        releases = await provider.list_pull_requests_async()
        print(releases)

    import asyncio

    asyncio.run(main())
