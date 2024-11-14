from __future__ import annotations

import fnmatch
import os
from typing import TYPE_CHECKING, Any, ClassVar, Literal
from urllib.parse import urlparse

from github import Auth, Github, NamedUser
from github.GithubException import GithubException

from githarbor.core.base import BaseRepository
from githarbor.exceptions import AuthenticationError, ResourceNotFoundError
from githarbor.providers import githubtools


HTML_ERROR_CODE = 404

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime

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


TOKEN = os.getenv("GITHUB_TOKEN")


class GitHubRepository(BaseRepository):
    """GitHub repository implementation."""

    url_patterns: ClassVar[list[str]] = ["github.com"]
    raw_prefix: ClassVar[str] = (
        "https://raw.githubusercontent.com/{owner}/{name}/{branch}/{path}"
    )

    def __init__(self, owner: str, name: str, token: str | None = None):
        """Initialize GitHub repository."""
        try:
            t = token or TOKEN
            if not t:
                msg = "GitHub token is required"
                raise ValueError(msg)

            self._gh = Github(auth=Auth.Token(t))
            self._repo = self._gh.get_repo(f"{owner}/{name}")
            self._owner = owner
            self._name = name
            self.user: NamedUser.NamedUser = self._gh.get_user(owner)  # type: ignore
        except GithubException as e:
            msg = f"GitHub authentication failed: {e!s}"
            raise AuthenticationError(msg) from e

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> GitHubRepository:
        """Create from URL like 'https://github.com/owner/repo'."""
        parsed = urlparse(url)
        parts = parsed.path.strip("/").split("/")
        if len(parts) < 2:  # noqa: PLR2004
            msg = f"Invalid GitHub URL: {url}"
            raise ValueError(msg)

        return cls(parts[0], parts[1], token=kwargs.get("token"))

    @property
    def name(self) -> str:
        return self._name

    @property
    def default_branch(self) -> str:
        return self._repo.default_branch

    @githubtools.handle_github_errors("Failed to get branch {name}")
    def get_branch(self, name: str) -> Branch:
        branch = self._repo.get_branch(name)
        model = githubtools.create_branch_model(branch)
        model.default = branch.name == self.default_branch
        return model

    @githubtools.handle_github_errors("Failed to get pull request {number}")
    def get_pull_request(self, number: int) -> PullRequest:
        pr = self._repo.get_pull(number)
        return githubtools.create_pull_request_model(pr)

    @githubtools.handle_github_errors("Failed to list pull requests")
    def list_pull_requests(self, state: str = "open") -> list[PullRequest]:
        prs = self._repo.get_pulls(state=state)
        return [githubtools.create_pull_request_model(pr) for pr in prs]

    @githubtools.handle_github_errors("Failed to get issue {issue_id}")
    def get_issue(self, issue_id: int) -> Issue:
        issue = self._repo.get_issue(issue_id)
        return githubtools.create_issue_model(issue)

    @githubtools.handle_github_errors("Failed to list issues")
    def list_issues(self, state: str = "open") -> list[Issue]:
        issues = self._repo.get_issues(state=state)
        return [githubtools.create_issue_model(issue) for issue in issues]

    @githubtools.handle_github_errors("Failed to get commit {sha}")
    def get_commit(self, sha: str) -> Commit:
        commit = self._repo.get_commit(sha)
        return githubtools.create_commit_model(commit)

    @githubtools.handle_github_errors("Failed to list commits")
    def list_commits(
        self,
        branch: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        author: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        kwargs = {
            "since": since,
            "until": until,
            "author": author,
            "path": path,
            "sha": branch,
        }
        # Filter out None values
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        commits = self._repo.get_commits(**kwargs)  # type: ignore
        results = commits[:max_results] if max_results else commits
        return [githubtools.create_commit_model(c) for c in results]

    @githubtools.handle_github_errors("Failed to get workflow {workflow_id}")
    def get_workflow(self, workflow_id: str) -> Workflow:
        workflow = self._repo.get_workflow(workflow_id)
        return githubtools.create_workflow_model(workflow)

    @githubtools.handle_github_errors("Failed to list workflows")
    def list_workflows(self) -> list[Workflow]:
        workflows = self._repo.get_workflows()
        return [githubtools.create_workflow_model(w) for w in workflows]

    @githubtools.handle_github_errors("Failed to get workflow run {run_id}")
    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        run = self._repo.get_workflow_run(int(run_id))
        return githubtools.create_workflow_run_model(run)

    @githubtools.handle_github_errors("Failed to download file {path}")
    def download(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ):
        user_name = self._gh.get_user().login if TOKEN else None
        return githubtools.download_from_github(
            org=self._owner,
            repo=self._name,
            path=path,
            destination=destination,
            username=user_name,
            token=TOKEN,
            recursive=recursive,
        )

    @githubtools.handle_github_errors("Failed to search commits")
    def search_commits(
        self,
        query: str,
        branch: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        # Build the search query
        search_query = f"{query} repo:{self._owner}/{self._name}"
        # Add branch qualifier if specified
        if branch:
            search_query += f" ref:{branch}"
        # Add path qualifier if specified
        if path:
            search_query += f" path:{path}"
        kwargs = {"query": search_query}
        # kwargs = {"query": f"{self._owner}/{self._name}+{query}"}
        # if branch:
        #     kwargs["ref"] = branch
        # if path:
        #     kwargs["path"] = path
        results = self._gh.search_commits(**kwargs)
        commits = list(results[:max_results] if max_results else results)
        return [self.get_commit(c.sha) for c in commits]

    @githubtools.handle_github_errors("Failed to list files for {path}")
    def iter_files(
        self,
        path: str = "",
        ref: str | None = None,
        pattern: str | None = None,
    ) -> Iterator[str]:
        contents = self._repo.get_contents(path, ref=ref or self.default_branch)
        assert isinstance(contents, list)
        kwargs = {"ref": ref} if ref else {}
        while contents:
            content = contents.pop(0)
            if content.type == "dir":
                c = self._repo.get_contents(content.path, **kwargs)
                assert isinstance(c, list)
                contents.extend(c)
            elif not pattern or fnmatch.fnmatch(content.path, pattern):
                yield content.path

    @githubtools.handle_github_errors("Failed to get contributors")
    def get_contributors(
        self,
        sort_by: Literal["commits", "name", "date"] = "commits",
        limit: int | None = None,
    ) -> list[User]:
        contributors = list(self._repo.get_contributors())
        if sort_by == "name":
            contributors = sorted(contributors, key=lambda c: c.login)
        elif sort_by == "date":
            contributors = sorted(contributors, key=lambda c: c.created_at)
        contributors = contributors[:limit] if limit else contributors
        return [u for c in contributors if (u := githubtools.create_user_model(c))]

    @githubtools.handle_github_errors("Failed to get languages")
    def get_languages(self) -> dict[str, int]:
        return self._repo.get_languages()

    @githubtools.handle_github_errors("Failed to compare branches")
    def compare_branches(
        self,
        base: str,
        head: str,
        include_commits: bool = True,
        include_files: bool = True,
        include_stats: bool = True,
    ) -> dict[str, Any]:
        comparison = self._repo.compare(base, head)
        result: dict[str, Any] = {
            "ahead_by": comparison.ahead_by,
            "behind_by": comparison.behind_by,
        }

        if include_commits:
            result["commits"] = [self.get_commit(c.sha) for c in comparison.commits]
        if include_files:
            result["files"] = [f.filename for f in comparison.files]
        if include_stats:
            result["stats"] = {
                "additions": comparison.total_commits,
                "deletions": comparison.total_commits,
                "changes": len(comparison.files),
            }
        return result

    @githubtools.handle_github_errors("Failed to get latest release")
    def get_latest_release(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:  # Changed from dict[str, Any] to Release
        releases = self._repo.get_releases()
        # Filter releases based on parameters
        filtered = [
            release
            for release in releases
            if (include_drafts or not release.draft)
            and (include_prereleases or not release.prerelease)
        ]
        if not filtered:
            msg = "No matching releases found"
            raise ResourceNotFoundError(msg)
        latest = filtered[0]  # Releases are returned in chronological order
        return githubtools.create_release_model(latest)

    @githubtools.handle_github_errors("Failed to list releases")
    def list_releases(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        filtered_releases = (
            release
            for release in self._repo.get_releases()
            if (include_drafts or not release.draft)
            and (include_prereleases or not release.prerelease)
        )
        return [
            githubtools.create_release_model(release)
            for release in (
                list(filtered_releases)[:limit] if limit else filtered_releases
            )
        ]

    @githubtools.handle_github_errors("Failed to get release {tag}")
    def get_release(self, tag: str) -> Release:
        release = self._repo.get_release(tag)
        return githubtools.create_release_model(release)

    @githubtools.handle_github_errors("Failed to get tag {name}")
    def get_tag(self, name: str) -> Tag:
        """Get a specific tag by name."""
        try:
            tag = self._repo.get_git_ref(f"tags/{name}")
            tag_obj = self._repo.get_git_tag(tag.object.sha)
            return githubtools.create_tag_model(tag_obj)
        except GithubException as e:
            if e.status == HTML_ERROR_CODE:  # Might be lightweight tag
                commit = self._repo.get_commit(name)
                return githubtools.create_tag_model(commit)
            raise

    @githubtools.handle_github_errors("Failed to list tags")
    def list_tags(self) -> list[Tag]:
        """List all repository tags."""
        return [githubtools.create_tag_model(tag) for tag in self._repo.get_tags()]


if __name__ == "__main__":
    repo = GitHubRepository.from_url("https://github.com/phil65/mknodes")
    commits = repo.search_commits("implement")
    print(commits)
    # print(repo.list_workflows())
    branch = repo.get_branch("main")
    print(branch)
