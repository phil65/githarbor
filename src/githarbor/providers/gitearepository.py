from __future__ import annotations

import fnmatch
import os
from typing import TYPE_CHECKING, Any, ClassVar, Literal
from urllib.parse import urlparse

import giteapy
from giteapy.rest import ApiException
import upath

from githarbor.core.base import BaseRepository
from githarbor.exceptions import AuthenticationError, ResourceNotFoundError
from githarbor.providers import giteatools


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


class GiteaRepository(BaseRepository):
    """Gitea repository implementation."""

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
            t = token or os.getenv("GITEA_TOKEN")
            if not t:
                msg = "Gitea token is required"
                raise ValueError(msg)

            configuration = giteapy.Configuration()
            configuration.host = url.rstrip("/") + "/api/v1"
            configuration.api_key["token"] = t

            self._api = giteapy.ApiClient(configuration)
            self._org_api = giteapy.OrganizationApi(self._api)
            self._repo_api = giteapy.RepositoryApi(self._api)
            self._issues_api = giteapy.IssueApi(self._api)
            self._user_api = giteapy.UserApi(self._api)
            # Verify access and get repo info
            self._repo = self._repo_api.repo_get(owner, name)
            self._owner = owner
            self._name = name

        except ApiException as e:
            msg = f"Gitea authentication failed: {e!s}"
            raise AuthenticationError(msg) from e

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> GiteaRepository:
        """Create from URL like 'https://gitea.com/owner/repo'."""
        parsed = urlparse(url)
        parts = parsed.path.strip("/").split("/")
        if len(parts) < 2:  # noqa: PLR2004
            msg = f"Invalid Gitea URL: {url}"
            raise ValueError(msg)

        return cls(
            owner=parts[0],
            name=parts[1],
            token=kwargs.get("token"),
            url=f"{parsed.scheme}://{parsed.netloc}",
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def default_branch(self) -> str:
        return self._repo.default_branch

    @giteatools.handle_api_errors("Failed to get branch")
    def get_branch(self, name: str) -> Branch:
        """Get a specific branch by name."""
        branch = self._repo_api.repo_get_branch(self._owner, self._name, name)
        return giteatools.create_branch_model(branch)

    @giteatools.handle_api_errors("Failed to get pull request")
    def get_pull_request(self, number: int) -> PullRequest:
        """Get a specific pull request by number."""
        pr = self._repo_api.repo_get_pull(self._owner, self._name, number)
        return giteatools.create_pull_request_model(pr)

    @giteatools.handle_api_errors("Failed to list pull requests")
    def list_pull_requests(self, state: str = "open") -> list[PullRequest]:
        """List pull requests."""
        prs = self._repo_api.repo_list_pull_requests(
            self._owner,
            self._name,
            state=state,
        )
        assert isinstance(prs, list)
        return [giteatools.create_pull_request_model(pr) for pr in prs]

    @giteatools.handle_api_errors("Failed to get issue")
    def get_issue(self, issue_id: int) -> Issue:
        """Get a specific issue by ID."""
        issue = self._issues_api.issue_get_issue(self._owner, self._name, issue_id)
        return giteatools.create_issue_model(issue)

    @giteatools.handle_api_errors("Failed to list issues")
    def list_issues(self, state: str = "open") -> list[Issue]:
        """List repository issues."""
        issues = self._issues_api.issue_list_issues(
            self._owner,
            self._name,
            state=state,
        )
        assert isinstance(issues, list)
        return [giteatools.create_issue_model(issue) for issue in issues]

    @giteatools.handle_api_errors("Failed to get commit")
    def get_commit(self, sha: str) -> Commit:
        """Get a specific commit by SHA."""
        commit = self._repo_api.repo_get_single_commit(self._owner, self._name, sha)
        return giteatools.create_commit_model(commit)

    @giteatools.handle_api_errors("Failed to list commits")
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
        kwargs: dict[str, Any] = {}
        if branch:
            kwargs["sha"] = branch
        if since:
            kwargs["since"] = since.isoformat()
        if until:
            kwargs["until"] = until.isoformat()
        if path:
            kwargs["path"] = path
        if max_results:
            kwargs["limit"] = max_results

        commits = self._repo_api.repo_get_all_commits(self._owner, self._name, **kwargs)
        assert isinstance(commits, list)

        if author:
            commits = [
                c
                for c in commits
                if author in (c.commit.author.name or c.commit.author.email)
            ]

        return [giteatools.create_commit_model(commit) for commit in commits]

    def get_workflow(self, workflow_id: str) -> Workflow:
        raise NotImplementedError

    def list_workflows(self) -> list[Workflow]:
        raise NotImplementedError

    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        raise NotImplementedError

    @giteatools.handle_api_errors("Failed to download file")
    def download(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ) -> None:
        """Download repository contents."""
        dest = upath.UPath(destination)
        dest.mkdir(exist_ok=True, parents=True)

        if recursive:
            contents = self._repo_api.repo_get_contents_list(
                self._owner,
                self._name,
                str(path),
                ref=self.default_branch,
            )

            for content in contents:
                if content.type == "file":
                    file_content = self._repo_api.repo_get_contents(
                        self._owner,
                        self._name,
                        content.path,
                        ref=self.default_branch,
                    )
                    file_dest = dest / content.path
                    file_dest.parent.mkdir(exist_ok=True, parents=True)
                    file_dest.write_bytes(file_content.content.encode())
        else:
            content = self._repo_api.repo_get_contents(
                self._owner,
                self._name,
                str(path),
                ref=self.default_branch,
            )
            file_dest = dest / upath.UPath(path).name
            file_dest.write_bytes(content.content.encode())

    @giteatools.handle_api_errors("Failed to search commits")
    def search_commits(
        self,
        query: str,
        branch: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        """Search repository commits."""
        kwargs: dict[str, Any] = {}
        if branch:
            kwargs["ref_name"] = branch
        if path:
            kwargs["path"] = path
        if max_results:
            kwargs["limit"] = max_results

        commits = self._repo_api.repo_search_commits(
            self._owner,
            self._name,
            keyword=query,
            **kwargs,
        )
        return [giteatools.create_commit_model(commit) for commit in commits]

    @giteatools.handle_api_errors("Failed to list files")
    def iter_files(
        self,
        path: str = "",
        ref: str | None = None,
        pattern: str | None = None,
    ) -> Iterator[str]:
        """Iterate over repository files."""
        entries = self._repo_api.repo_get_contents_list(
            self._owner,
            self._name,
            str(path),
            ref=ref or self.default_branch,
        )
        for entry in entries:
            if entry.type == "file":
                if not pattern or fnmatch.fnmatch(entry.path, pattern):
                    yield entry.path
            elif entry.type == "dir":
                yield from self.iter_files(entry.path, ref, pattern)

    @giteatools.handle_api_errors("Failed to get contributors")
    def get_contributors(
        self,
        sort_by: Literal["commits", "name", "date"] = "commits",
        limit: int | None = None,
    ) -> list[User]:
        """Get repository contributors."""
        commits = self._repo_api.repo_get_all_commits(self._owner, self._name)
        assert isinstance(commits, list)

        # Build contributor stats from commits
        contributors: dict[str, dict[str, Any]] = {}
        for commit in commits:
            author = commit.author
            if not author:
                continue

            if author.login not in contributors:
                contributors[author.login] = {
                    "user": author,
                    "commits": 0,
                }
            contributors[author.login]["commits"] += 1

        # Convert to list and sort
        contributor_list = list(contributors.values())
        if sort_by == "name":
            contributor_list.sort(key=lambda c: c["user"].login)
        elif sort_by == "commits":
            contributor_list.sort(key=lambda c: c["commits"], reverse=True)

        # Apply limit if specified
        if limit:
            contributor_list = contributor_list[:limit]

        return [giteatools.create_user_model(c["user"]) for c in contributor_list]

    @giteatools.handle_api_errors("Failed to get latest release")
    def get_latest_release(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:
        """Get the latest repository release."""
        kwargs = {
            "draft": include_drafts,
            "pre_release": include_prereleases,
        }
        releases = self._repo_api.repo_list_releases(
            self._owner, self._name, per_page=1, **kwargs
        )

        if not releases:
            msg = "No matching releases found"
            raise ResourceNotFoundError(msg)

        return giteatools.create_release_model(releases[0])

    @giteatools.handle_api_errors("Failed to list releases")
    def list_releases(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        """List repository releases."""
        kwargs = {"per_page": limit} if limit else {}
        results = self._repo_api.repo_list_releases(self._owner, self._name, **kwargs)
        assert isinstance(results, list)
        return [
            giteatools.create_release_model(release)
            for release in results
            if (release.draft and include_drafts or not release.draft)
            and (release.prerelease and include_prereleases or not release.prerelease)
        ]

    @giteatools.handle_api_errors("Failed to get release")
    def get_release(self, tag: str) -> Release:
        """Get a specific release by tag."""
        release = self._repo_api.repo_get_release(self._owner, self._name, tag)
        return giteatools.create_release_model(release)

    @giteatools.handle_api_errors("Failed to list tags")
    def list_tags(self) -> list[Tag]:
        """List all repository tags."""
        tags = self._repo_api.repo_list_tags(self._owner, self._name)
        return [giteatools.create_tag_model(tag) for tag in tags]

    @giteatools.handle_api_errors("Failed to get tag {name}")
    def get_tag(self, name: str) -> Tag:
        """Get a specific repository tag."""
        tag = self._repo_api.repo_get_tag(self._owner, self._name, name)
        return giteatools.create_tag_model(tag)

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


if __name__ == "__main__":
    import os

    # Test Gitea API
    gitea = GiteaRepository.from_url(url="https://gitea.com/phil65/test")
    # print(gitea._repo_api.repo_list_releases("phil65", "test"))
    releases = gitea.list_releases()
    print(releases)
