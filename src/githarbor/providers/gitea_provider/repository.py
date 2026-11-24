"""Gitea repository implementation."""

from __future__ import annotations

import fnmatch
import logging
import os
from typing import TYPE_CHECKING, Any, ClassVar, Literal
from urllib.parse import urlparse

from githarbor.core.base import BaseRepository
from githarbor.exceptions import (
    AuthenticationError,
    FeatureNotSupportedError,
    ResourceNotFoundError,
)
from githarbor.providers.gitea_provider import utils as giteatools


logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime

    import giteapy
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
    )

StrPath = str | os.PathLike[str]


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
        import giteapy
        from giteapy.rest import ApiException

        t = token or os.getenv("GITEA_TOKEN")
        if not t:
            msg = "Gitea token is required"
            raise ValueError(msg)
        configuration = giteapy.Configuration()
        configuration.host = url.rstrip("/") + "/api/v1"
        configuration.api_key["token"] = t
        # configuration.temp_folder_path = ...
        self._owner = owner
        self._name = name
        try:
            self._api = giteapy.ApiClient(configuration)
            self._org_api = giteapy.OrganizationApi(self._api)
            self._repo_api = giteapy.RepositoryApi(self._api)
            self._issues_api = giteapy.IssueApi(self._api)
            self._user_api = giteapy.UserApi(self._api)
            # Verify access and get repo info
            self._repo: giteapy.Repository = self._repo_api.repo_get(owner, name)
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
        url = f"{parsed.scheme}://{parsed.netloc}"
        return cls(owner=parts[0], name=parts[1], token=kwargs.get("token"), url=url)

    @property
    def default_branch(self) -> str:
        return self._repo.default_branch

    @property
    def edit_base_uri(self) -> str:
        return f"_edit/{self.default_branch}/"

    @giteatools.handle_api_errors("Failed to get branch")
    def get_branch(self, name: str) -> Branch:
        """Get a specific branch by name."""
        branch = self._repo_api.repo_get_branch(self._owner, self._name, name)
        return giteatools.create_branch_model(branch)

    @giteatools.handle_api_errors("Failed to get repository owner info")
    def get_repo_user(self) -> User:
        """Get user (repository owner) information."""
        user = self._user_api.user_get(self._owner)
        return giteatools.create_user_model(user)

    @giteatools.handle_api_errors("Failed to get pull request")
    def get_pull_request(self, number: int) -> PullRequest:
        """Get a specific pull request by number."""
        pr = self._repo_api.repo_get_pull_request(self._owner, self._name, number)
        return giteatools.create_pull_request_model(pr)

    @giteatools.handle_api_errors("Failed to list pull requests")
    def list_pull_requests(self, state: PullRequestState = "open") -> list[PullRequest]:
        """List pull requests."""
        prs = self._repo_api.repo_list_pull_requests(
            self._owner,
            self._name,
            state=state,
        )
        assert isinstance(prs, list)
        return [giteatools.create_pull_request_model(pr) for pr in prs]  # pyright: ignore

    @giteatools.handle_api_errors("Failed to list branches")
    def list_branches(self) -> list[Branch]:
        branches = self._repo_api.repo_list_branches(self._owner, self._name)
        assert isinstance(branches, list)
        return [giteatools.create_branch_model(branch) for branch in branches]  # pyright: ignore

    @giteatools.handle_api_errors("Failed to get issue")
    def get_issue(self, issue_id: int) -> Issue:
        """Get a specific issue by ID."""
        issue: giteapy.Issue = self._issues_api.issue_get_issue(self._owner, self._name, issue_id)
        return giteatools.create_issue_model(issue)

    @giteatools.handle_api_errors("Failed to list issues")
    def list_issues(self, state: IssueState = "open") -> list[Issue]:
        """List repository issues."""
        issues: list[giteapy.Issue] = self._issues_api.issue_list_issues(
            self._owner,
            self._name,
            state=state,
        )
        return [giteatools.create_issue_model(issue) for issue in issues]

    @giteatools.handle_api_errors("Failed to create issue")
    def create_issue(
        self,
        title: str,
        body: str,
        labels: list[str] | None = None,
        assignees: list[str] | None = None,
    ) -> Issue:
        """Create a new issue."""
        params: dict[str, Any] = {
            "title": title,
            "body": body,
        }
        if labels:
            params["labels"] = labels
        if assignees:
            params["assignees"] = assignees

        issue = self._issues_api.issue_create_issue(
            owner=self._owner,
            repo=self._name,
            body=params,
        )
        return giteatools.create_issue_model(issue)

    @giteatools.handle_api_errors("Failed to get commit")
    def get_commit(self, sha: str) -> Commit:
        """Get a specific commit by SHA."""
        commit: giteapy.Commit = self._repo_api.repo_get_single_commit(self._owner, self._name, sha)
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
        if path:
            msg = "Filtering commits by path is not supported for Gitea"
            raise FeatureNotSupportedError(msg)
        kwargs: dict[str, Any] = {}
        if branch:
            kwargs["sha"] = branch

        commits = self._repo_api.repo_get_all_commits(self._owner, self._name, **kwargs)
        assert isinstance(commits, list)

        models = [giteatools.create_commit_model(commit) for commit in commits]
        if since:
            models = [m for m in models if m.created_at and m.created_at >= since]
        if until:
            models = [m for m in models if m.created_at and m.created_at <= until]
        if author:
            models = [
                m
                for m in models
                if m.author
                and (author in (m.author.name or "") or author in (m.author.email or ""))
            ]
        if max_results:
            models = models[:max_results]

        return models

    @giteatools.handle_api_errors("Failed to download file")
    def download(self, path: str, destination: JoinablePathLike, recursive: bool = False) -> None:
        """Download repository contents."""
        import base64

        from upathtools import to_upath

        dest = to_upath(destination)
        dest.mkdir(exist_ok=True, parents=True)

        def _decode_content(file_content: Any) -> bytes:
            raw = file_content.content or ""
            if getattr(file_content, "encoding", "base64") == "base64":
                return base64.b64decode(raw)
            return raw.encode()

        if recursive:
            contents = self._list_contents(str(path), self.default_branch)

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
                    file_dest.write_bytes(_decode_content(file_content))
        else:
            content = self._repo_api.repo_get_contents(
                self._owner,
                self._name,
                str(path),
                ref=self.default_branch,
            )
            file_dest = dest / to_upath(path).name
            file_dest.write_bytes(_decode_content(content))

    @giteatools.handle_api_errors("Failed to search commits")
    def search_commits(
        self,
        query: str,
        branch: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        """Search repository commits."""
        # giteapy has no commit search endpoint, so filter commit messages client-side.
        if path:
            msg = "Filtering commit search by path is not supported for Gitea"
            raise FeatureNotSupportedError(msg)
        kwargs = {"sha": branch} if branch else {}
        commits = self._repo_api.repo_get_all_commits(self._owner, self._name, **kwargs)
        query_lower = query.lower()
        matches = [c for c in commits if query_lower in (c.commit.message or "").lower()]
        if max_results:
            matches = matches[:max_results]
        return [giteatools.create_commit_model(commit) for commit in matches]

    def _list_contents(self, path: str, ref: str) -> list[Any]:
        """List the entries of a directory (giteapy only supports the root dir)."""
        path = path.strip("/")
        if not path:
            return self._repo_api.repo_get_contents_list(self._owner, self._name, ref=ref)
        return self._api.call_api(
            "/repos/{owner}/{repo}/contents/{filepath}",
            "GET",
            {"owner": self._owner, "repo": self._name, "filepath": path},
            [("ref", ref)],
            {"Accept": "application/json"},
            response_type="list[ContentsResponse]",
            auth_settings=["AccessToken", "AuthorizationHeaderToken", "BasicAuth", "Token"],
            _return_http_data_only=True,
        )

    @giteatools.handle_api_errors("Failed to list files")
    def iter_files(
        self,
        path: str = "",
        ref: str | None = None,
        pattern: str | None = None,
    ) -> Iterator[str]:
        """Iterate over repository files."""
        ref_ = ref or self.default_branch
        entries = self._list_contents(path, ref_)
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
                contributors[author.login] = {"user": author, "commits": 0}
            contributors[author.login]["commits"] += 1

        # Convert to list and sort
        contributor_list = list(contributors.values())
        if sort_by == "name":
            contributor_list.sort(key=lambda c: c["user"].login)
        elif sort_by == "commits":
            contributor_list.sort(key=lambda c: c["commits"], reverse=True)
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
        releases = self.list_releases(
            include_drafts=include_drafts,
            include_prereleases=include_prereleases,
            limit=1,
        )
        if not releases:
            msg = "No matching releases found"
            raise ResourceNotFoundError(msg)
        return releases[0]

    @giteatools.handle_api_errors("Failed to list releases")
    def list_releases(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        """List repository releases."""
        results = self._repo_api.repo_list_releases(self._owner, self._name)
        assert isinstance(results, list)
        filtered = [
            giteatools.create_release_model(release)
            for release in results
            if (include_drafts or not release.draft)
            and (include_prereleases or not release.prerelease)
        ]
        return filtered[:limit] if limit else filtered

    @giteatools.handle_api_errors("Failed to get release")
    def get_release(self, tag: str) -> Release:
        """Get a specific release by tag."""
        results = self._repo_api.repo_list_releases(self._owner, self._name)
        assert isinstance(results, list)
        for release in results:
            if release.tag_name == tag:
                return giteatools.create_release_model(release)
        msg = f"Release with tag {tag!r} not found"
        raise ResourceNotFoundError(msg)

    @giteatools.handle_api_errors("Failed to list tags")
    def list_tags(self) -> list[Tag]:
        """List all repository tags."""
        tags = self._repo_api.repo_list_tags(self._owner, self._name)
        return [giteatools.create_tag_model(tag) for tag in tags]

    @giteatools.handle_api_errors("Failed to get tag {name}")
    def get_tag(self, name: str) -> Tag:
        """Get a specific repository tag."""
        tags = self._repo_api.repo_list_tags(self._owner, self._name)
        for tag in tags:
            if tag.name == name:
                return giteatools.create_tag_model(tag)
        msg = f"Tag {name!r} not found"
        raise ResourceNotFoundError(msg)

    @giteatools.handle_api_errors("Failed to create pull request")
    def create_pull_request(
        self,
        title: str,
        body: str,
        head_branch: str,
        base_branch: str,
        draft: bool = False,
    ) -> PullRequest:
        # Gitea doesn't support draft PRs, so we'll ignore that parameter
        if draft:
            logger.warning("Gitea does not support draft pull requests")

        pr = self._repo_api.repo_create_pull_request(
            owner=self._owner,
            repo=self._name,
            body={"title": title, "body": body, "head": head_branch, "base": base_branch},
        )
        return giteatools.create_pull_request_model(pr)


if __name__ == "__main__":
    gitea = GiteaRepository.from_url(url="https://gitea.com/phil65/test")
    print(gitea.list_branches())
