from __future__ import annotations

from datetime import datetime, timedelta
import fnmatch
import os
from typing import TYPE_CHECKING, Any, ClassVar, Literal
from urllib.parse import urlparse

import giteapy
from giteapy.rest import ApiException

from githarbor.core.base import Repository
from githarbor.core.models import (
    Branch,
    Commit,
    Issue,
    Label,
    PullRequest,
    Release,
    User,
    Workflow,
    WorkflowRun,
)
from githarbor.exceptions import AuthenticationError, ResourceNotFoundError


if TYPE_CHECKING:
    from collections.abc import Iterator


class GiteaRepository(Repository):
    """Gitea repository implementation."""

    url_patterns: ClassVar[list[str]] = ["gitea.com"]  # Add your Gitea instances here

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

    def get_branch(self, name: str) -> Branch:
        try:
            branch = self._repo_api.repo_get_branch(self._owner, self._name, name)
            return Branch(
                name=branch.name,
                sha=branch.commit.id,
                protected=branch.protected,
                created_at=None,  # Gitea doesn't provide branch creation date
                updated_at=None,  # Gitea doesn't provide branch update date
            )
        except ApiException as e:
            msg = f"Branch {name} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_pull_request(self, number: int) -> PullRequest:
        try:
            pr = self._repo_api.repo_get_pull(self._owner, self._name, number)
            return PullRequest(
                number=pr.number,
                title=pr.title,
                description=pr.body or "",
                state=pr.state,
                source_branch=pr.head.ref,
                target_branch=pr.base.ref,
                created_at=pr.created_at,
                updated_at=pr.updated_at,
                merged_at=pr.merged_at,
                closed_at=pr.closed_at,
                author=User(
                    username=pr.user.login,
                    name=pr.user.full_name or pr.user.login,
                    avatar_url=pr.user.avatar_url,
                )
                if pr.user
                else None,
                labels=[Label(name=label.name) for label in (pr.labels or [])],
            )
        except ApiException as e:
            msg = f"Pull request #{number} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def list_pull_requests(self, state: str = "open") -> list[PullRequest]:
        try:
            prs = self._repo_api.repo_list_pull_requests(
                self._owner,
                self._name,
                state=state,
            )
            return [self.get_pull_request(pr.number) for pr in prs]
        except ApiException as e:
            msg = f"Failed to list pull requests: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_issue(self, issue_id: int) -> Issue:
        try:
            issue = self._issues_api.issue_get_issue(self._owner, self._name, issue_id)
            return Issue(
                number=issue.number,
                title=issue.title,
                description=issue.body or "",
                state=issue.state,
                created_at=issue.created_at,
                updated_at=issue.updated_at,
                closed_at=issue.closed_at,
                closed=issue.state == "closed",
                author=User(
                    username=issue.user.login,
                    name=issue.user.full_name or issue.user.login,
                    avatar_url=issue.user.avatar_url,
                )
                if issue.user
                else None,
                assignee=User(
                    username=issue.assignee.login,
                    name=issue.assignee.full_name or issue.assignee.login,
                    avatar_url=issue.assignee.avatar_url,
                )
                if issue.assignee
                else None,
                labels=[Label(name=label.name) for label in (issue.labels or [])],
            )
        except ApiException as e:
            msg = f"Issue #{issue_id} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def list_issues(self, state: str = "open") -> list[Issue]:
        try:
            issues = self._issues_api.issue_list_issues(
                self._owner,
                self._name,
                state=state,
            )
            return [self.get_issue(issue.number) for issue in issues]
        except ApiException as e:
            msg = f"Failed to list issues: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_commit(self, sha: str) -> Commit:
        try:
            commit = self._repo_api.repo_get_single_commit(self._owner, self._name, sha)
            return Commit(
                sha=commit.sha,
                message=commit.commit.message,
                created_at=commit.commit.author._date,
                author=User(
                    username=commit.commit.author.name,
                    email=commit.commit.author.email,
                    name=commit.commit.author.name,
                ),
                url=commit.html_url,
            )
        except ApiException as e:
            msg = f"Commit {sha} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def list_commits(
        self,
        branch: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        author: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        try:
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

            commits = self._repo_api.repo_get_all_commits(
                self._owner,
                self._name,
                **kwargs,
            )

            # Filter by author if specified
            if author:
                commits = [
                    c
                    for c in commits
                    if author in (c.commit.author.name or c.commit.author.email)
                ]

            return [self.get_commit(c.sha) for c in commits]

        except ApiException as e:
            msg = f"Failed to list commits: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_workflow(self, workflow_id: str) -> Workflow:
        raise NotImplementedError

    def list_workflows(self) -> list[Workflow]:
        raise NotImplementedError

    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        raise NotImplementedError

    def download(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ):
        import upath

        dest = upath.UPath(destination)
        dest.mkdir(exist_ok=True, parents=True)

        try:
            if recursive:
                # Get repository contents recursively
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
                # Download single file
                content = self._repo_api.repo_get_contents(
                    self._owner,
                    self._name,
                    str(path),
                    ref=self.default_branch,
                )
                file_dest = dest / upath.UPath(path).name
                file_dest.write_bytes(content.content.encode())

        except ApiException as e:
            msg = f"Failed to download {path}: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def search_commits(
        self,
        query: str,
        branch: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        try:
            kwargs: dict[str, Any] = {"keyword": query}
            if branch:
                kwargs["ref"] = branch
            if path:
                kwargs["path"] = path
            if max_results:
                kwargs["limit"] = max_results
            commits = self._repo_api.repo_search_commits(
                self._owner, self._name, **kwargs
            )
            return [self.get_commit(c.sha) for c in commits]
        except ApiException as e:
            msg = f"Failed to search commits: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def iter_files(
        self,
        path: str = "",
        ref: str | None = None,
        pattern: str | None = None,
    ) -> Iterator[str]:
        try:
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
        except ApiException as e:
            msg = f"Failed to list files: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_contributors(
        self,
        sort_by: Literal["commits", "name", "date"] = "commits",
        limit: int | None = None,
    ) -> list[User]:
        try:
            # Get all commits to analyze contributors
            # since API doesn't provide direct endpoint
            commits = self._repo_api.repo_get_all_commits(self._owner, self._name)

            # Build contributor stats from commits
            contributors: dict[str, dict[str, Any]] = {}
            for commit in commits:
                author = commit.author
                if not author:
                    continue

                if author.login not in contributors:
                    contributors[author.login] = {
                        "username": author.login,
                        "name": author.full_name,
                        "avatar_url": author.avatar_url,
                        "commits": 0,
                    }
                contributors[author.login]["commits"] += 1

            # Convert to list and sort
            contributor_list = list(contributors.values())
            if sort_by == "name":
                contributor_list.sort(key=lambda c: c["username"])
            elif sort_by == "commits":
                contributor_list.sort(key=lambda c: c["commits"], reverse=True)

            # Apply limit if specified
            if limit:
                contributor_list = contributor_list[:limit]

            return [
                User(
                    username=c["username"],
                    name=c["name"],
                    avatar_url=c["avatar_url"],
                )
                for c in contributor_list
            ]

        except ApiException as e:
            msg = f"Failed to get contributors: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_latest_release(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:
        try:
            kwargs = {
                "draft": include_drafts,
                "pre_release": include_prereleases,
            }
            releases = self._repo_api.repo_list_releases(
                self._owner,
                self._name,
                limit=1,
                **kwargs,
            )

            if not releases:
                msg = "No matching releases found"
                raise ResourceNotFoundError(msg)

            latest = releases[0]

            return Release(
                tag_name=latest.tag_name,
                name=latest.name,
                description=latest.body or "",
                created_at=latest.created_at,
                published_at=latest.published_at,
                draft=latest.draft,
                prerelease=latest.prerelease,
                author=User(
                    username=latest.author.login,
                    name=latest.author.full_name,
                    avatar_url=latest.author.avatar_url,
                )
                if latest.author
                else None,
                assets=[
                    {
                        "name": asset.name,
                        "url": asset.browser_download_url,
                        "size": asset.size,
                        "download_count": asset.download_count,
                    }
                    for asset in latest.assets
                ],
                url=latest.url,
                target_commitish=latest.target_commitish,
            )

        except giteapy.ApiException as e:
            msg = f"Failed to get latest release: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def list_releases(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        try:
            kwargs: dict[str, Any] = {
                "draft": include_drafts,
                "pre_release": include_prereleases,
            }
            if limit:
                kwargs["limit"] = limit
                return [
                    Release(
                        tag_name=release.tag_name,
                        name=release.name,
                        description=release.body or "",
                        created_at=release.created_at,
                        published_at=release.published_at,
                        draft=release.draft,
                        prerelease=release.prerelease,
                        author=User(
                            username=release.author.login,
                            name=release.author.full_name,
                            avatar_url=release.author.avatar_url,
                        )
                        if release.author
                        else None,
                        assets=[
                            {
                                "name": asset.name,
                                "url": asset.browser_download_url,
                                "size": asset.size,
                                "download_count": asset.download_count,
                            }
                            for asset in release.assets
                        ],
                        url=release.url,
                        target_commitish=release.target_commitish,
                    )
                    for release in self._repo_api.repo_list_releases(
                        self._owner,
                        self._name,
                        **kwargs,
                    )
                ]
            return []
        except ApiException as e:
            msg = f"Failed to list releases: {e!s}"
            raise ResourceNotFoundError(msg) from e
        else:
            return []

    def get_release(self, tag: str) -> Release:
        try:
            release = self._repo_api.repo_get_release_by_tag(
                self._owner,
                self._name,
                tag,
            )
            return Release(
                tag_name=release.tag_name,
                name=release.name,
                description=release.body or "",
                created_at=release.created_at,
                published_at=release.published_at,
                draft=release.draft,
                prerelease=release.prerelease,
                author=User(
                    username=release.author.login,
                    name=release.author.full_name,
                    avatar_url=release.author.avatar_url,
                )
                if release.author
                else None,
                assets=[
                    {
                        "name": asset.name,
                        "url": asset.browser_download_url,
                        "size": asset.size,
                        "download_count": asset.download_count,
                    }
                    for asset in release.assets
                ],
                url=release.url,
                target_commitish=release.target_commitish,
            )

        except ApiException as e:
            msg = f"Release with tag {tag} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_languages(self) -> dict[str, int]:
        try:
            return self._repo_api.repo_get_languages(self._owner, self._name)
        except ApiException as e:
            msg = f"Failed to get languages: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def compare_branches(
        self,
        base: str,
        head: str,
        include_commits: bool = True,
        include_files: bool = True,
        include_stats: bool = True,
    ) -> dict[str, Any]:
        try:
            comparison = self._repo_api.repo_compare(self._owner, self._name, base, head)
            result: dict[str, Any] = {
                "ahead_by": len(comparison.commits),
                "behind_by": 0,  # Gitea API doesn't provide this info
            }

            if include_commits:
                result["commits"] = [self.get_commit(c.sha) for c in comparison.commits]
            if include_files:
                result["files"] = [f.filename for f in comparison.files]
            if include_stats:
                result["stats"] = {
                    "additions": sum(f.additions for f in comparison.files),
                    "deletions": sum(f.deletions for f in comparison.files),
                    "changes": len(comparison.files),
                }
        except ApiException as e:
            msg = f"Failed to compare branches: {e!s}"
            raise ResourceNotFoundError(msg) from e
        else:
            return result

    def get_recent_activity(
        self,
        days: int = 30,
        include_commits: bool = True,
        include_prs: bool = True,
        include_issues: bool = True,
    ) -> dict[str, int]:
        try:
            since = datetime.now() - timedelta(days=days)
            stats = {}

            if include_commits:
                commits = self._repo_api.repo_get_all_commits(
                    self._owner,
                    self._name,
                    since=since.isoformat(),
                )
                stats["commits"] = len(commits)

            if include_prs:
                prs = self._repo_api.repo_list_pull_requests(
                    self._owner,
                    self._name,
                    state="all",
                    since=since.isoformat(),
                )
                stats["pull_requests"] = len(prs)

            if include_issues:
                issues = self._issues_api.issue_list_issues(
                    self._owner,
                    self._name,
                    state="all",
                    since=since.isoformat(),
                )
                stats["issues"] = len(issues)

        except ApiException as e:
            msg = f"Failed to get recent activity: {e!s}"
            raise ResourceNotFoundError(msg) from e
        else:
            return stats


if __name__ == "__main__":
    import os

    # Test Gitea API
    gitea = GiteaRepository.from_url(url="https://gitea.com/phil65/test")
    # gitea.download("README.md", "teststststs")
    print(gitea.list_issues())
