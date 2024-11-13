from __future__ import annotations

from datetime import datetime
import fnmatch
import os
import re
from typing import TYPE_CHECKING, Any, ClassVar, Literal
from urllib.parse import urlparse

import gitlab
from gitlab.exceptions import GitlabAuthenticationError, GitlabGetError

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

    from gitlab.base import RESTObject


class GitLabRepository(Repository):
    """GitLab repository implementation."""

    url_patterns: ClassVar[list[str]] = ["gitlab.com"]
    TIMESTAMP_FORMATS: ClassVar[list[str]] = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",  # Format with explicit timezone
    ]

    def __init__(
        self,
        owner: str,
        name: str,
        token: str | None = None,
        url: str = "https://gitlab.com",
    ):
        try:
            t = token or os.getenv("GITLAB_TOKEN")
            if not t:
                msg = "GitLab token is required"
                raise ValueError(msg)

            self._gl = gitlab.Gitlab(url=url, private_token=t)
            self._gl.auth()
            self._repo = self._gl.projects.get(f"{owner}/{name}")
            self._owner = owner
            self._name = name

        except GitlabAuthenticationError as e:
            msg = f"GitLab authentication failed: {e!s}"
            raise AuthenticationError(msg) from e

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> GitLabRepository:
        """Create from URL like 'https://gitlab.com/owner/repo'."""
        parsed = urlparse(url)
        parts = parsed.path.strip("/").split("/")
        if len(parts) < 2:  # noqa: PLR2004
            msg = f"Invalid GitLab URL: {url}"
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
            branch = self._repo.branches.get(name)
            return Branch(
                name=branch.name,
                sha=branch.commit["id"],
                protected=branch.protected,
                created_at=None,  # GitLab doesn't provide branch creation date
                updated_at=None,  # GitLab doesn't provide branch update date
            )
        except GitlabGetError as e:
            msg = f"Branch {name} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def _parse_timestamp(self, timestamp: str) -> datetime:
        """Parse GitLab timestamp string to datetime.

        Args:
            timestamp: Timestamp string from GitLab API
        """
        # Convert 'Z' to +00:00 for consistent parsing
        timestamp = re.sub(r"Z$", "+00:00", timestamp)

        for fmt in self.TIMESTAMP_FORMATS:
            try:
                return datetime.strptime(timestamp, fmt)
            except ValueError:
                continue
        msg = f"Unable to parse timestamp: {timestamp}"
        raise ValueError(msg)

    def get_pull_request(self, number: int) -> PullRequest:
        try:
            mr = self._repo.mergerequests.get(number)
            return PullRequest(
                number=mr.iid,
                title=mr.title,
                description=mr.description or "",
                state=mr.state,
                source_branch=mr.source_branch,
                target_branch=mr.target_branch,
                created_at=self._parse_timestamp(mr.created_at),
                updated_at=self._parse_timestamp(mr.updated_at),
                merged_at=self._parse_timestamp(mr.merged_at),
                closed_at=self._parse_timestamp(mr.closed_at),
                # ... rest of implementation
            )
        except GitlabGetError as e:
            msg = f"Merge request #{number} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def list_pull_requests(self, state: str = "open") -> list[PullRequest]:
        try:
            mrs = self._repo.mergerequests.list(state=state, all=True)
            return [self.get_pull_request(mr.iid) for mr in mrs]
        except GitlabGetError as e:
            msg = f"Failed to list merge requests: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_issue(self, issue_id: int) -> Issue:
        try:
            issue = self._repo.issues.get(issue_id)
            return Issue(
                number=issue.iid,
                title=issue.title,
                description=issue.description or "",
                state=issue.state,
                created_at=self._parse_timestamp(issue.created_at),
                updated_at=self._parse_timestamp(issue.updated_at)
                if issue.updated_at
                else None,
                closed_at=self._parse_timestamp(issue.closed_at)
                if issue.closed_at
                else None,
                closed=issue.state == "closed",
                author=User(
                    username=issue.author["username"],
                    name=issue.author["name"],
                    avatar_url=issue.author["avatar_url"],
                )
                if issue.author
                else None,
                assignee=User(
                    username=issue.assignee["username"],
                    name=issue.assignee["name"],
                    avatar_url=issue.assignee["avatar_url"],
                )
                if issue.assignee
                else None,
                labels=[Label(name=lbl) for lbl in issue.labels],
            )
        except GitlabGetError as e:
            msg = f"Issue #{issue_id} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def list_issues(self, state: str | None = None) -> list[Issue]:
        if state == "open":
            state = "opened"
        try:
            issues = self._repo.issues.list(state=state, all=True)
            return [self.get_issue(issue.iid) for issue in issues]
        except GitlabGetError as e:
            msg = f"Failed to list issues: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_commit(self, sha: str) -> Commit:
        try:
            commit = self._repo.commits.get(sha)
            return Commit(
                sha=commit.id,
                message=commit.message,
                created_at=self._parse_timestamp(commit.created_at),
                author=User(
                    username=commit.author_name,
                    email=commit.author_email,
                    name=commit.author_name,
                ),
                url=commit.web_url,
            )
        except GitlabGetError as e:
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
        """List commits for the repository.

        Args:
            branch: Branch to get commits from. Defaults to default branch.
            since: Only commits after this date will be returned
            until: Only commits before this date will be returned
            path: Only commits containing this file path will be returned
            author: Filter commits by author name/email
            max_results: Maximum number of commits to return

        Returns:
            List of Commit objects

        Raises:
            ResourceNotFoundError: If commits cannot be retrieved
        """
        try:
            kwargs: dict[str, Any] = {}
            if branch:
                kwargs["ref_name"] = branch
            if since:
                kwargs["since"] = since.isoformat()
            if until:
                kwargs["until"] = until.isoformat()
            if path:
                kwargs["path"] = path
            if author:
                kwargs["author"] = author
            if max_results:
                kwargs["per_page"] = max_results
                kwargs["page"] = 1
            else:
                kwargs["all"] = True

            commits = self._repo.commits.list(**kwargs)

            # Convert to list to materialize the results
            commits = list(commits)

            return [self.get_commit(c.id) for c in commits]

        except GitlabGetError as e:
            msg = f"Failed to list commits: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_workflow(self, workflow_id: str) -> Workflow:
        try:
            pipeline = self._repo.pipelines.get(workflow_id)
            return Workflow(
                id=str(pipeline.id),
                name=pipeline.ref,
                path="",  # GitLab doesn't have workflow paths
                state=pipeline.status,
                created_at=self._parse_timestamp(pipeline.created_at),
                updated_at=None,
            )
        except GitlabGetError as e:
            msg = f"Pipeline {id} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def list_workflows(self) -> list[Workflow]:
        try:
            pipelines = self._repo.pipelines.list()
            return [self.get_workflow(str(p.id)) for p in pipelines]
        except GitlabGetError as e:
            msg = f"Failed to list pipelines: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        try:
            job = self._repo.jobs.get(run_id)
            return WorkflowRun(
                id=str(job.id),
                name=job.name,
                workflow_id=str(job.pipeline["id"]),
                status=job.status,
                conclusion=job.status,
                branch=job.ref,
                created_at=self._parse_timestamp(job.created_at),
                started_at=self._parse_timestamp(job.started_at)
                if job.started_at
                else None,
                completed_at=self._parse_timestamp(job.finished_at)
                if job.finished_at
                else None,
            )
        except GitlabGetError as e:
            msg = f"Job {id} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def download(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ):
        """Download a file or directory from this GitLab repository.

        Args:
            path: Path to the file or directory we want to download.
            destination: Path where file/directory should be saved.
            recursive: Download all files from a folder (and subfolders).
        """
        import upath

        dest = upath.UPath(destination)
        dest.mkdir(exist_ok=True, parents=True)

        if recursive:
            # For recursive downloads, we need to get all files in the directory
            try:
                items = self._repo.repository_tree(path=str(path), recursive=True)
                for item in items:
                    if item["type"] == "blob":  # Only download files, not directories
                        file_path = item["path"]
                        try:
                            content = self._repo.files.get(
                                file_path=file_path, ref=self.default_branch
                            )
                            # Create subdirectories if needed
                            file_dest = dest / file_path
                            file_dest.parent.mkdir(exist_ok=True, parents=True)
                            # Save the file content
                            file_dest.write_bytes(content.decode())
                        except GitlabGetError:
                            continue
            except GitlabGetError as e:
                msg = f"Failed to download directory {path}: {e!s}"
                raise ResourceNotFoundError(msg) from e
        else:
            # For single file download
            try:
                content = self._repo.files.get(
                    file_path=str(path), ref=self.default_branch
                )
                file_dest = dest / upath.UPath(path).name
                file_dest.write_bytes(content.decode())
            except GitlabGetError as e:
                msg = f"Failed to download file {path}: {e!s}"
                raise ResourceNotFoundError(msg) from e

    def search_commits(
        self,
        query: str,
        branch: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        kwargs: dict[str, Any] = {}
        if branch:
            kwargs["ref_name"] = branch
        if path:
            kwargs["path"] = path
        if max_results:
            kwargs["per_page"] = max_results
        commits = self._repo.commits.list(search=query, get_all=True, **kwargs)
        return [self.get_commit(c.id) for c in commits]

    def iter_files(
        self,
        path: str = "",
        ref: str | None = None,
        pattern: str | None = None,
    ) -> Iterator[str]:
        items = self._repo.repository_tree(
            path=path, ref=ref or self.default_branch, recursive=True
        )
        for item in items:
            if item["type"] == "blob" and (
                not pattern or fnmatch.fnmatch(item["path"], pattern)
            ):
                yield item["path"]

    def get_contributors(
        self,
        sort_by: Literal["commits", "name", "date"] = "commits",
        limit: int | None = None,
    ) -> list[User]:
        contributors = self._repo.users.list(include_stats=True)
        assert isinstance(contributors, list)
        if sort_by == "name":
            contributors = sorted(contributors, key=lambda c: c.username)
        elif sort_by == "date":
            contributors = sorted(contributors, key=lambda c: c.created_at)
        contributors = contributors[:limit] if limit else contributors
        return [
            User(
                username=c.username,
                name=c.name,
                email=getattr(c, "email", None),
                avatar_url=c.avatar_url,
                created_at=getattr(c, "created_at", None),
                locked=c.locked,
                state=c.state,
            )
            for c in contributors
        ]

    def get_languages(self) -> dict[str, int]:
        return self._repo.languages()

    def compare_branches(
        self,
        base: str,
        head: str,
        include_commits: bool = True,
        include_files: bool = True,
        include_stats: bool = True,
    ) -> dict[str, Any]:
        comparison = self._repo.compare(base, head)
        result: dict[str, Any] = {"ahead_by": len(comparison["commits"])}

        if include_commits:
            result["commits"] = [self.get_commit(c["id"]) for c in comparison["commits"]]
        if include_files:
            result["files"] = [f["new_path"] for f in comparison["diffs"]]
        if include_stats:
            result["stats"] = {
                "additions": sum(d["additions"] for d in comparison["diffs"]),
                "deletions": sum(d["deletions"] for d in comparison["diffs"]),
                "changes": len(comparison["diffs"]),
            }
        return result

    def get_recent_activity(
        self,
        days: int = 30,
        include_commits: bool = True,
        include_prs: bool = True,
        include_issues: bool = True,
    ) -> dict[str, int]:
        """Get repository activity statistics for the last N days.

        Args:
            days: Number of days to look back
            include_commits: Whether to include commit counts
            include_prs: Whether to include merge request counts
            include_issues: Whether to include issue counts

        Returns:
            Dictionary with activity counts by type
        """
        from datetime import datetime, timedelta

        since = datetime.now() - timedelta(days=days)
        activity = {}

        if include_commits:
            commits = self._repo.commits.list(since=since.isoformat(), all=True)
            activity["commits"] = len(list(commits))

        if include_prs:
            # Get merge requests updated in time period
            mrs = self._repo.mergerequests.list(updated_after=since.isoformat(), all=True)
            activity["pull_requests"] = len(list(mrs))

        if include_issues:
            # Get issues updated in time period
            issues = self._repo.issues.list(updated_after=since.isoformat(), all=True)
            activity["issues"] = len(list(issues))

        return activity

    def get_latest_release(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:
        try:
            # Get all releases
            releases = self._repo.releases.list()

            if not releases:
                msg = "No releases found"
                raise ResourceNotFoundError(msg)

            # Filter releases
            filtered: list[RESTObject] = []
            for release in releases:
                # GitLab doesn't have draft releases
                if not include_prereleases and release.tag_name.startswith((
                    "alpha",
                    "beta",
                    "rc",
                )):
                    continue
                filtered.append(release)

            if not filtered:
                msg = "No matching releases found"
                raise ResourceNotFoundError(msg)

            latest = filtered[0]  # GitLab returns in descending order

            return Release(
                tag_name=latest.tag_name,
                name=latest.name,
                description=latest.description or "",
                created_at=self._parse_timestamp(latest.created_at),
                published_at=self._parse_timestamp(latest.released_at),
                draft=False,  # GitLab doesn't have draft releases
                prerelease=latest.tag_name.startswith(("alpha", "beta", "rc")),
                author=User(
                    username=latest.author["username"],
                    name=latest.author["name"],
                    avatar_url=latest.author["avatar_url"],
                )
                if latest.author
                else None,
                assets=[
                    {
                        "name": asset["name"],
                        "url": asset["url"],
                        "size": asset.get("size", 0),
                        "download_count": 0,  # GitLab doesn't provide this
                    }
                    for asset in latest.assets.get("links", [])
                ]
                if latest.assets
                else [],
                url=latest.assets.get("_links", {}).get("self", ""),
                target_commitish=latest.commit["id"] if latest.commit else None,
            )

        except gitlab.exceptions.GitlabError as e:
            msg = f"Failed to get latest release: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def list_releases(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        try:
            releases: list[Release] = []
            for release in self._repo.releases.list():
                if not include_prereleases and release.tag_name.startswith((
                    "alpha",
                    "beta",
                    "rc",
                )):
                    continue

                releases.append(
                    Release(
                        tag_name=release.tag_name,
                        name=release.name,
                        description=release.description or "",
                        created_at=self._parse_timestamp(release.created_at),
                        published_at=self._parse_timestamp(release.released_at),
                        draft=False,
                        prerelease=release.tag_name.startswith(("alpha", "beta", "rc")),
                        author=User(
                            username=release.author["username"],
                            name=release.author["name"],
                            avatar_url=release.author["avatar_url"],
                        )
                        if release.author
                        else None,
                        assets=[
                            {
                                "name": asset["name"],
                                "url": asset["url"],
                                "size": asset.get("size", 0),
                            }
                            for asset in release.assets.get("links", [])
                        ]
                        if release.assets
                        else [],
                        url=release.assets.get("_links", {}).get("self", ""),
                        target_commitish=release.commit["id"] if release.commit else None,
                    )
                )

                if limit and len(releases) >= limit:
                    break

        except gitlab.exceptions.GitlabError as e:
            msg = f"Failed to list releases: {e!s}"
            raise ResourceNotFoundError(msg) from e
        else:
            return releases

    def get_release(self, tag: str) -> Release:
        try:
            release = self._repo.releases.get(tag)
            return Release(
                tag_name=release.tag_name,
                name=release.name,
                description=release.description or "",
                created_at=self._parse_timestamp(release.created_at),
                published_at=self._parse_timestamp(release.released_at),
                draft=False,
                prerelease=release.tag_name.startswith(("alpha", "beta", "rc")),
                author=User(
                    username=release.author["username"],
                    name=release.author["name"],
                    avatar_url=release.author["avatar_url"],
                )
                if release.author
                else None,
                assets=[
                    {
                        "name": asset["name"],
                        "url": asset["url"],
                        "size": asset.get("size", 0),
                    }
                    for asset in release.assets.get("links", [])
                ]
                if release.assets
                else [],
                url=release.assets.get("_links", {}).get("self", ""),
                target_commitish=release.commit["id"] if release.commit else None,
            )

        except gitlab.exceptions.GitlabError as e:
            msg = f"Release with tag {tag} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e


if __name__ == "__main__":
    repo = GitLabRepository("phil65", "test")
    print(repo.list_issues())
