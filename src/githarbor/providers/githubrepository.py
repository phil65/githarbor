from __future__ import annotations

import fnmatch
import logging
import os
from typing import TYPE_CHECKING, Any, ClassVar, Literal
from urllib.parse import urlparse

from github import Auth, Github, NamedUser
from github.GithubException import GithubException

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
    from datetime import datetime


TOKEN = os.getenv("GITHUB_TOKEN")


def download_from_github(
    org: str,
    repo: str,
    path: str | os.PathLike[str],
    destination: str | os.PathLike[str],
    username: str | None = None,
    token: str | None = None,
    recursive: bool = False,
):
    import fsspec
    import upath

    token = token or TOKEN
    if token and not username:
        token = None
    dest = upath.UPath(destination)
    dest.mkdir(exist_ok=True, parents=True)
    fs = fsspec.filesystem("github", org=org, repo=repo)
    logging.info("Copying files from Github: %s", path)
    files = fs.ls(str(path))
    fs.get(files, dest.as_posix(), recursive=recursive)


class GitHubRepository(Repository):
    """GitHub repository implementation."""

    url_patterns: ClassVar[list[str]] = ["github.com"]
    raw_prefix = "https://raw.githubusercontent.com/{owner}/{name}/{branch}/{path}"

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

    def get_branch(self, name: str) -> Branch:
        try:
            branch = self._repo.get_branch(name)
            return Branch(
                name=branch.name, sha=branch.commit.sha, protected=branch.protected
            )
        except GithubException as e:
            msg = f"Branch {name} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_pull_request(self, number: int) -> PullRequest:
        try:
            pr = self._repo.get_pull(number)
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
                    name=pr.user.name,
                    avatar_url=pr.user.avatar_url,
                )
                if pr.user
                else None,
                assignees=[
                    User(username=a.login, name=a.name, avatar_url=a.avatar_url)
                    for a in pr.assignees
                ],
                labels=[
                    Label(name=lbl.name, color=lbl.color, description=lbl.description)
                    for lbl in pr.labels
                ],
            )
        except GithubException as e:
            msg = f"Pull request #{number} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def list_pull_requests(self, state: str = "open") -> list[PullRequest]:
        try:
            prs = self._repo.get_pulls(state=state)
            return [self.get_pull_request(pr.number) for pr in prs]
        except GithubException as e:
            msg = f"Failed to list pull requests: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_issue(self, issue_id: int) -> Issue:
        try:
            issue = self._repo.get_issue(issue_id)
            return Issue(
                number=issue.number,
                title=issue.title,
                description=issue.body or "",
                state=issue.state,
                created_at=issue.created_at,
                updated_at=issue.updated_at,
                closed_at=issue.closed_at,
                closed=issue.closed_at is not None,
                author=User(
                    username=issue.user.login,
                    name=issue.user.name,
                    avatar_url=issue.user.avatar_url,
                )
                if issue.user
                else None,
                assignee=User(
                    username=issue.assignee.login,
                    name=issue.assignee.name,
                    avatar_url=issue.assignee.avatar_url,
                )
                if issue.assignee
                else None,
                labels=[
                    Label(name=lbl.name, color=lbl.color, description=lbl.description)
                    for lbl in issue.labels
                ],
            )
        except GithubException as e:
            msg = f"Issue #{issue_id} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def list_issues(self, state: str = "open") -> list[Issue]:
        try:
            issues = self._repo.get_issues(state=state)
            return [self.get_issue(issue.number) for issue in issues]
        except GithubException as e:
            msg = f"Failed to list issues: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_commit(self, sha: str) -> Commit:
        try:
            commit = self._repo.get_commit(sha)
            return Commit(
                sha=commit.sha,
                message=commit.commit.message,
                created_at=commit.commit.author.date,
                author=User(
                    username=commit.author.login if commit.author else "",
                    name=commit.commit.author.name,
                    email=commit.commit.author.email,
                ),
                committer=User(
                    username=commit.committer.login if commit.committer else "",
                    name=commit.commit.committer.name,
                    email=commit.commit.committer.email,
                )
                if commit.committer
                else None,
                url=commit.html_url,
                stats={
                    "additions": commit.stats.additions,
                    "deletions": commit.stats.deletions,
                    "total": commit.stats.total,
                },
                parents=[p.sha for p in commit.parents],
            )
        except GithubException as e:
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
            if since:
                kwargs["since"] = since
            if until:
                kwargs["until"] = until
            if author:
                kwargs["author"] = author
            if path:
                kwargs["path"] = path
            if branch:
                kwargs["sha"] = branch

            commits = self._repo.get_commits(**kwargs)

            # Apply max_results limit if specified
            filtered = list(commits[:max_results]) if max_results else list(commits)

            return [self.get_commit(c.sha) for c in filtered]

        except GithubException as e:
            msg = f"Failed to list commits: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_workflow(self, workflow_id: str) -> Workflow:
        try:
            workflow = self._repo.get_workflow(workflow_id)
            return Workflow(
                id=str(workflow.id),
                name=workflow.name,
                path=workflow.path,
                state=workflow.state,
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
            )
        except GithubException as e:
            msg = f"Workflow {id} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def list_workflows(self) -> list[Workflow]:
        try:
            workflows = self._repo.get_workflows()
            raw_prefix = f"https://raw.githubusercontent.com/{self._owner}/{self._name}/"
            return [
                Workflow(
                    id=str(w.id),
                    name=w.name,
                    path=w.path,
                    state=w.state,
                    created_at=w.created_at,
                    updated_at=w.updated_at,
                    description=w.name,  # GitHub API doesn't provide separate description
                    triggers=[],  # Would need to parse the workflow file to get triggers
                    disabled=w.state.lower() == "disabled",
                    last_run_at=None,  # Not directly available from the API
                    badge_url=w.badge_url,
                    definition=f"{raw_prefix}{self.default_branch}/{w.path}",
                )
                for w in workflows
            ]
        except GithubException as e:
            msg = f"Failed to list workflows: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        try:
            run = self._repo.get_workflow_run(int(run_id))
            return WorkflowRun(
                id=str(run.id),
                name=run.name,
                workflow_id=str(run.workflow_id),
                status=run.status,
                conclusion=run.conclusion,
                branch=run.head_branch,
                commit_sha=run.head_sha,
                url=run.html_url,
                created_at=run.created_at,
                updated_at=run.updated_at,
                started_at=run.run_started_at,
                # completed_at=run.run_concluded_at,
            )
        except GithubException as e:
            msg = f"Workflow run {id} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def download(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ):
        """Download a file from this github repository.

        Args:
            path: Path to the file we want to download.
            destination: Path where file should be saved.
            recursive: Download all files from a folder (and subfolders).
        """
        user_name = self._gh.get_user().login if TOKEN else None
        return download_from_github(
            org=self._owner,
            repo=self._name,
            path=path,
            destination=destination,
            username=user_name,
            token=TOKEN,
            recursive=recursive,
        )

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
        return [
            User(
                username=c.login,
                name=c.name,
                email=c.email,
                avatar_url=c.avatar_url,
                created_at=c.created_at,
            )
            for c in contributors
        ]

    def get_languages(self) -> dict[str, int]:
        return self._repo.get_languages()

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

    def get_latest_release(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:  # Changed from dict[str, Any] to Release
        """Get information about the latest release.

        Args:
            include_drafts: Whether to include draft releases
            include_prereleases: Whether to include pre-releases

        Returns:
            Release object containing release information

        Raises:
            ResourceNotFoundError: If no releases are found
        """
        try:
            # Get all releases
            releases = self._repo.get_releases()

            # Filter releases based on parameters
            filtered = []
            for release in releases:
                # Skip drafts if not requested
                if not include_drafts and release.draft:
                    continue

                # Skip pre-releases if not requested
                if not include_prereleases and release.prerelease:
                    continue

                filtered.append(release)

            if not filtered:
                msg = "No matching releases found"
                raise ResourceNotFoundError(msg)

            # Get latest release
            latest = filtered[0]  # Releases are returned in chronological order

            return Release(
                tag_name=latest.tag_name,
                name=latest.title,
                description=latest.body or "",
                created_at=latest.created_at,
                published_at=latest.published_at,
                draft=latest.draft,
                prerelease=latest.prerelease,
                author=User(
                    username=latest.author.login,
                    name=latest.author.name,
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
                        "created_at": asset.created_at,
                        "updated_at": asset.updated_at,
                    }
                    for asset in latest.assets
                ],
                url=latest.html_url,
                target_commitish=latest.target_commitish,
            )

        except GithubException as e:
            msg = f"Failed to get latest release: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def list_releases(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        """List repository releases.

        Args:
            include_drafts: Whether to include draft releases
            include_prereleases: Whether to include pre-releases
            limit: Maximum number of releases to return

        Returns:
            List of Release objects
        """
        try:
            releases: list[Release] = []
            for release in self._repo.get_releases():
                if not include_drafts and release.draft:
                    continue
                if not include_prereleases and release.prerelease:
                    continue

                releases.append(
                    Release(
                        tag_name=release.tag_name,
                        name=release.title,
                        description=release.body or "",
                        created_at=release.created_at,
                        published_at=release.published_at,
                        draft=release.draft,
                        prerelease=release.prerelease,
                        author=User(
                            username=release.author.login,
                            name=release.author.name,
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
                                "created_at": asset.created_at,
                                "updated_at": asset.updated_at,
                            }
                            for asset in release.assets
                        ],
                        url=release.html_url,
                        target_commitish=release.target_commitish,
                    )
                )

                if limit and len(releases) >= limit:
                    break

        except GithubException as e:
            msg = f"Failed to list releases: {e!s}"
            raise ResourceNotFoundError(msg) from e
        else:
            return releases

    def get_release(self, tag: str) -> Release:
        """Get a specific release by tag name.

        Args:
            tag: Tag name of the release

        Returns:
            Release object

        Raises:
            ResourceNotFoundError: If release is not found
        """
        try:
            release = self._repo.get_release(tag)
            return Release(
                tag_name=release.tag_name,
                name=release.title,
                description=release.body or "",
                created_at=release.created_at,
                published_at=release.published_at,
                draft=release.draft,
                prerelease=release.prerelease,
                author=User(
                    username=release.author.login,
                    name=release.author.name,
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
                        "created_at": asset.created_at,
                        "updated_at": asset.updated_at,
                    }
                    for asset in release.assets
                ],
                url=release.html_url,
                target_commitish=release.target_commitish,
            )

        except GithubException as e:
            msg = f"Release with tag {tag} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

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
            include_prs: Whether to include pull request counts
            include_issues: Whether to include issue counts

        Returns:
            Dictionary with activity counts by type
        """
        from datetime import UTC, datetime, timedelta

        since = datetime.now(UTC) - timedelta(days=days)
        activity = {}

        try:
            if include_commits:
                commits = self._repo.get_commits(since=since)
                activity["commits"] = len(list(commits))

            if include_prs:
                # Get PRs updated in time period
                prs = self._repo.get_pulls(state="all", sort="updated", direction="desc")
                activity["pull_requests"] = len([
                    pr for pr in prs if pr.updated_at and pr.updated_at >= since
                ])

            if include_issues:
                # Get issues updated in time period
                issues = self._repo.get_issues(
                    state="all", sort="updated", direction="desc"
                )
                activity["issues"] = len([
                    issue
                    for issue in issues
                    if issue.updated_at
                    and issue.updated_at >= since
                    # Exclude PRs which GitHub also returns as issues
                    and not hasattr(issue, "pull_request")
                ])

        except GithubException as e:
            msg = f"Failed to get recent activity: {e!s}"
            raise ResourceNotFoundError(msg) from e

        return activity


if __name__ == "__main__":
    repo = GitHubRepository.from_url("https://github.com/phil65/mknodes")
    commits = repo.search_commits("implement")
    print(commits)
    # print(repo.list_workflows())
    branch = repo.get_branch("main")
    print(branch)
