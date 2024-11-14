from __future__ import annotations

import fnmatch
import functools
import logging
import os
from typing import TYPE_CHECKING, Any, ClassVar, Literal, ParamSpec, TypeVar
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
    from collections.abc import Callable, Iterator
    from datetime import datetime


TOKEN = os.getenv("GITHUB_TOKEN")

T = TypeVar("T")
P = ParamSpec("P")


def handle_github_errors(error_msg: str) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator to handle GitHub API exceptions consistently.

    Args:
        error_msg: Base error message to use in exception
    """

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            try:
                return func(*args, **kwargs)
            except GithubException as e:
                msg = f"{error_msg}: {e!s}"
                raise ResourceNotFoundError(msg) from e

        return wrapper

    return decorator


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

    def _create_user_model(self, gh_user: Any) -> User | None:
        """Create User model from GitHub user object."""
        if not gh_user:
            return None
        return User(
            username=gh_user.login,
            name=gh_user.name,
            email=gh_user.email,
            avatar_url=gh_user.avatar_url,
            created_at=gh_user.created_at,
            bio=gh_user.bio,
            location=gh_user.location,
            company=gh_user.company,
            url=gh_user.html_url,
            followers=gh_user.followers,
            following=gh_user.following,
            public_repos=gh_user.public_repos,
        )

    def _create_label_model(self, gh_label: Any) -> Label:
        """Create Label model from GitHub label object."""
        return Label(
            name=gh_label.name,
            color=gh_label.color,
            description=gh_label.description or "",
            url=gh_label.url,
        )

    def _create_pull_request_model(self, pr: Any) -> PullRequest:
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
            author=self._create_user_model(pr.user),
            assignees=[self._create_user_model(a) for a in pr.assignees if a],
            labels=[self._create_label_model(lbl) for lbl in pr.labels],
            merged_by=self._create_user_model(pr.merged_by),
            review_comments_count=pr.review_comments,
            commits_count=pr.commits,
            additions=pr.additions,
            deletions=pr.deletions,
            changed_files=pr.changed_files,
            mergeable=pr.mergeable,
            url=pr.html_url,
        )

    def _create_issue_model(self, issue: Any) -> Issue:
        return Issue(
            number=issue.number,
            title=issue.title,
            description=issue.body or "",
            state=issue.state,
            created_at=issue.created_at,
            updated_at=issue.updated_at,
            closed_at=issue.closed_at,
            closed=issue.state == "closed",
            author=self._create_user_model(issue.user),
            assignee=self._create_user_model(issue.assignee),
            labels=[self._create_label_model(lbl) for lbl in issue.labels],
            comments_count=issue.comments,
            url=issue.html_url,
            milestone=issue.milestone.title if issue.milestone else None,
        )

    def _create_commit_model(self, commit: Any) -> Commit:
        return Commit(
            sha=commit.sha,
            message=commit.commit.message,
            created_at=commit.commit.author.date,
            author=self._create_user_model(commit.author)
            or User(
                username="",
                name=commit.commit.author.name,
                email=commit.commit.author.email,
            ),
            committer=self._create_user_model(commit.committer),
            url=commit.html_url,
            stats={
                "additions": commit.stats.additions,
                "deletions": commit.stats.deletions,
                "total": commit.stats.total,
            },
            parents=[p.sha for p in commit.parents],
            # verified=commit.commit.verification.verified,
            files_changed=[f.filename for f in commit.files],
        )

    def _create_release_model(self, release: Any) -> Release:
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

    def _create_workflow_model(self, workflow: Any) -> Workflow:
        """Create Workflow model from GitHub workflow object."""
        raw_prefix = f"https://raw.githubusercontent.com/{self._owner}/{self._name}/"
        return Workflow(
            id=str(workflow.id),
            name=workflow.name,
            path=workflow.path,
            state=workflow.state,
            created_at=workflow.created_at,
            updated_at=workflow.updated_at,
            description=workflow.name,  # GitHub API doesn't provide separate description
            triggers=[],  # Would need to parse the workflow file to get triggers
            disabled=workflow.state.lower() == "disabled",
            last_run_at=None,  # Not directly available from the API
            badge_url=workflow.badge_url,
            definition=f"{raw_prefix}{self.default_branch}/{workflow.path}",
        )

    def _create_workflow_run_model(self, run: Any) -> WorkflowRun:
        """Create WorkflowRun model from GitHub workflow run object."""
        return WorkflowRun(
            id=str(run.id),
            name=run.name or run.display_title,
            workflow_id=str(run.workflow_id),
            status=run.status,
            conclusion=run.conclusion,
            branch=run.head_branch,
            commit_sha=run.head_sha,
            url=run.html_url,
            created_at=run.created_at,
            updated_at=run.updated_at,
            started_at=run.run_started_at,
            completed_at=run.run_attempt_started_at,
            run_number=run.run_number,
            jobs_count=len(list(run.jobs())),
            logs_url=run.logs_url,
        )

    @handle_github_errors("Failed to get branch")
    def get_branch(self, name: str) -> Branch:
        branch = self._repo.get_branch(name)
        last_commit = branch.commit
        return Branch(
            name=branch.name,
            sha=branch.commit.sha,
            protected=branch.protected,
            default=branch.name == self.default_branch,
            protection_rules=(
                {
                    "required_reviews": branch.get_required_status_checks(),
                    "dismiss_stale_reviews": (branch.get_required_pull_request_reviews()),
                    "require_code_owner_reviews": (branch.get_required_signatures()),
                }
                if branch.protected
                else None
            ),
            last_commit_date=last_commit.commit.author.date,
            last_commit_message=last_commit.commit.message,
            last_commit_author=self._create_user_model(last_commit.author),
        )

    @handle_github_errors("Failed to get pull request")
    def get_pull_request(self, number: int) -> PullRequest:
        pr = self._repo.get_pull(number)
        return self._create_pull_request_model(pr)

    @handle_github_errors("Failed to list pull requests")
    def list_pull_requests(self, state: str = "open") -> list[PullRequest]:
        prs = self._repo.get_pulls(state=state)
        return [self._create_pull_request_model(pr) for pr in prs]

    @handle_github_errors("Failed to get issue")
    def get_issue(self, issue_id: int) -> Issue:
        issue = self._repo.get_issue(issue_id)
        return self._create_issue_model(issue)

    @handle_github_errors("Failed to list issues")
    def list_issues(self, state: str = "open") -> list[Issue]:
        issues = self._repo.get_issues(state=state)
        return [self._create_issue_model(issue) for issue in issues]

    @handle_github_errors("Failed to get commit")
    def get_commit(self, sha: str) -> Commit:
        commit = self._repo.get_commit(sha)
        return self._create_commit_model(commit)

    @handle_github_errors("Failed to list commits")
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
        commits = self._repo.get_commits(**kwargs)
        results = commits[:max_results] if max_results else commits
        return [self._create_commit_model(c) for c in results]

    @handle_github_errors("Failed to get workflow")
    def get_workflow(self, workflow_id: str) -> Workflow:
        workflow = self._repo.get_workflow(workflow_id)
        return self._create_workflow_model(workflow)

    @handle_github_errors("Failed to list workflows")
    def list_workflows(self) -> list[Workflow]:
        workflows = self._repo.get_workflows()
        return [self._create_workflow_model(w) for w in workflows]

    @handle_github_errors("Failed to get workflow run")
    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        run = self._repo.get_workflow_run(int(run_id))
        return self._create_workflow_run_model(run)

    @handle_github_errors("Failed to download file")
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

    @handle_github_errors("Failed to search commits")
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

    @handle_github_errors("Failed to list files")
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

    @handle_github_errors("Failed to get contributors")
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

    @handle_github_errors("Failed to get languages")
    def get_languages(self) -> dict[str, int]:
        return self._repo.get_languages()

    @handle_github_errors("Failed to compare branches")
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

    @handle_github_errors("Failed to get latest release")
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
        return self._create_release_model(latest)

    @handle_github_errors("Failed to list releases")
    def list_releases(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        releases: list[Release] = []
        for release in self._repo.get_releases():
            if not include_drafts and release.draft:
                continue
            if not include_prereleases and release.prerelease:
                continue
            releases.append(self._create_release_model(release))
            if limit and len(releases) >= limit:
                break
        return releases

    @handle_github_errors("Failed to get release")
    def get_release(self, tag: str) -> Release:
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

    @handle_github_errors("Failed to get recent activity")
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
            issues = self._repo.get_issues(state="all", sort="updated", direction="desc")
            activity["issues"] = len([
                issue
                for issue in issues
                if issue.updated_at
                and issue.updated_at >= since
                # Exclude PRs which GitHub also returns as issues
                and not hasattr(issue, "pull_request")
            ])

        return activity


if __name__ == "__main__":
    repo = GitHubRepository.from_url("https://github.com/phil65/mknodes")
    commits = repo.search_commits("implement")
    print(commits)
    # print(repo.list_workflows())
    branch = repo.get_branch("main")
    print(branch)
