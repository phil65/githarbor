from __future__ import annotations

import fnmatch
import os
from typing import TYPE_CHECKING, Any, ClassVar, Literal
from urllib.parse import urlparse

import gitlab
from gitlab.exceptions import GitlabAuthenticationError

from githarbor.core.base import BaseRepository
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
from githarbor.exceptions import AuthenticationError, ResourceNotFoundError
from githarbor.providers import gitlabtools


if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime

    from gitlab.base import RESTObject


class GitLabRepository(BaseRepository):
    """GitLab repository implementation."""

    url_patterns: ClassVar[list[str]] = ["gitlab.com"]

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

    @gitlabtools.handle_gitlab_errors("Branch {name} not found")
    def get_branch(self, name: str) -> Branch:
        branch = self._repo.branches.get(name)
        return Branch(
            name=branch.name,
            sha=branch.commit["id"],
            protected=branch.protected,
            created_at=None,  # GitLab doesn't provide branch creation date
            updated_at=None,  # GitLab doesn't provide branch update date
        )

    @gitlabtools.handle_gitlab_errors("Merge request #{number} not found")
    def get_pull_request(self, number: int) -> PullRequest:
        mr = self._repo.mergerequests.get(number)
        return gitlabtools.create_pull_request_model(mr)

    @gitlabtools.handle_gitlab_errors("Failed to list merge requests")
    def list_pull_requests(self, state: str = "open") -> list[PullRequest]:
        mrs = self._repo.mergerequests.list(state=state, all=True)
        return [gitlabtools.create_pull_request_model(mr) for mr in mrs]

    @gitlabtools.handle_gitlab_errors("Issue #{issue_id} not found")
    def get_issue(self, issue_id: int) -> Issue:
        issue = self._repo.issues.get(issue_id)
        return gitlabtools.create_issue_model(issue)

    @gitlabtools.handle_gitlab_errors("Failed to list issues")
    def list_issues(self, state: str | None = None) -> list[Issue]:
        if state == "open":
            state = "opened"
        issues = self._repo.issues.list(state=state, all=True)
        return [gitlabtools.create_issue_model(issue) for issue in issues]

    @gitlabtools.handle_gitlab_errors("Commit {sha} not found")
    def get_commit(self, sha: str) -> Commit:
        commit = self._repo.commits.get(sha)
        return gitlabtools.create_commit_model(commit)

    @gitlabtools.handle_gitlab_errors("Failed to list commits")
    def list_commits(
        self,
        branch: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        author: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
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
        return [gitlabtools.create_commit_model(commit) for commit in commits]

    @gitlabtools.handle_gitlab_errors("Pipeline {workflow_id} not found")
    def get_workflow(self, workflow_id: str) -> Workflow:
        pipeline = self._repo.pipelines.get(workflow_id)
        return gitlabtools.create_workflow_model(pipeline)

    @gitlabtools.handle_gitlab_errors("Failed to list pipelines")
    def list_workflows(self) -> list[Workflow]:
        pipelines = self._repo.pipelines.list()
        return [gitlabtools.create_workflow_model(pipeline) for pipeline in pipelines]

    @gitlabtools.handle_gitlab_errors("Job {run_id} not found")
    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        job = self._repo.jobs.get(run_id)
        return gitlabtools.create_workflow_run_model(job)

    @gitlabtools.handle_gitlab_errors("Failed to download {path}")
    def download(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ):
        import upath

        dest = upath.UPath(destination)
        dest.mkdir(exist_ok=True, parents=True)

        if recursive:
            # For recursive downloads, we need to get all files in the directory
            items = self._repo.repository_tree(path=str(path), recursive=True)
            for item in items:
                if item["type"] == "blob":  # Only download files, not directories
                    file_path = item["path"]
                    content = self._repo.files.get(
                        file_path=file_path, ref=self.default_branch
                    )
                    # Create subdirectories if needed
                    file_dest = dest / file_path
                    file_dest.parent.mkdir(exist_ok=True, parents=True)
                    # Save the file content
                    file_dest.write_bytes(content.decode())
        else:
            # For single file download
            content = self._repo.files.get(file_path=str(path), ref=self.default_branch)
            file_dest = dest / upath.UPath(path).name
            file_dest.write_bytes(content.decode())

    @gitlabtools.handle_gitlab_errors("Failed to search commits")
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
        return [gitlabtools.create_commit_model(commit) for commit in commits]

    @gitlabtools.handle_gitlab_errors("Failed to iter files from {path}")
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

    @gitlabtools.handle_gitlab_errors("Failed to get contributors")
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
        items = [gitlabtools.create_user_model(c) for c in contributors]
        return [i for i in items if i is not None]

    @gitlabtools.handle_gitlab_errors("Failed to get languages")
    def get_languages(self) -> dict[str, int]:
        return self._repo.languages()

    @gitlabtools.handle_gitlab_errors("Failed to compare branches {base} and {head}")
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
            result["commits"] = [
                Commit(
                    sha=c["id"],
                    message=c["message"],
                    created_at=gitlabtools.parse_timestamp(c["created_at"]),
                    author=User(
                        username=c["author_name"],
                        email=c["author_email"],
                        name=c["author_name"],
                    ),
                    url=c["web_url"],
                )
                for c in comparison["commits"]
            ]
        if include_files:
            result["files"] = [f["new_path"] for f in comparison["diffs"]]
        if include_stats:
            result["stats"] = {
                "additions": sum(d["additions"] for d in comparison["diffs"]),
                "deletions": sum(d["deletions"] for d in comparison["diffs"]),
                "changes": len(comparison["diffs"]),
            }
        return result

    @gitlabtools.handle_gitlab_errors("Failed to get latest release")
    def get_latest_release(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:
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
        return gitlabtools.create_release_model(latest)

    @gitlabtools.handle_gitlab_errors("Failed to list releases")
    def list_releases(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        releases: list[Release] = []
        for release in self._repo.releases.list():
            if not include_prereleases and release.tag_name.startswith((
                "alpha",
                "beta",
                "rc",
            )):
                continue
            releases.append(gitlabtools.create_release_model(release))
            if limit and len(releases) >= limit:
                break
        return releases

    @gitlabtools.handle_gitlab_errors("Release with tag {tag} not found")
    def get_release(self, tag: str) -> Release:
        release = self._repo.releases.get(tag)
        return gitlabtools.create_release_model(release)

    @gitlabtools.handle_gitlab_errors("Failed to get tag {name}")
    def get_tag(self, name: str) -> Tag:
        """Get a specific tag by name."""
        tag = self._repo.tags.get(name)
        return Tag(
            name=tag.name,
            sha=tag.commit["id"],
            message=tag.message,
            created_at=tag.commit["created_at"],
            author=gitlabtools.create_user_model(tag.commit["author"]),
            url=f"{self._repo.web_url}/-/tags/{name}",
            verified=bool(getattr(tag, "verified", False)),
        )

    @gitlabtools.handle_gitlab_errors("Failed to list tags")
    def list_tags(self) -> list[Tag]:
        """List all repository tags."""
        return [
            Tag(
                name=tag.name,
                sha=tag.commit["id"],
                message=tag.message,
                created_at=tag.commit["created_at"],
                author=gitlabtools.create_user_model(tag.commit["author"]),
                url=f"{self._repo.web_url}/-/tags/{tag.name}",
                verified=bool(getattr(tag, "verified", False)),
            )
            for tag in self._repo.tags.list()
        ]


if __name__ == "__main__":
    repo = GitLabRepository("phil65", "test")
    print(repo.list_issues())
