"""Azure DevOps repository implementation."""

from __future__ import annotations

import fnmatch
import os
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import urlparse

from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication

from githarbor.core.base import BaseRepository
from githarbor.core.models import (
    Branch,
    Commit,
    Issue,
    PullRequest,
    Release,
)
from githarbor.exceptions import AuthenticationError, ResourceNotFoundError
from githarbor.providers import azuretools


if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime

    from azure.devops.v7_1.build.build_client import BuildClient
    from azure.devops.v7_1.git.git_client import GitClient
    from azure.devops.v7_1.git.models import GitCommit, GitPullRequest, GitRepository
    from azure.devops.v7_1.work_item_tracking.work_item_tracking_client import (
        WorkItemTrackingClient,
    )


class AzureRepository(BaseRepository):
    """Azure DevOps repository implementation."""

    url_patterns: ClassVar[list[str]] = ["dev.azure.com", "visualstudio.com"]

    def __init__(
        self,
        organization: str,
        project: str,
        name: str,
        token: str | None = None,
    ) -> None:
        """Initialize Azure DevOps repository.

        Args:
            organization: Azure DevOps organization name
            project: Project name
            name: Repository name
            token: Personal access token for authentication

        Raises:
            AuthenticationError: If authentication fails
            ValueError: If token is missing
        """
        t = token or os.getenv("AZURE_DEVOPS_PAT")
        if not t:
            msg = "Azure DevOps PAT token is required"
            raise ValueError(msg)
        try:
            credentials = BasicAuthentication("", t)
            organization_url = f"https://dev.azure.com/{organization}"
            self._connection = Connection(base_url=organization_url, creds=credentials)

            self._git_client: GitClient = self._connection.clients.get_git_client()
            self._work_client: WorkItemTrackingClient = (
                self._connection.clients.get_work_item_tracking_client()
            )
            self._build_client: BuildClient = self._connection.clients.get_build_client()

            self._project = project
            self._name = name
            self._owner = organization
            self._repo: GitRepository = self._git_client.get_repository(
                name,
                project=project,
            )

        except Exception as e:
            msg = f"Azure DevOps authentication failed: {e!s}"
            raise AuthenticationError(msg) from e

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> AzureRepository:
        """Create from URL.

        Example URL: 'https://dev.azure.com/org/project/_git/repo'
        """
        parsed = urlparse(url)
        parts = parsed.path.strip("/").split("/")

        if len(parts) < 4:  # noqa: PLR2004
            msg = f"Invalid Azure DevOps URL: {url}"
            raise ValueError(msg)

        organization = parts[0]
        project = parts[1]
        repo_name = parts[3]  # After '_git'

        return cls(
            organization=organization,
            project=project,
            name=repo_name,
            token=kwargs.get("token"),
        )

    @property
    def name(self) -> str:
        """Repository name."""
        return self._name

    @property
    def default_branch(self) -> str:
        """Default branch name."""
        return self._repo.default_branch or "main"

    @azuretools.handle_azure_errors("Failed to get branch {name}")
    def get_branch(self, name: str) -> Branch:
        """Get branch information."""
        branch = self._git_client.get_branch(
            repository_id=self._repo.id,
            name=name,
            project=self._project,
        )
        commit = self._git_client.get_commit(branch.commit.commit_id, self._repo.id)
        return Branch(
            name=branch.name,
            sha=branch.commit.commit_id,
            protected=False,  # Azure DevOps handles branch protection differently
            default=branch.name == self.default_branch,
            created_at=None,  # Not provided by Azure API
            updated_at=None,  # Not provided by Azure API
            last_commit_date=commit.author.date,
            last_commit_message=commit.comment,
            last_commit_author=azuretools.create_user_model(commit.author),
        )

    @azuretools.handle_azure_errors("Failed to get pull request {number}")
    def get_pull_request(self, number: int) -> PullRequest:
        """Get pull request by number."""
        pr: GitPullRequest = self._git_client.get_pull_request_by_id(
            number,
            self._project,
        )
        return azuretools.create_pull_request_model(pr)

    @azuretools.handle_azure_errors("Failed to list pull requests")
    def list_pull_requests(self, state: str = "open") -> list[PullRequest]:
        """List pull requests."""
        # Map state to Azure DevOps status
        status_map = {"open": "active", "closed": "completed", "all": "all"}
        azure_status = status_map.get(state, "active")

        prs = self._git_client.get_pull_requests(
            self._repo.id,
            project=self._project,
            status=azure_status,
        )
        return [azuretools.create_pull_request_model(pr) for pr in prs]

    @azuretools.handle_azure_errors("Failed to get issue {issue_id}")
    def get_issue(self, issue_id: int) -> Issue:
        """Get issue by ID (work item in Azure DevOps)."""
        work_item = self._work_client.get_work_item(issue_id, self._project)
        return azuretools.create_issue_model(work_item)

    @azuretools.handle_azure_errors("Failed to list issues")
    def list_issues(self, state: str = "open") -> list[Issue]:
        """List issues (work items in Azure DevOps)."""
        # Build WIQL query based on state
        state_map = {"open": "Active", "closed": "Closed", "all": ""}
        azure_state = state_map.get(state, "Active")
        state_clause = f"AND [System.State] = '{azure_state}'" if azure_state else ""
        wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.TeamProject] = '{self._project}' "
            f"{state_clause} ORDER BY [System.ChangedDate] DESC"
        )
        wiql_result = self._work_client.query_by_wiql({"query": wiql})
        if not wiql_result.work_items:
            return []

        work_items = self._work_client.get_work_items(
            [wi.id for wi in wiql_result.work_items],
        )
        return [azuretools.create_issue_model(wi) for wi in work_items]

    @azuretools.handle_azure_errors("Failed to get commit {sha}")
    def get_commit(self, sha: str) -> Commit:
        """Get commit by SHA."""
        commit: GitCommit = self._git_client.get_commit(
            commit_id=sha,
            repository_id=self._repo.id,
            project=self._project,
        )
        return azuretools.create_commit_model(commit)

    @azuretools.handle_azure_errors("Failed to list commits")
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
        commits = self._git_client.get_commits(
            repository_id=self._repo.id,
            search_criteria=None,
            project=self._project,
            # branch_name=branch,
            # from_date=since,
            # to_date=until,
            # author=author,
            # item_path=path,
            top=max_results,
        )
        return [azuretools.create_commit_model(c) for c in commits]

    @azuretools.handle_azure_errors("Failed to download file {path}")
    def download(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ) -> None:
        """Download file(s) from repository."""
        return azuretools.download_from_azure(
            organization=self._owner,
            project=self._project,
            repo=self._repo.id,
            path=path,
            destination=destination,
            recursive=recursive,
        )

    @azuretools.handle_azure_errors("Failed to list files for {path}")
    def iter_files(
        self,
        path: str = "",
        ref: str | None = None,
        pattern: str | None = None,
    ) -> Iterator[str]:
        """Iterate over repository files."""
        items = self._git_client.get_items(
            repository_id=self._repo.id,
            project=self._project,
            recursion_level="full",
            version_descriptor={"version": ref} if ref else None,
        )
        for item in items:
            if item.is_folder:
                continue
            if not pattern or fnmatch.fnmatch(item.path, pattern):
                yield item.path

    @azuretools.handle_azure_errors("Failed to get latest release")
    def get_latest_release(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:
        """Get latest release (mapped from Git tags)."""
        tags = self._git_client.get_tags(
            repository_id=self._repo.id,
            project=self._project,
        )
        if not tags:
            msg = "No releases found"
            raise ResourceNotFoundError(msg)

        latest_tag = sorted(tags, key=lambda t: t.commit.committer.date, reverse=True)[0]
        return azuretools.create_release_model(latest_tag)
