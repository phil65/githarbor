from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import urlparse

from azure.devops.connection import Connection
from azure.devops.exceptions import AzureDevOpsServiceError
from msrest.authentication import BasicAuthentication

from githarbor.core.base import Repository
from githarbor.core.models import (
    Branch,
    Commit,
    Issue,
    PullRequest,
    User,
    Workflow,
    WorkflowRun,
)
from githarbor.exceptions import AuthenticationError, ResourceNotFoundError


if TYPE_CHECKING:
    from datetime import datetime


class AzureRepository(Repository):
    """Azure DevOps repository implementation."""

    url_patterns: ClassVar[list[str]] = ["dev.azure.com", "visualstudio.com"]

    def __init__(
        self,
        organization: str,
        project: str,
        name: str,
        token: str | None = None,
    ):
        try:
            t = token or os.getenv("AZURE_DEVOPS_PAT")
            if not t:
                msg = "Azure DevOps PAT token is required"
                raise ValueError(msg)

            credentials = BasicAuthentication("", t)
            organization_url = f"https://dev.azure.com/{organization}"
            self._connection = Connection(base_url=organization_url, creds=credentials)

            self._git_client = self._connection.clients.get_git_client()
            self._work_client = self._connection.clients.get_work_item_tracking_client()
            self._build_client = self._connection.clients.get_build_client()

            self._project = project
            self._name = name
            self._owner = organization

            # Get repository ID
            self._repo = self._git_client.get_repository(name, project=project)

        except AzureDevOpsServiceError as e:
            msg = f"Azure DevOps authentication failed: {e!s}"
            raise AuthenticationError(msg) from e

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> AzureRepository:
        """Create from URL like 'https://dev.azure.com/org/project/_git/repo'."""
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
        return self._name

    @property
    def default_branch(self) -> str:
        return self._repo.default_branch

    def get_branch(self, name: str) -> Branch:
        try:
            branch = self._git_client.get_branch(
                repository_id=self._repo.id,
                name=name,
                project=self._project,
            )
            return Branch(
                name=branch.name,
                sha=branch.commit.commit_id,
                protected=False,  # Azure DevOps handles branch protection differently
                created_at=None,  # Not provided by Azure API
                updated_at=None,  # Not provided by Azure API
            )
        except AzureDevOpsServiceError as e:
            msg = f"Branch {name} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_pull_request(self, number: int) -> PullRequest:
        try:
            pr = self._git_client.get_pull_request_by_id(number, self._project)
            return PullRequest(
                number=pr.pull_request_id,
                title=pr.title,
                description=pr.description or "",
                state=pr.status,
                source_branch=pr.source_ref_name.split("/")[-1],
                target_branch=pr.target_ref_name.split("/")[-1],
                created_at=pr.creation_date,
                updated_at=None,  # Not directly provided
                merged_at=pr.closed_date if pr.status == "completed" else None,
                closed_at=pr.closed_date
                if pr.status in ["abandoned", "completed"]
                else None,
            )
        except AzureDevOpsServiceError as e:
            msg = f"Pull request #{number} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def list_pull_requests(self, state: str = "open") -> list[PullRequest]:
        try:
            # Map state to Azure DevOps status
            status_map = {"open": "active", "closed": "completed", "all": "all"}
            azure_status = status_map.get(state, "active")

            prs = self._git_client.get_pull_requests(
                self._repo.id,
                project=self._project,
                status=azure_status,
            )
            return [self.get_pull_request(pr.pull_request_id) for pr in prs]
        except AzureDevOpsServiceError as e:
            msg = f"Failed to list pull requests: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_issue(self, issue_id: int) -> Issue:
        try:
            work_item = self._work_client.get_work_item(issue_id, self._project)
            return Issue(
                number=work_item.id,
                title=work_item.fields["System.Title"],
                description=work_item.fields.get("System.Description", ""),
                state=work_item.fields["System.State"],
                created_at=work_item.fields["System.CreatedDate"],
                updated_at=work_item.fields.get("System.ChangedDate"),
                closed_at=None,  # Not directly provided
                closed=work_item.fields["System.State"] in ["Closed", "Resolved"],
                author=User(
                    username=work_item.fields["System.CreatedBy"].get("uniqueName", ""),
                    name=work_item.fields["System.CreatedBy"].get("displayName", ""),
                    avatar_url=None,
                ),
                assignee=None,  # Would need additional processing
                labels=[],  # Would need additional processing
            )
        except AzureDevOpsServiceError as e:
            msg = f"Work item #{issue_id} not found: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def list_issues(self, state: str = "open") -> list[Issue]:
        msg = "Azure DevOps work items cannot be listed directly without a query"
        raise NotImplementedError(msg)

    def get_commit(self, sha: str) -> Commit:
        try:
            commit = self._git_client.get_commit(
                commit_id=sha,
                repository_id=self._repo.id,
                project=self._project,
            )
            return Commit(
                sha=commit.commit_id,
                message=commit.comment,
                created_at=commit.author.date,
                author=User(
                    username=commit.author.name,
                    email=commit.author.email,
                    name=commit.author.name,
                ),
                url=commit.url,
            )
        except AzureDevOpsServiceError as e:
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
            commits = self._git_client.get_commits(
                repository_id=self._repo.id,
                project=self._project,
                branch_name=branch,
                from_date=since,
                to_date=until,
                author=author,
                item_path=path,
                top=max_results,
            )
            return [self.get_commit(c.commit_id) for c in commits]
        except AzureDevOpsServiceError as e:
            msg = f"Failed to list commits: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def get_workflow(self, workflow_id: str) -> Workflow:
        msg = "Azure DevOps uses different concepts that don't map directly to workflows"
        raise NotImplementedError(msg)

    def list_workflows(self) -> list[Workflow]:
        msg = "Azure DevOps uses different concepts that don't map directly to workflows"
        raise NotImplementedError(msg)

    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        msg = "Azure DevOps uses different concepts that don't map directly to wf runs"
        raise NotImplementedError(msg)

    def download(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ):
        try:
            content = self._git_client.get_item_content(
                repository_id=self._repo.id,
                path=str(path),
                project=self._project,
                download=True,
            )

            import upath

            dest = upath.UPath(destination)
            dest.mkdir(exist_ok=True, parents=True)

            if recursive:
                msg = "Recursive download not yet implemented for Azure DevOps"
                raise NotImplementedError(msg)
            file_dest = dest / upath.UPath(path).name
            file_dest.write_bytes(content)

        except AzureDevOpsServiceError as e:
            msg = f"Failed to download {path}: {e!s}"
            raise ResourceNotFoundError(msg) from e
