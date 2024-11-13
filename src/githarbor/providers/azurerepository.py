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
    Release,
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

    def get_latest_release(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:
        try:
            # Azure DevOps uses "releases" API which is different from Git releases
            # This is a simplified version that maps to Git tags
            tags = self._git_client.get_tags(
                repository_id=self._repo.id,
                project=self._project,
            )

            if not tags:
                msg = "No releases found"
                raise ResourceNotFoundError(msg)

            # Sort by creation date
            latest_tag = sorted(
                tags, key=lambda t: t.commit.committer.date, reverse=True
            )[0]

            return Release(
                tag_name=latest_tag.name,
                name=latest_tag.name,
                description=latest_tag.message or "",
                created_at=latest_tag.commit.committer.date,
                published_at=latest_tag.commit.committer.date,
                draft=False,  # Azure doesn't have draft concept for tags
                prerelease=False,  # Azure doesn't have prerelease concept for tags
                author=User(
                    username=latest_tag.commit.committer.name,
                    name=latest_tag.commit.committer.name,
                    email=latest_tag.commit.committer.email,
                ),
                url=None,  # Azure doesn't provide direct URLs for tags
                target_commitish=latest_tag.commit.commit_id,
            )

        except AzureDevOpsServiceError as e:
            msg = f"Failed to get latest release: {e!s}"
            raise ResourceNotFoundError(msg) from e

    def list_releases(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        try:
            tags = self._git_client.get_tags(
                repository_id=self._repo.id,
                project=self._project,
            )

            releases = []
            for tag in sorted(tags, key=lambda t: t.commit.committer.date, reverse=True):
                releases.append(
                    Release(
                        tag_name=tag.name,
                        name=tag.name,
                        description=tag.message or "",
                        created_at=tag.commit.committer.date,
                        published_at=tag.commit.committer.date,
                        draft=False,
                        prerelease=False,
                        author=User(
                            username=tag.commit.committer.name,
                            name=tag.commit.committer.name,
                            email=tag.commit.committer.email,
                        ),
                        url=None,
                        target_commitish=tag.commit.commit_id,
                    )
                )

                if limit and len(releases) >= limit:
                    break

        except AzureDevOpsServiceError as e:
            msg = f"Failed to list releases: {e!s}"
            raise ResourceNotFoundError(msg) from e
        else:
            return releases

    def get_release(self, tag: str) -> Release:
        try:
            tag_ref = self._git_client.get_tag(
                repository_id=self._repo.id,
                project=self._project,
                tag=tag,
            )

            return Release(
                tag_name=tag_ref.name,
                name=tag_ref.name,
                description=tag_ref.message or "",
                created_at=tag_ref,
            )
        except AzureDevOpsServiceError as e:
            msg = f"Failed to get release {tag}: {e!s}"
            raise ResourceNotFoundError(msg) from e
