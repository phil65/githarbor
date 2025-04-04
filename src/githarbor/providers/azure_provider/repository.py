"""Azure DevOps repository implementation."""

from __future__ import annotations

import fnmatch
import os
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import urlparse

from githarbor.core.base import BaseRepository, IssueState, PullRequestState
from githarbor.core.models import (
    Branch,
    Comment,
    Commit,
    Issue,
    PullRequest,
    User,
)
from githarbor.exceptions import AuthenticationError, ResourceNotFoundError
from githarbor.providers.azure_provider import utils as azuretools


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
        from azure.devops.connection import Connection
        from msrest.authentication import BasicAuthentication

        t = token or os.getenv("AZURE_DEVOPS_PAT")
        if not t:
            msg = "Azure DevOps PAT token is required"
            raise ValueError(msg)
        try:
            credentials = BasicAuthentication("", t)
            organization_url = f"https://dev.azure.com/{organization}"
            self._connection = Connection(base_url=organization_url, creds=credentials)

            self._client: GitClient = self._connection.clients.get_git_client()
            self._work_client: WorkItemTrackingClient = (
                self._connection.clients.get_work_item_tracking_client()
            )
            self._build_client: BuildClient = self._connection.clients.get_build_client()

            self._project = project
            self._name = name
            self._owner = organization
            self._repo: GitRepository = self._client.get_repository(name, project=project)

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

        org = parts[0]
        project = parts[1]
        repo_name = parts[3]  # After '_git'
        token = kwargs.get("token")
        return cls(organization=org, project=project, name=repo_name, token=token)

    @property
    def default_branch(self) -> str:
        """Default branch name."""
        return self._repo.default_branch or "main"

    @azuretools.handle_azure_errors("Failed to get user {username}")
    def get_user(self, username: str | None = None) -> User:
        """Get user information.

        If username is not provided, returns the authenticated user.

        Args:
            username: Optional username to get information for.

        Returns:
            User model with user information.
        """
        # Get the identity client
        identity_client = self._connection.clients.get_identity_client()

        if username is None:
            # Get the authenticated user
            core_client = self._connection.clients.get_core_client()
            user_info = core_client.get_authorized_user()
        else:
            # Search for the specific user
            filter_ = f"General,localAccount,{username}"
            user_descriptor = identity_client.read_identities(search_filter=filter_)
            if not user_descriptor or not user_descriptor[0]:
                msg = f"User {username} not found"
                raise ResourceNotFoundError(msg)
            user_info = user_descriptor[0]

        return User(
            username=user_info.properties.get("Account", user_info.unique_name),
            name=user_info.display_name or user_info.unique_name,
            email=user_info.properties.get("Mail", ""),
            avatar_url=user_info.properties.get("Avatar", ""),
        )

    @azuretools.handle_azure_errors("Failed to get branch {name}")
    def get_branch(self, name: str) -> Branch:
        """Get branch information."""
        branch = self._client.get_branch(self._repo.id, name=name, project=self._project)
        commit = self._client.get_commit(branch.commit.commit_id, self._repo.id)
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
        pr: GitPullRequest = self._client.get_pull_request_by_id(number, self._project)
        return azuretools.create_pull_request_model(pr)

    @azuretools.handle_azure_errors("Failed to list pull requests")
    def list_pull_requests(self, state: PullRequestState = "open") -> list[PullRequest]:
        """List pull requests."""
        # Map state to Azure DevOps status
        from azure.devops.v7_1.git.models import GitPullRequestSearchCriteria

        status_map = {"open": "active", "closed": "completed", "all": "all"}
        azure_status = status_map.get(state, "active")
        criteria = GitPullRequestSearchCriteria(status=azure_status)
        prs = self._client.get_pull_requests(
            self._repo.id, search_criteria=criteria, project=self._project
        )
        return [azuretools.create_pull_request_model(pr) for pr in prs]

    @azuretools.handle_azure_errors("Failed to get issue {issue_id}")
    def get_issue(self, issue_id: int) -> Issue:
        """Get issue by ID (work item in Azure DevOps)."""
        work_item = self._work_client.get_work_item(issue_id, self._project)
        return azuretools.create_issue_model(work_item)

    @azuretools.handle_azure_errors("Failed to list issues")
    def list_issues(self, state: IssueState = "open") -> list[Issue]:
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

    @azuretools.handle_azure_errors("Failed to create issue")
    def create_issue(
        self,
        title: str,
        body: str,
        labels: list[str] | None = None,
        assignees: list[str] | None = None,
    ) -> Issue:
        """Create a new issue."""
        doc = {"System.Title": title, "System.Description": body}
        work_item = self._work_client.create_work_item(doc, self._project, type="Issue")
        # if labels:
        #     for label in labels:
        #         self._work_client.add_work_item_label(
        #             work_item.id,
        #             label,
        #             project=self._project,
        #         )
        # if assignees:
        #     # Azure needs user ids instead of usernames
        #     for username in assignees:
        #         user = self._connection.identity_client.read_identities(
        #             search_filter=f"General,localAccount,{username}",
        #         )[0]
        #         self._work_client.update_work_item(
        #             work_item.id,
        #             [
        #                 {
        #                     "op": "add",
        #                     "path": "/fields/System.AssignedTo",
        #                     "value": user.id,
        #                 }
        #             ],
        #             project=self._project,
        #         )
        return azuretools.create_issue_model(work_item)

    @azuretools.handle_azure_errors("Failed to get commit {sha}")
    def get_commit(self, sha: str) -> Commit:
        """Get commit by SHA."""
        commit: GitCommit = self._client.get_commit(
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
        commits = self._client.get_commits(
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
            repo=self._repo.id,  # type: ignore
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
        items = self._client.get_items(
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

    # @azuretools.handle_azure_errors("Failed to get latest release")
    # def get_latest_release(
    #     self,
    #     include_drafts: bool = False,
    #     include_prereleases: bool = False,
    # ) -> Release:
    #     """Get latest release (mapped from Git tags)."""
    #     tags = self._client.get_tags(
    #         repository_id=self._repo.id,
    #         project=self._project,
    #     )
    #     if not tags:
    #         msg = "No releases found"
    #         raise ResourceNotFoundError(msg)

    #     latest_tags = sorted(tags, key=lambda t: t.commit.committer.date, reverse=True)
    #     latest_tag = latest_tags[0]
    #     return azuretools.create_release_model(latest_tag)

    @azuretools.handle_azure_errors("Failed to create pull request")
    def create_pull_request(
        self,
        title: str,
        body: str,
        head_branch: str,
        base_branch: str,
        draft: bool = False,
    ) -> PullRequest:
        """Create a pull request."""
        from azure.devops.v7_1.git.models import GitPullRequest

        request = GitPullRequest(
            source_ref_name=f"refs/heads/{head_branch}",
            target_ref_name=f"refs/heads/{base_branch}",
            title=title,
            description=body,
            is_draft=draft,
        )
        pr = self._client.create_pull_request(
            request,
            repository_id=self._repo.id,
            project=self._project,
        )
        return azuretools.create_pull_request_model(pr)

    @azuretools.handle_azure_errors("Failed to add pull request comment")
    def add_pull_request_comment(
        self,
        number: int,
        body: str,
    ) -> Comment:
        from azure.devops.v7_1.git.models import Comment, GitPullRequestCommentThread

        comment = Comment(content=body)
        thread = GitPullRequestCommentThread(comments=[comment], status="active")
        result = self._client.create_thread(
            comment_thread=thread,
            repository_id=self._repo.id,
            pull_request_id=number,
            project=self._project,
        )
        return azuretools.create_comment_model(result.comments[0])

    @azuretools.handle_azure_errors("Failed to add pull request review comment")
    def add_pull_request_review_comment(
        self,
        number: int,
        body: str,
        commit_id: str,
        path: str,
        position: int,
    ) -> Comment:
        from azure.devops.v7_1.git.models import (
            Comment,
            CommentThreadContext,
            GitPullRequestCommentThread,
        )

        ctx = CommentThreadContext(
            file_path=path,
            right_file_start={"line": position, "offset": 0},
            right_file_end={"line": position, "offset": 0},
        )
        thread = GitPullRequestCommentThread(
            comments=[Comment(content=body)],
            status="active",
            thread_context=ctx,
        )
        result = self._client.create_thread(
            comment_thread=thread,
            repository_id=self._repo.id,
            pull_request_id=number,
            project=self._project,
        )
        return azuretools.create_comment_model(result.comments[0])


if __name__ == "__main__":
    pass
