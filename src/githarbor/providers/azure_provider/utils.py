"""Azure DevOps helper functions and decorators."""

from __future__ import annotations

import functools
import inspect
import logging
import os
import string
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, overload

from githarbor.core.models import (
    Comment,
    Commit,
    Issue,
    Label,
    PullRequest,
    Release,
    Tag,
    User,
    Workflow,
    WorkflowRun,
)
from githarbor.exceptions import ResourceNotFoundError


if TYPE_CHECKING:
    from collections.abc import Callable

    from azure.devops.v7_1.git.models import GitCommit, GitPullRequest
    from azure.devops.v7_1.work_item_tracking.models import WorkItem as AzureWorkItem


logger = logging.getLogger(__name__)
T = TypeVar("T")
P = ParamSpec("P")
TOKEN = os.getenv("AZURE_DEVOPS_PAT")


def handle_azure_errors(
    error_msg_template: str,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator to handle Azure DevOps API exceptions consistently.

    Args:
        error_msg_template: Message template with format placeholders

    Example:
        @handle_azure_errors("Could not fetch branch {branch_name}")
        def get_branch(self, branch_name: str) -> Branch:
            ...
    """
    parser = string.Formatter()
    param_names = {
        field_name
        for _, field_name, _, _ in parser.parse(error_msg_template)
        if field_name and field_name != "error"
    }

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            from azure.devops.exceptions import AzureDevOpsServiceError

            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            params = {
                name: bound_args.arguments[name]
                for name in param_names
                if name in bound_args.arguments
            }

            try:
                return func(*args, **kwargs)
            except AzureDevOpsServiceError as e:
                msg = error_msg_template.format(**params, error=str(e))
                raise ResourceNotFoundError(msg) from e

        return wrapper

    return decorator


def download_from_azure(
    organization: str,
    project: str,
    repo: str,
    path: str | os.PathLike[str],
    destination: str | os.PathLike[str],
    token: str | None = None,
    recursive: bool = False,
):
    """Download files from Azure DevOps repository.

    Args:
        organization: Azure DevOps organization name
        project: Project name
        repo: Repository name
        path: Path to file/folder to download
        destination: Local destination path
        token: Personal access token
        recursive: Whether to download recursively
    """
    from azure.devops.connection import Connection
    from msrest.authentication import BasicAuthentication
    import upath

    token = token or TOKEN
    if not token:
        msg = "Azure DevOps PAT token is required"
        raise ValueError(msg)

    dest = upath.UPath(destination)
    dest.mkdir(exist_ok=True, parents=True)
    credentials = BasicAuthentication("", token)
    url = f"https://dev.azure.com/{organization}"
    connection = Connection(base_url=url, creds=credentials)
    git_client = connection.clients.get_git_client()
    logger.info("Downloading files from Azure DevOps: %s", path)
    content = git_client.get_item_content(
        repository_id=repo,
        path=str(path),
        project=project,
        download=True,
    )

    if recursive:
        msg = "Recursive download not yet implemented for Azure DevOps"
        raise NotImplementedError(msg)

    file_dest = dest / upath.UPath(path).name
    file_dest.write_bytes(content)


@overload
def create_user_model(azure_user: None) -> None: ...


@overload
def create_user_model(azure_user: Any) -> User: ...


def create_user_model(azure_user: Any) -> User | None:
    """Create User model from Azure DevOps identity object."""
    if not azure_user:
        return None
    return User(
        username=azure_user.get("uniqueName", ""),
        name=azure_user.get("displayName", ""),
        email=azure_user.get("mailAddress", ""),
        avatar_url=azure_user.get("imageUrl", ""),
    )


def create_label_model(azure_label: Any) -> Label:
    """Create Label model from Azure DevOps label object."""
    return Label(
        name=azure_label.name,
        color=azure_label.color or "000000",
        description=azure_label.description or "",
        url="",  # Azure DevOps doesn't provide direct URLs for labels
    )


def create_pull_request_model(pr: GitPullRequest) -> PullRequest:
    """Create PullRequest model from Azure DevOps pull request object."""
    return PullRequest(
        number=pr.pull_request_id or 0,
        title=pr.title or "unknown",
        description=pr.description or "",
        state=pr.status,
        source_branch=pr.source_ref_name.split("/")[-1],  # type: ignore
        target_branch=pr.target_ref_name.split("/")[-1],  # type: ignore
        created_at=pr.creation_date,
        updated_at=None,  # Not directly provided by Azure DevOps
        merged_at=pr.closed_date if pr.status == "completed" else None,
        closed_at=pr.closed_date if pr.status in ["abandoned", "completed"] else None,
        author=create_user_model(pr.created_by),
        assignees=[create_user_model(r) for r in pr.reviewers or []],
        labels=[],  # Would need additional processing
        merged_by=create_user_model(pr.closed_by) if pr.closed_by else None,
        url=pr.url,
    )


def create_issue_model(work_item: AzureWorkItem) -> Issue:
    """Create Issue model from Azure DevOps work item object."""
    fields = work_item.fields
    return Issue(
        number=work_item.id,
        title=fields["System.Title"],
        description=fields.get("System.Description", ""),
        state=fields["System.State"],
        created_at=fields["System.CreatedDate"],
        updated_at=fields.get("System.ChangedDate"),
        closed_at=None,  # Not directly provided
        closed=fields["System.State"] in ["Closed", "Resolved"],
        author=create_user_model(fields.get("System.CreatedBy")),
        assignee=create_user_model(fields.get("System.AssignedTo")),
        labels=[],  # Would need additional processing
        url=work_item.url,
    )


def create_commit_model(commit: GitCommit) -> Commit:
    """Create Commit model from Azure DevOps commit object."""
    author = User(
        username=commit.author.name,
        name=commit.author.name,
        email=commit.author.email,
    )
    committer = User(
        username=commit.committer.name,
        name=commit.committer.name,
        email=commit.committer.email,
    )
    stats = {
        "additions": commit.change_counts.add if commit.change_counts else 0,
        "deletions": commit.change_counts.delete if commit.change_counts else 0,
        "total": commit.change_counts.edit if commit.change_counts else 0,
    }
    return Commit(
        sha=commit.commit_id,
        message=commit.comment,
        created_at=commit.author.date,
        author=author,
        committer=committer,
        url=commit.url,
        stats=stats,
        parents=[p.commit_id for p in commit.parents] if commit.parents else [],
    )


def create_release_model(tag: Any) -> Release:
    """Create Release model from Azure DevOps tag object."""
    author = User(
        username=tag.commit.committer.name,
        name=tag.commit.committer.name,
        email=tag.commit.committer.email,
    )
    return Release(
        tag_name=tag.name,
        name=tag.name,
        description=tag.message or "",
        created_at=tag.commit.committer.date,
        published_at=tag.commit.committer.date,
        draft=False,  # Azure doesn't have draft concept for tags
        prerelease=False,  # Azure doesn't have prerelease concept for tags
        author=author,
        url=None,  # Azure doesn't provide direct URLs for tags
        target_commitish=tag.commit.commit_id,
        assets=[],  # Azure tags don't have assets
    )


def create_workflow_model(definition: Any) -> Workflow:
    """Create Workflow model from Azure DevOps build definition object."""
    return Workflow(
        id=str(definition.id),
        name=definition.name,
        path=definition.path or "",
        state="enabled" if definition.quality == "enabled" else "disabled",
        created_at=None,  # Not provided by Azure DevOps
        updated_at=None,  # Not provided by Azure DevOps
        description=definition.description or "",
        triggers=[t.type for t in definition.triggers] if definition.triggers else [],
        disabled=definition.quality != "enabled",
        badge_url=None,  # Not directly provided
    )


def create_workflow_run_model(build: Any) -> WorkflowRun:
    """Create WorkflowRun model from Azure DevOps build object."""
    return WorkflowRun(
        id=str(build.id),
        name=build.definition.name,
        workflow_id=str(build.definition.id),
        status=build.status,
        conclusion=build.result or "",
        branch=build.source_branch.split("/")[-1],
        commit_sha=build.source_version,
        url=build.url,
        created_at=build.queue_time,
        updated_at=None,  # Not directly provided
        started_at=build.start_time,
        completed_at=build.finish_time,
        run_number=build.build_number,
        jobs_count=None,  # Would need additional API calls
        logs_url=build.logs.url if build.logs else None,
    )


def create_tag_model(tag: Any) -> Tag:
    author = User(
        username=tag.commit.committer.name,
        name=tag.commit.committer.name,
        email=tag.commit.committer.email,
    )
    return Tag(
        name=tag.name,
        sha=tag.commit.commit_id,
        message=tag.message or "",
        created_at=tag.commit.committer.date,
        author=author,
        url=None,  # Azure DevOps doesn't provide direct URLs for tags
    )


def create_comment_model(azure_comment: Any) -> Comment:
    """Create Comment model from Azure DevOps comment object."""
    return Comment(
        id=str(azure_comment.id),
        body=azure_comment.content,
        author=create_user_model(azure_comment.author),
        created_at=azure_comment.published_date,
        updated_at=azure_comment.last_updated_date,
        url=None,  # Azure doesn't provide direct URLs for comments
    )
