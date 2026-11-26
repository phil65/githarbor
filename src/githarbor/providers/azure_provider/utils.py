"""Azure DevOps helper functions and decorators."""

import annotationlib
import functools
import inspect
import logging
import os
import string
from typing import TYPE_CHECKING, Any, overload

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
    from upath.types import JoinablePathLike


logger = logging.getLogger(__name__)

TOKEN = os.getenv("AZURE_DEVOPS_PAT")


def handle_azure_errors[T, **P](
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

            sig = inspect.signature(func, annotation_format=annotationlib.Format.FORWARDREF)
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
    destination: JoinablePathLike,
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
    from upathtools import to_upath

    token = token or TOKEN
    if not token:
        msg = "Azure DevOps PAT token is required"
        raise ValueError(msg)

    dest = to_upath(destination)
    dest.mkdir(exist_ok=True, parents=True)
    credentials = BasicAuthentication("", token)
    url = f"https://dev.azure.com/{organization}"
    connection = Connection(base_url=url, creds=credentials)
    git_client = connection.clients.get_git_client()
    logger.info("Downloading files from Azure DevOps: %s", path)
    if recursive:
        msg = "Recursive download not yet implemented for Azure DevOps"
        raise NotImplementedError(msg)

    content = git_client.get_item_content(
        repository_id=repo,
        path=str(path),
        project=project,
        download=True,
    )
    data = content if isinstance(content, bytes) else b"".join(content)
    file_dest = dest / to_upath(path).name
    file_dest.write_bytes(data)


@overload
def create_user_model(azure_user: None) -> None: ...


@overload
def create_user_model(azure_user: Any) -> User: ...


def create_user_model(azure_user: Any) -> User | None:
    """Create User model from Azure DevOps identity dict, IdentityRef, or GitUserDate."""
    if not azure_user:
        return None
    if isinstance(azure_user, dict):
        return User(
            username=azure_user.get("uniqueName", "") or azure_user.get("displayName", ""),
            name=azure_user.get("displayName", ""),
            email=azure_user.get("mailAddress", ""),
            avatar_url=azure_user.get("imageUrl", ""),
        )
    # Handle IdentityRef (unique_name, display_name, image_url) and GitUserDate (name, email)
    display_name = getattr(azure_user, "display_name", None) or getattr(azure_user, "name", "")
    username = getattr(azure_user, "unique_name", None) or display_name or ""
    email = getattr(azure_user, "email", None) or getattr(azure_user, "mail_address", "")
    avatar_url = getattr(azure_user, "image_url", "") or ""
    return User(
        username=username,
        name=display_name or "",
        email=email or "",
        avatar_url=avatar_url,
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
        source_branch=(pr.source_ref_name or "").removeprefix("refs/heads/"),
        target_branch=(pr.target_ref_name or "").removeprefix("refs/heads/"),
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


def _parse_azure_datetime(value: Any) -> Any:
    """Parse Azure DevOps ISO datetime string if needed."""
    from datetime import datetime

    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


def create_issue_model(work_item: AzureWorkItem) -> Issue:
    """Create Issue model from Azure DevOps work item object."""
    fields = work_item.fields
    assert fields
    return Issue(
        number=work_item.id if work_item.id else -1,
        title=fields["System.Title"],
        description=fields.get("System.Description", ""),
        state=fields["System.State"],
        created_at=_parse_azure_datetime(fields.get("System.CreatedDate")),
        updated_at=_parse_azure_datetime(fields.get("System.ChangedDate")),
        closed_at=None,  # Not directly provided
        closed=fields["System.State"] in ["Closed", "Resolved"],
        author=create_user_model(fields.get("System.CreatedBy")),
        assignee=create_user_model(fields.get("System.AssignedTo")),
        labels=[],  # Would need additional processing
        url=work_item.url,
    )


def create_commit_model(commit: GitCommit) -> Commit:
    """Create Commit model from Azure DevOps commit object."""
    author = (
        User(
            username=commit.author.name,
            name=commit.author.name,
            email=commit.author.email,
        )
        if commit.author
        else None
    )
    committer = (
        User(
            username=commit.committer.name,
            name=commit.committer.name,
            email=commit.committer.email,
        )
        if commit.committer
        else None
    )
    counts = commit.change_counts
    if isinstance(counts, dict):
        additions = counts.get("Add", counts.get("add", 0))
        deletions = counts.get("Delete", counts.get("delete", 0))
        edits = counts.get("Edit", counts.get("edit", 0))
    else:
        additions = getattr(counts, "add", 0) if counts else 0
        deletions = getattr(counts, "delete", 0) if counts else 0
        edits = getattr(counts, "edit", 0) if counts else 0
    stats = {
        "additions": additions,
        "deletions": deletions,
        "total": additions + deletions + edits,
    }
    parents = [
        p if isinstance(p, str) else getattr(p, "commit_id", str(p)) for p in (commit.parents or [])
    ]
    return Commit(
        sha=commit.commit_id or "",
        message=commit.comment or "",
        created_at=commit.author.date if commit.author else None,
        author=author,
        committer=committer,
        url=commit.url,
        stats=stats,
        parents=parents,
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
