"""Tools for converting between GitHubKit and GitHarbor models."""

from __future__ import annotations

import functools
import inspect
import string
from typing import TYPE_CHECKING, ParamSpec, TypeVar, overload

from githarbor.core.models import (
    Branch,
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

    from githubkit.versions.latest import models as ghk_models

T = TypeVar("T")
P = ParamSpec("P")


def handle_githubkit_errors(
    error_msg_template: str,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator to handle GitHubKit API exceptions consistently."""
    parser = string.Formatter()
    param_names = {
        field_name
        for _, field_name, _, _ in parser.parse(error_msg_template)
        if field_name and field_name != "error"
    }

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            from githubkit.exception import GitHubException

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
            except GitHubException as e:
                msg = error_msg_template.format(**params, error=str(e))
                raise ResourceNotFoundError(msg) from e

        return wrapper

    return decorator


@overload
def create_user_model(ghk_user: None) -> None: ...


@overload
def create_user_model(
    ghk_user: ghk_models.SimpleUser | ghk_models.PrivateUser,
) -> User: ...


def create_user_model(
    ghk_user: ghk_models.SimpleUser | ghk_models.PrivateUser | None,
) -> User | None:
    """Convert GitHubKit user model to GitHarbor User model."""
    from githubkit.utils import UNSET
    from githubkit.versions.latest import models as ghk_models

    if not ghk_user:
        return None

    # Handle potentially UNSET values
    name = (
        ghk_user.name
        if hasattr(ghk_user, "name") and ghk_user.name is not UNSET
        else None
    )
    email = (
        ghk_user.email
        if hasattr(ghk_user, "email") and ghk_user.email is not UNSET
        else None
    )

    user = User(
        username=ghk_user.login,
        name=name,
        email=email,
        avatar_url=ghk_user.avatar_url,
        url=ghk_user.html_url,
        gravatar_id=ghk_user.gravatar_id,
    )

    if isinstance(ghk_user, ghk_models.PrivateUser):
        user.created_at = ghk_user.created_at
        user.bio = ghk_user.bio if ghk_user.bio is not UNSET else None
        user.location = ghk_user.location if ghk_user.location is not UNSET else None
        user.company = ghk_user.company if ghk_user.company is not UNSET else None
        user.followers = ghk_user.followers
        user.following = ghk_user.following
        user.public_repos = ghk_user.public_repos
        user.blog = ghk_user.blog if ghk_user.blog is not UNSET else None
        user.twitter_username = (
            ghk_user.twitter_username if ghk_user.twitter_username is not UNSET else None
        )
        user.hireable = ghk_user.hireable if ghk_user.hireable is not UNSET else None
        user.is_admin = ghk_user.site_admin
        user.last_activity_on = ghk_user.updated_at

    return user


def create_label_model(ghk_label: ghk_models.IssuePropLabelsItemsOneof1) -> Label:
    """Convert GitHubKit label model to GitHarbor Label model."""
    return Label(
        name=ghk_label.name or "",
        color=ghk_label.color or "",
        description=ghk_label.description or "",
        url=ghk_label.url,  # type: ignore
    )


def create_branch_model(
    ghk_branch: ghk_models.BranchShort | ghk_models.BranchWithProtection,
    *,
    is_default: bool = False,
) -> Branch:
    """Convert GitHubKit branch model to GitHarbor Branch model."""
    from githubkit.versions.latest import models as ghk_models

    if isinstance(ghk_branch, ghk_models.BranchWithProtection):
        return Branch(
            name=ghk_branch.name,
            sha=ghk_branch.commit.sha,
            protected=ghk_branch.protected,
            default=is_default,
            protection_rules={"url": ghk_branch.protection_url}
            if ghk_branch.protected
            else None,
        )
    return Branch(
        name=ghk_branch.name,
        sha=ghk_branch.commit.sha,
        protected=ghk_branch.protected,
        default=is_default,
    )


def create_workflow_model(ghk_workflow: ghk_models.Workflow) -> Workflow:
    """Convert GitHubKit workflow model to GitHarbor Workflow model."""
    return Workflow(
        id=str(ghk_workflow.id),
        name=ghk_workflow.name,
        path=ghk_workflow.path,
        state=ghk_workflow.state,
        created_at=ghk_workflow.created_at,
        updated_at=ghk_workflow.updated_at,
        badge_url=ghk_workflow.badge_url,
    )


def create_workflow_run_model(ghk_run: ghk_models.WorkflowRun) -> WorkflowRun:
    """Convert GitHubKit workflow run model to GitHarbor WorkflowRun model."""
    from githubkit.utils import UNSET

    return WorkflowRun(
        id=str(ghk_run.id),
        name=ghk_run.name or ghk_run.display_title,
        workflow_id=str(ghk_run.workflow_id),
        status=ghk_run.status or "",
        conclusion=ghk_run.conclusion or "",
        branch=ghk_run.head_branch,
        commit_sha=ghk_run.head_sha,
        url=ghk_run.html_url,
        created_at=ghk_run.created_at,
        updated_at=ghk_run.updated_at,
        started_at=None
        if ghk_run.run_started_at is UNSET or not ghk_run.run_started_at
        else ghk_run.run_started_at,
        completed_at=None,  # Not directly available
        run_number=ghk_run.run_number,
        jobs_count=None,  # Would need additional API call
        logs_url=ghk_run.logs_url,
    )


def create_pull_request_model(ghk_pr: ghk_models.PullRequest) -> PullRequest:
    """Convert GitHubKit pull request model to GitHarbor PullRequest model."""
    return PullRequest(
        number=ghk_pr.number,
        title=ghk_pr.title,
        description=ghk_pr.body or "",
        state=ghk_pr.state,
        source_branch=ghk_pr.head.ref,
        target_branch=ghk_pr.base.ref,
        created_at=ghk_pr.created_at,
        updated_at=ghk_pr.updated_at,
        merged_at=ghk_pr.merged_at,
        closed_at=ghk_pr.closed_at,
        author=create_user_model(ghk_pr.user),
        assignees=[create_user_model(a) for a in (ghk_pr.assignees or []) if a],
        labels=[
            # create_label_model(label) for label in ghk_pr.labels if hasattr(label, "id")
        ],
        merged_by=create_user_model(ghk_pr.merged_by),
        review_comments_count=ghk_pr.review_comments,
        commits_count=ghk_pr.commits,
        additions=ghk_pr.additions,
        deletions=ghk_pr.deletions,
        changed_files=ghk_pr.changed_files,
        mergeable=ghk_pr.mergeable,
        url=ghk_pr.html_url,
    )


def create_issue_model(ghk_issue: ghk_models.Issue) -> Issue:
    """Convert GitHubKit issue model to GitHarbor Issue model."""
    return Issue(
        number=ghk_issue.number,
        title=ghk_issue.title,
        description=ghk_issue.body or "",
        state=ghk_issue.state,
        created_at=ghk_issue.created_at,
        updated_at=ghk_issue.updated_at,
        closed_at=ghk_issue.closed_at,
        closed=ghk_issue.state == "closed",
        author=create_user_model(ghk_issue.user),
        assignee=create_user_model(ghk_issue.assignee),
        labels=[
            # create_label_model(label)
            # for label in ghk_issue.labels
            # if hasattr(label, "id")
        ],
        comments_count=ghk_issue.comments,
        url=ghk_issue.html_url,
        milestone=ghk_issue.milestone.title if ghk_issue.milestone else None,
    )


def create_commit_model(ghk_commit: ghk_models.Commit) -> Commit:
    """Convert GitHubKit commit model to GitHarbor Commit model."""
    from githubkit.utils import UNSET
    from githubkit.versions.latest import models as ghk_models

    commit_data = ghk_commit.commit

    # Get author - must be non-None
    author = (
        create_user_model(ghk_commit.author)
        if not isinstance(ghk_commit.author, ghk_models.EmptyObject)
        else User(
            username=commit_data.author.name or "" if commit_data.author else "",
            name=commit_data.author.name or "" if commit_data.author else "",
            email=commit_data.author.email or "" if commit_data.author else "",
        )
    )

    # Get stats carefully
    stats = ghk_commit.stats
    stats_dict = {
        "additions": 0,
        "deletions": 0,
        "total": 0,
    }
    if stats and stats is not UNSET:
        if stats.additions is not UNSET:
            stats_dict["additions"] = stats.additions or 0
        if stats.deletions is not UNSET:
            stats_dict["deletions"] = stats.deletions or 0
        if stats.total is not UNSET:
            stats_dict["total"] = stats.total or 0

    return Commit(
        sha=ghk_commit.sha,
        message=commit_data.message,
        author=author,  # type: ignore
        created_at=commit_data.author.date if commit_data.author else None,  # type: ignore
        committer=create_user_model(ghk_commit.committer),  # type: ignore
        url=ghk_commit.html_url,
        stats=stats_dict,
        parents=[p.sha for p in ghk_commit.parents],
        verified=bool(
            commit_data.verification.verified
            if commit_data.verification is not UNSET and commit_data.verification
            else False
        ),
        files_changed=[f.filename for f in (ghk_commit.files or [])],
    )


def create_release_model(ghk_release: ghk_models.Release) -> Release:
    """Convert GitHubKit release model to GitHarbor Release model."""
    return Release(
        tag_name=ghk_release.tag_name,
        name=ghk_release.name or ghk_release.tag_name,
        description=ghk_release.body or "",
        created_at=ghk_release.created_at,
        published_at=ghk_release.published_at,
        draft=ghk_release.draft,
        prerelease=ghk_release.prerelease,
        author=create_user_model(ghk_release.author),
        assets=[
            {
                "name": asset.name,
                "url": asset.browser_download_url,
                "size": asset.size,
                "download_count": asset.download_count,
            }
            for asset in ghk_release.assets
        ],
        url=ghk_release.html_url,
        target_commitish=ghk_release.target_commitish,
    )


def create_tag_model(ghk_tag: ghk_models.Tag) -> Tag:
    """Convert GitHubKit tag model to GitHarbor Tag model."""
    return Tag(
        name=ghk_tag.name,
        sha=ghk_tag.commit.sha,
        url=None,  # Not available in the Tag model
    )
