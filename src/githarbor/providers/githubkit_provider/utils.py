"""Tools for converting between GitHubKit and GitHarbor models."""

from __future__ import annotations

from datetime import datetime
import functools
import inspect
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, overload

from githarbor.core.models import (
    Asset,
    Branch,
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

    from githubkit.versions.latest import models as ghk_models
    from githubkit.versions.latest.models import ReleaseAsset


T = TypeVar("T")
P = ParamSpec("P")


def handle_githubkit_errors(
    error_msg_template: str,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        if not inspect.iscoroutinefunction(func):
            # For sync functions
            @functools.wraps(func)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:  # pyright: ignore
                from githubkit.exception import GitHubException

                try:
                    return func(*args, **kwargs)  # type: ignore
                except GitHubException as e:
                    msg = error_msg_template.format(error=str(e))
                    raise ResourceNotFoundError(msg) from e

        else:
            # For async functions
            @functools.wraps(func)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                from githubkit.exception import GitHubException

                try:
                    return await func(*args, **kwargs)  # type: ignore
                except GitHubException as e:
                    msg = error_msg_template.format(error=str(e))
                    raise ResourceNotFoundError(msg) from e

        return wrapper  # type: ignore

    return decorator


@overload
def create_user_model(ghk_user: None) -> None: ...


@overload
def create_user_model(
    ghk_user: ghk_models.SimpleUser
    | ghk_models.PrivateUser
    | ghk_models.PublicUser
    | ghk_models.GitUser,
) -> User: ...


def create_user_model(
    ghk_user: ghk_models.SimpleUser
    | ghk_models.PrivateUser
    | ghk_models.PublicUser
    | ghk_models.GitUser
    | None,
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
    if isinstance(ghk_user, ghk_models.GitUser):
        date = ghk_user.date
        return User(
            username=name or "unknown",
            name=name,
            email=email,
            created_at=datetime.fromisoformat(date) if date else None,
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


def create_pull_request_model(
    ghk_pr: ghk_models.PullRequest | ghk_models.PullRequestSimple,
) -> PullRequest:
    """Convert GitHubKit pull request model to GitHarbor PullRequest model."""
    from githubkit.versions.latest import models as ghk_models

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
        url=ghk_pr.html_url,
        merged_by=create_user_model(ghk_pr.merged_by)
        if isinstance(ghk_pr, ghk_models.PullRequest)
        else None,
        review_comments_count=ghk_pr.review_comments
        if isinstance(ghk_pr, ghk_models.PullRequest)
        else None,
        commits_count=ghk_pr.commits
        if isinstance(ghk_pr, ghk_models.PullRequest)
        else None,
        additions=ghk_pr.additions
        if isinstance(ghk_pr, ghk_models.PullRequest)
        else None,
        deletions=ghk_pr.deletions
        if isinstance(ghk_pr, ghk_models.PullRequest)
        else None,
        changed_files=ghk_pr.changed_files
        if isinstance(ghk_pr, ghk_models.PullRequest)
        else None,
        mergeable=ghk_pr.mergeable
        if isinstance(ghk_pr, ghk_models.PullRequest)
        else None,
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


@overload
def create_commit_model(ghk_commit: None) -> None: ...


@overload
def create_commit_model(
    ghk_commit: ghk_models.Commit | ghk_models.CommitSearchResultItemPropCommit,
) -> Commit: ...


def create_commit_model(
    ghk_commit: (ghk_models.Commit | ghk_models.CommitSearchResultItemPropCommit | None),
) -> Commit | None:
    """Create Commit model from GitHubKit commit object."""
    from githubkit.versions.latest import models as ghk_models

    if not ghk_commit:
        return None

    from githubkit.utils import UNSET

    # Handle CommitSearchResultItemPropCommit differently
    if isinstance(ghk_commit, ghk_models.CommitSearchResultItemPropCommit):
        return Commit(
            sha="",  # No sha in this type
            message=ghk_commit.message,
            author=User(
                username=ghk_commit.author.name or "",
                name=ghk_commit.author.name or "",
                email=ghk_commit.author.email or "",
            ),
            created_at=ghk_commit.author.date,
            committer=create_user_model(ghk_commit.committer)
            if ghk_commit.committer
            else None,
            url=ghk_commit.url,
            stats={},  # No stats in this type
            parents=[],  # No parents in this type
            verified=bool(
                ghk_commit.verification.verified
                if ghk_commit.verification is not UNSET and ghk_commit.verification
                else False
            ),
            files_changed=[],  # No files in this type
        )

    # Original handling for regular Commit type
    commit_data = ghk_commit.commit
    author = (
        create_user_model(ghk_commit.author)
        if not isinstance(ghk_commit.author, ghk_models.EmptyObject)
        else User(
            username=commit_data.author.name or "" if commit_data.author else "",
            name=commit_data.author.name or "" if commit_data.author else "",
            email=commit_data.author.email or "" if commit_data.author else "",
        )
    )

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
    created_at = (
        datetime.fromisoformat(commit_data.author.date)
        if commit_data.author
        and commit_data.author.date
        and commit_data.author.date is not UNSET
        else None
    )
    return Commit(
        sha=ghk_commit.sha,
        message=commit_data.message,
        author=author,
        created_at=created_at,
        committer=create_user_model(ghk_commit.committer)
        if not isinstance(ghk_commit.committer, ghk_models.EmptyObject)
        else None,
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
        assets=[create_asset_model(asset) for asset in ghk_release.assets],
        url=ghk_release.html_url,
        target_commitish=ghk_release.target_commitish,
    )


def create_asset_model(ghk_asset: ReleaseAsset) -> Asset:
    """Create Asset model from GitHubKit asset object."""
    from githubkit.utils import UNSET

    return Asset(
        name=ghk_asset.name,
        url=ghk_asset.browser_download_url,
        size=ghk_asset.size,
        download_count=ghk_asset.download_count,
        created_at=ghk_asset.created_at if ghk_asset.created_at is not UNSET else None,
        updated_at=ghk_asset.updated_at if ghk_asset.updated_at is not UNSET else None,
    )


def create_tag_model(ghk_tag: ghk_models.Tag | ghk_models.GitTag) -> Tag:
    """Convert GitHubKit tag model to GitHarbor Tag model."""
    from githubkit.versions.latest import models as ghk_models

    if isinstance(ghk_tag, ghk_models.Tag):
        return Tag(name=ghk_tag.name, sha=ghk_tag.commit.sha)
    return Tag(
        name=ghk_tag.tag,
        sha=ghk_tag.sha,
        url=ghk_tag.url,
        message=ghk_tag.message,
    )


def create_comment_model(ghk_comment: Any) -> Comment:
    """Create Comment model from GitHubKit comment object."""
    return Comment(
        id=str(ghk_comment.id),
        body=ghk_comment.body,
        author=create_user_model(ghk_comment.user),
        created_at=ghk_comment.created_at,
        updated_at=ghk_comment.updated_at,
        url=ghk_comment.html_url,
    )
