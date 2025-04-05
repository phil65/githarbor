from __future__ import annotations

import functools
import inspect
import logging
import os
import string
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

    from github.AuthenticatedUser import AuthenticatedUser
    from github.NamedUser import NamedUser


T = TypeVar("T")
P = ParamSpec("P")
TOKEN = os.getenv("GITHUB_TOKEN")
logger = logging.getLogger(__name__)


def handle_github_errors(
    error_msg_template: str,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator to handle GitHub API exceptions consistently.

    Args:
        error_msg_template: Message template with format placeholders

    Example:
        @handle_github_errors("Could not fetch branch {branch_name}")
        def get_branch(self, branch_name: str) -> Branch:
            ...
    """
    # Extract field names from the template string
    parser = string.Formatter()
    param_names = {
        field_name
        for _, field_name, _, _ in parser.parse(error_msg_template)
        if field_name and field_name != "error"
    }

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            from github.GithubException import GithubException

            # Extract parameter values from args/kwargs based on function signature
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
            except GithubException as e:
                msg = error_msg_template.format(**params, error=str(e))
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
    logger.info("Copying files from Github: %s", path)
    files = fs.ls(str(path))
    fs.get(files, dest.as_posix(), recursive=recursive)


@overload
def create_user_model(gh_user: None) -> None: ...


@overload
def create_user_model(gh_user: NamedUser | AuthenticatedUser) -> User: ...


def create_user_model(gh_user: NamedUser | AuthenticatedUser | None) -> User | None:
    """Create User model from GitHub user object."""
    from github.NamedUser import NamedUser

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
        blog=gh_user.blog,
        twitter_username=gh_user.twitter_username
        if isinstance(gh_user, NamedUser)
        else None,
        hireable=gh_user.hireable,
        gravatar_id=gh_user.gravatar_id,
    )


def create_label_model(gh_label: Any) -> Label:
    """Create Label model from GitHub label object."""
    return Label(
        name=gh_label.name,
        color=gh_label.color,
        description=gh_label.description or "",
        url=gh_label.url,
    )


def create_pull_request_model(pr: Any) -> PullRequest:
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
        author=create_user_model(pr.user),
        assignees=[create_user_model(a) for a in pr.assignees if a],
        labels=[create_label_model(lbl) for lbl in pr.labels],
        merged_by=create_user_model(pr.merged_by),
        review_comments_count=pr.review_comments,
        commits_count=pr.commits,
        additions=pr.additions,
        deletions=pr.deletions,
        changed_files=pr.changed_files,
        mergeable=pr.mergeable,
        url=pr.html_url,
    )


def create_issue_model(issue: Any) -> Issue:
    return Issue(
        number=issue.number,
        title=issue.title,
        description=issue.body or "",
        state=issue.state,
        created_at=issue.created_at,
        updated_at=issue.updated_at,
        closed_at=issue.closed_at,
        closed=issue.state == "closed",
        author=create_user_model(issue.user),
        assignee=create_user_model(issue.assignee),
        labels=[create_label_model(lbl) for lbl in issue.labels],
        comments_count=issue.comments,
        url=issue.html_url,
        milestone=issue.milestone.title if issue.milestone else None,
    )


def create_commit_model(commit: Any) -> Commit:
    return Commit(
        sha=commit.sha,
        message=commit.commit.message,
        created_at=commit.commit.author.date,
        author=create_user_model(commit.author)
        or User(
            username="",
            name=commit.commit.author.name,
            email=commit.commit.author.email,
        ),
        committer=create_user_model(commit.committer),
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


def create_release_model(release: Any) -> Release:
    author = (
        User(
            username=release.author.login,
            name=release.author.name,
            avatar_url=release.author.avatar_url,
        )
        if release.author
        else None
    )
    return Release(
        tag_name=release.tag_name,
        name=release.title,
        description=release.body or "",
        created_at=release.created_at,
        published_at=release.published_at,
        draft=release.draft,
        prerelease=release.prerelease,
        author=author,
        assets=[create_asset_model(asset) for asset in release.assets],
        url=release.html_url,
        target_commitish=release.target_commitish,
    )


def create_workflow_model(workflow: Any) -> Workflow:
    """Create Workflow model from GitHub workflow object."""
    # raw_prefix = f"https://raw.githubusercontent.com/{self._owner}/{self._name}/"
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
        # definition=f"{raw_prefix}{self.default_branch}/{workflow.path}",
    )


def create_workflow_run_model(run: Any) -> WorkflowRun:
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


def create_tag_model(tag: Any) -> Tag:
    """Create Tag model from GitHub tag object."""
    return Tag(
        name=tag.name,
        sha=tag.commit.sha if hasattr(tag, "commit") else tag.object.sha,
        message=tag.message if hasattr(tag, "message") else None,
        created_at=tag.tagger.date if hasattr(tag, "tagger") else None,
        author=create_user_model(tag.tagger) if hasattr(tag, "tagger") else None,
        url=tag.url if hasattr(tag, "url") else None,
    )


def create_branch_model(branch: Any) -> Branch:
    """Create Branch model from GitHub branch object."""
    last_commit = branch.commit
    rules = (
        {
            "required_reviews": branch.get_required_status_checks(),
            "dismiss_stale_reviews": branch.get_required_pull_request_reviews(),
            "require_code_owner_reviews": branch.get_required_signatures(),
        }
        if branch.protected
        else None
    )
    return Branch(
        name=branch.name,
        sha=branch.commit.sha,
        protected=branch.protected,
        protection_rules=rules,
        last_commit_date=last_commit.commit.author.date,
        last_commit_message=last_commit.commit.message,
        last_commit_author=create_user_model(last_commit.author),
    )


def create_file_model(content: Any) -> dict[str, Any]:
    """Create file info dictionary from GitHub content object."""
    return {
        "name": content.name,
        "path": content.path,
        "sha": content.sha,
        "size": content.size,
        "type": content.type,
        "url": content.html_url,
        "download_url": content.download_url,
        "encoding": content.encoding if hasattr(content, "encoding") else None,
    }


def create_comment_model(gh_comment: Any) -> Comment:
    """Create Comment model from GitHub comment object."""
    return Comment(
        id=str(gh_comment.id),
        body=gh_comment.body,
        author=create_user_model(gh_comment.user),
        created_at=gh_comment.created_at,
        updated_at=gh_comment.updated_at,
        url=gh_comment.html_url,
    )


def create_asset_model(gh_asset: Any) -> Asset:
    """Create Asset model from GitHub asset object."""
    return Asset(
        name=gh_asset.name,
        url=gh_asset.browser_download_url,
        size=gh_asset.size,
        download_count=gh_asset.download_count,
        created_at=gh_asset.created_at,
        updated_at=gh_asset.updated_at,
    )
