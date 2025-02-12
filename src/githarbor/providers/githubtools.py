from __future__ import annotations

from dataclasses import dataclass
import functools
import inspect
import logging
import os
import string
from typing import TYPE_CHECKING, Any, Literal, ParamSpec, TypeVar, overload

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
from githarbor.exceptions import GitHarborError, ResourceNotFoundError


if TYPE_CHECKING:
    from collections.abc import Callable

    from github.NamedUser import NamedUser
    from github.Repository import Repository


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
def create_user_model(gh_user: NamedUser) -> User: ...


def create_user_model(gh_user: NamedUser | None) -> User | None:
    """Create User model from GitHub user object."""
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
        twitter_username=gh_user.twitter_username,
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
    return Release(
        tag_name=release.tag_name,
        name=release.title,
        description=release.body or "",
        created_at=release.created_at,
        published_at=release.published_at,
        draft=release.draft,
        prerelease=release.prerelease,
        author=User(
            username=release.author.login,
            name=release.author.name,
            avatar_url=release.author.avatar_url,
        )
        if release.author
        else None,
        assets=[
            {
                "name": asset.name,
                "url": asset.browser_download_url,
                "size": asset.size,
                "download_count": asset.download_count,
                "created_at": asset.created_at,
                "updated_at": asset.updated_at,
            }
            for asset in release.assets
        ],
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
    return Branch(
        name=branch.name,
        sha=branch.commit.sha,
        protected=branch.protected,
        default=False,  # This needs to be set by the caller
        protection_rules=(
            {
                "required_reviews": branch.get_required_status_checks(),
                "dismiss_stale_reviews": branch.get_required_pull_request_reviews(),
                "require_code_owner_reviews": branch.get_required_signatures(),
            }
            if branch.protected
            else None
        ),
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


@dataclass
class FileChange:
    """Represents a file change in a diff."""

    path: str
    content: str | None  # None means file deletion
    mode: Literal["add", "modify", "delete"]
    old_path: str | None = None  # For renamed files


def parse_diff(diff_str: str) -> list[FileChange]:
    """Parse a unified diff string into a list of file changes.

    Uses the unidiff library for robust diff parsing.

    Args:
        diff_str: Unified diff string

    Returns:
        List of FileChange objects
    """
    import unidiff

    patch_set = unidiff.PatchSet(diff_str.splitlines(keepends=True))
    changes: list[FileChange] = []

    for patched_file in patch_set:
        if patched_file.is_rename:
            # Handle renamed files
            changes.append(
                FileChange(
                    path=patched_file.target_file,
                    old_path=patched_file.source_file,
                    content=patched_file.target_file[1:],  # Remove leading /
                    mode="modify",
                )
            )
            continue

        # Determine the change type
        if patched_file.is_added_file:
            mode = "add"
        elif patched_file.is_removed_file:
            mode = "delete"
        else:
            mode = "modify"

        # For deletions, we don't need content
        if mode == "delete":
            change = FileChange(path=patched_file.path, content=None, mode="delete")
            changes.append(change)
            continue
        # Reconstruct the final content
        lines = [ln.value for hunk in patched_file for ln in hunk if not ln.is_removed]
        change = FileChange(patched_file.path, content="".join(lines), mode=mode)  # type: ignore
        changes.append(change)

    return changes


def create_pull_request_from_diff(
    repo: Repository,
    base_branch: str,
    head_branch: str,
    title: str,
    body: str,
    diff: str,
) -> dict[str, Any]:
    """Create a pull request from a diff string.

    Uses the unidiff library for robust diff parsing.

    Args:
        repo: GitHub repository object
        base_branch: Target branch for the PR
        head_branch: Source branch for the PR
        title: Pull request title
        body: Pull request description
        diff: Diff as a string

    Returns:
        Dictionary with status and url/error message

    Raises:
        GitHarborError: If PR creation fails
    """
    from github import InputGitTreeElement
    from github.GithubException import GithubException

    try:
        # Get the base branch's last commit
        base_ref = repo.get_git_ref(f"heads/{base_branch}")
        base_commit = repo.get_git_commit(base_ref.object.sha)

        # Create a new branch
        try:
            head_ref = repo.get_git_ref(f"heads/{head_branch}")
            msg = f"Branch {head_branch} already exists"
            raise GitHarborError(msg)
        except GithubException:
            ref = f"refs/heads/{head_branch}"
            head_ref = repo.create_git_ref(ref=ref, sha=base_ref.object.sha)

        # Parse the diff and apply changes
        changes = parse_diff(diff)

        # Create blobs and trees for the changes
        new_tree: list[InputGitTreeElement] = []
        for change in changes:
            if change.mode == "delete":
                # For deletions, we add a null SHA
                elem = InputGitTreeElement(
                    path=change.path,
                    mode="100644",
                    type="blob",
                    sha=None,
                )
                new_tree.append(elem)
                continue

            if change.content is not None:
                # Create blob for the file content
                blob = repo.create_git_blob(content=change.content, encoding="utf-8")

                if change.old_path:
                    # For renamed files, we need to remove the old path
                    elem = InputGitTreeElement(
                        path=change.old_path,
                        mode="100644",
                        type="blob",
                        sha=None,
                    )
                    new_tree.append(elem)
                elem = InputGitTreeElement(
                    path=change.path,
                    mode="100644",
                    type="blob",
                    sha=blob.sha,
                )
                new_tree.append(elem)

        # Create a new tree
        base_tree = repo.get_git_tree(base_commit.tree.sha)
        tree = repo.create_git_tree(new_tree, base_tree)

        # Create a commit
        msg = f"Changes for {title}"
        commit = repo.create_git_commit(msg, tree=tree, parents=[base_commit])

        # Update the reference
        head_ref.edit(commit.sha, force=True)

        # Create the pull request
        pr = repo.create_pull(
            title=title,
            body=body,
            base=base_branch,
            head=head_branch,
        )
    except Exception as e:
        msg = f"Failed to create pull request: {e!s}"
        raise GitHarborError(msg) from e
    else:
        return {"status": "success", "url": pr.html_url, "number": pr.number}
