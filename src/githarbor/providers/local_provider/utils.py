"""Module containing helper functions for local repository operations."""

from __future__ import annotations

import functools
import inspect
import string
from typing import TYPE_CHECKING, ParamSpec, TypeVar

from githarbor.core.models import Branch, Commit, Tag, User
from githarbor.exceptions import ResourceNotFoundError


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from datetime import datetime

    import git


T = TypeVar("T")
P = ParamSpec("P")


def handle_git_errors(
    error_msg_template: str,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator to handle Git operation exceptions consistently.

    Args:
        error_msg_template: Message template with format placeholders

    Example:
        @handle_git_errors("Could not fetch branch {branch_name}")
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
            from git.exc import GitError
            from gitdb.exc import ODBError

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
            except (GitError, ODBError) as e:
                msg = error_msg_template.format(**params, error=str(e))
                raise ResourceNotFoundError(msg) from e

        return wrapper

    return decorator


def create_branch_model(branch: git.Reference, is_default: bool = False) -> Branch:
    """Create Branch model from GitPython branch reference.

    Args:
        branch: GitPython branch reference
        is_default: Whether this is the default branch

    Returns:
        Branch model instance
    """
    from datetime import UTC, datetime

    commit = branch.commit
    return Branch(
        name=branch.name,
        sha=commit.hexsha,
        protected=False,  # Local repos don't have branch protection
        default=is_default,
        last_commit_date=datetime.fromtimestamp(commit.committed_date, UTC),
        last_commit_message=commit.message,
        last_commit_author=create_user_model(commit.author),
    )


def create_commit_model(commit: git.Commit) -> Commit:
    """Create Commit model from GitPython commit.

    Args:
        commit: GitPython commit object

    Returns:
        Commit model instance
    """
    from datetime import UTC, datetime

    msg = commit.message if isinstance(commit.message, str) else commit.message.decode()
    return Commit(
        sha=commit.hexsha,
        message=msg,
        author=create_user_model(commit.author),
        created_at=datetime.fromtimestamp(commit.committed_date, UTC),
        committer=create_user_model(commit.committer),
        stats=get_commit_stats(commit),
        parents=[c.hexsha for c in commit.parents],
        files_changed=[diff.a_path for diff in commit.diff() if diff.a_path],
    )


def create_tag_model(
    tag: git.Tag | git.TagObject,
    commit: git.Commit,
) -> Tag:
    """Create Tag model from GitPython tag.

    Args:
        tag: GitPython tag or tag object
        commit: Associated commit

    Returns:
        Tag model instance
    """
    from datetime import UTC, datetime

    import git

    # Get author info if tag is annotated
    author = None
    created_at = None
    message = None

    if isinstance(tag, git.TagObject):
        author = create_user_model(tag.tagger)
        created_at = datetime.fromtimestamp(tag.tagged_date, UTC)
        message = tag.message

    return Tag(
        name=tag.name if hasattr(tag, "name") else tag.tag,  # type: ignore
        sha=commit.hexsha,
        message=message,
        created_at=created_at,
        author=author,
    )


def create_user_model(git_actor: git.Actor) -> User:
    """Create User model from GitPython actor.

    Args:
        git_actor: GitPython actor object

    Returns:
        User model instance or None if no actor
    """
    return User(username=git_actor.name, name=git_actor.name, email=git_actor.email)  # type: ignore


def get_commit_stats(commit: git.Commit) -> dict[str, int]:
    """Extract statistics from a commit.

    Args:
        commit: GitPython commit object

    Returns:
        Dictionary with commit statistics
    """
    return {
        "additions": commit.stats.total["insertions"],
        "deletions": commit.stats.total["deletions"],
        "total": commit.stats.total["lines"],
    }


def filter_commits(
    commits: Iterator[git.Commit],
    since: datetime | None = None,
    until: datetime | None = None,
    author: str | None = None,
    path: str | None = None,
    max_results: int | None = None,
) -> list[git.Commit]:
    """Filter commits based on criteria.

    Args:
        commits: Iterator of commits
        since: Include commits after this date
        until: Include commits before this date
        author: Filter by author name/email
        path: Filter by file path
        max_results: Maximum number of commits to return

    Returns:
        List of filtered commits
    """
    from datetime import UTC, datetime

    filtered = []
    for commit in commits:
        commit_date = datetime.fromtimestamp(commit.committed_date, UTC)

        if since and commit_date < since:
            continue
        if until and commit_date > until:
            continue
        if author and author not in (commit.author.name, commit.author.email):
            continue
        if path and not any(path in f.path for f in commit.stats.files):  # type: ignore
            continue

        filtered.append(commit)
        if max_results and len(filtered) >= max_results:
            break

    return filtered
