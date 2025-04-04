"""Module containing local repository implementation."""

from __future__ import annotations

import fnmatch
import functools
import pathlib
from typing import TYPE_CHECKING, Any, ClassVar

from githarbor.core.base import BaseRepository
from githarbor.exceptions import ResourceNotFoundError
from githarbor.providers.local_provider import utils as localtools


if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime
    import os

    from githarbor.core.models import Branch, Commit, Tag
    from githarbor.core.proxy import Repository


class LocalRepository(BaseRepository):
    """Local Git repository implementation using GitPython."""

    url_patterns: ClassVar[list[str]] = []  # Local repos don't have URL patterns

    def __init__(self, path: str | os.PathLike[str]) -> None:
        import git

        try:
            self.path = pathlib.Path(path)
            self.repo = git.Repo(self.path)
            self._name = self.path.name
            self._owner = self.path.parent.name  # or None?
        except (git.InvalidGitRepositoryError, git.NoSuchPathError) as e:
            msg = f"Not a valid git repository: {path}"
            raise ResourceNotFoundError(msg) from e

    @classmethod
    def from_url(cls, url: str, **_: Any) -> LocalRepository:
        return cls(url)

    @classmethod
    def supports_url(cls, url: str) -> bool:
        return pathlib.Path(url).exists()

    @property
    def default_branch(self) -> str:
        return self.repo.active_branch.name

    @localtools.handle_git_errors("Failed to get branch {name}")
    def get_branch(self, name: str) -> Branch:
        branch = self.repo.heads[name]
        is_default = branch.name == self.default_branch
        return localtools.create_branch_model(branch, is_default=is_default)

    @localtools.handle_git_errors("Failed to list branches")
    def list_branches(self) -> list[Branch]:
        return [
            localtools.create_branch_model(b, is_default=b.name == self.default_branch)
            for b in self.repo.heads
        ]

    @localtools.handle_git_errors("Failed to get commit {sha}")
    def get_commit(self, sha: str) -> Commit:
        commit = self.repo.commit(sha)
        return localtools.create_commit_model(commit)

    @localtools.handle_git_errors("Failed to list commits")
    def list_commits(
        self,
        branch: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        author: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        commits = self.repo.iter_commits(rev=branch or self.default_branch)
        filtered = localtools.filter_commits(
            commits,
            since=since,
            until=until,
            author=author,
            path=path,
            max_results=max_results,
        )
        return [localtools.create_commit_model(commit) for commit in filtered]

    @localtools.handle_git_errors("Failed to iterate files")
    def iter_files(
        self,
        path: str = "",
        ref: str | None = None,
        pattern: str | None = None,
    ) -> Iterator[str]:
        tree = self.repo.head.commit.tree if ref is None else self.repo.commit(ref).tree
        for blob in tree.traverse():
            # Skip dirs
            if blob.type != "blob":  # type: ignore
                continue
            file_path = str(blob.path)  # type: ignore
            if (not pattern or fnmatch.fnmatch(file_path, pattern)) and (
                not path or file_path.startswith(path)
            ):
                yield file_path

    @localtools.handle_git_errors("Failed to get tag {name}")
    def get_tag(self, name: str) -> Tag:
        tag = self.repo.tags[name]
        return localtools.create_tag_model(tag.tag, tag.commit)  # type: ignore

    @localtools.handle_git_errors("Failed to list tags")
    def list_tags(self) -> list[Tag]:
        return [
            localtools.create_tag_model(tag.tag, tag.commit)  # type: ignore
            for tag in self.repo.tags  # type: ignore
        ]

    @functools.cached_property
    def remote_repository(self) -> Repository:
        """Get the remote code repository."""
        from githarbor import create_repository

        return create_repository(self.repo.remotes.origin.url)


if __name__ == "__main__":
    repo = LocalRepository(".")
    for commit in repo.list_commits(max_results=5):
        print(f"{commit.sha[:8]}: {commit.message.splitlines()[0]}")
    print(repo.list_tags())
