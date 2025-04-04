from __future__ import annotations

import fnmatch
import logging
import os
from typing import TYPE_CHECKING, Any, ClassVar, Literal
from urllib.parse import urlparse

from githarbor.core.base import BaseRepository, IssueState, PullRequestState
from githarbor.exceptions import AuthenticationError, ResourceNotFoundError
from githarbor.providers.github_provider import utils as githubtools


if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime

    from githarbor.core.models import (
        Branch,
        Comment,
        Commit,
        Issue,
        PullRequest,
        Release,
        Tag,
        User,
        Workflow,
        WorkflowRun,
    )


logger = logging.getLogger(__name__)
HTML_ERROR_CODE = 404
TOKEN = os.getenv("GITHUB_TOKEN")


class GitHubRepository(BaseRepository):
    """GitHub repository implementation."""

    url_patterns: ClassVar[list[str]] = ["github.com"]
    raw_prefix: ClassVar[str] = (
        "https://raw.githubusercontent.com/{owner}/{name}/{branch}/{path}"
    )

    def __init__(self, owner: str, name: str, token: str | None = None):
        """Initialize GitHub repository."""
        from github import Auth, Github, NamedUser
        from github.GithubException import GithubException

        try:
            t = token or TOKEN
            if t is None:
                logger.info("No GitHub token provided. Stricter rate limit.")
            auth = Auth.Token(t) if t else None
            self._gh = Github(auth=auth)
            self._repo = self._gh.get_repo(f"{owner}/{name}")
            self._owner = owner
            self._name = name
            self.user: NamedUser.NamedUser = self._gh.get_user(owner)  # type: ignore
        except GithubException as e:
            msg = f"GitHub authentication failed: {e!s}"
            raise AuthenticationError(msg) from e

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> GitHubRepository:
        """Create from URL like 'https://github.com/owner/repo'."""
        parsed = urlparse(url)
        parts = parsed.path.strip("/").split("/")
        if len(parts) < 2:  # noqa: PLR2004
            msg = f"Invalid GitHub URL: {url}"
            raise ValueError(msg)

        return cls(parts[0], parts[1].removesuffix(".git"), token=kwargs.get("token"))

    @property
    def default_branch(self) -> str:
        return self._repo.default_branch

    @property
    def edit_base_uri(self):
        return f"edit/{self.default_branch}/"

    @githubtools.handle_github_errors("Failed to get branch {name}")
    def get_branch(self, name: str) -> Branch:
        branch = self._repo.get_branch(name)
        model = githubtools.create_branch_model(branch)
        model.default = branch.name == self.default_branch
        return model

    @githubtools.handle_github_errors("Failed to get pull request {number}")
    def get_pull_request(self, number: int) -> PullRequest:
        pr = self._repo.get_pull(number)
        return githubtools.create_pull_request_model(pr)

    @githubtools.handle_github_errors("Failed to list pull requests")
    def list_pull_requests(self, state: PullRequestState = "open") -> list[PullRequest]:
        prs = self._repo.get_pulls(state=state)
        return [githubtools.create_pull_request_model(pr) for pr in prs]

    @githubtools.handle_github_errors("Failed to get issue {issue_id}")
    def get_issue(self, issue_id: int) -> Issue:
        issue = self._repo.get_issue(issue_id)
        return githubtools.create_issue_model(issue)

    @githubtools.handle_github_errors("Failed to list issues")
    def list_issues(self, state: IssueState = "open") -> list[Issue]:
        issues = self._repo.get_issues(state=state)
        return [githubtools.create_issue_model(issue) for issue in issues]

    @githubtools.handle_github_errors("Failed to create issue")
    def create_issue(
        self,
        title: str,
        body: str,
        labels: list[str] | None = None,
        assignees: list[str] | None = None,
    ) -> Issue:
        """Create a new issue."""
        issue = self._repo.create_issue(
            title=title,
            body=body,
            labels=labels or [],
            assignees=assignees or [],
        )
        return githubtools.create_issue_model(issue)

    @githubtools.handle_github_errors("Failed to get commit {sha}")
    def get_commit(self, sha: str) -> Commit:
        commit = self._repo.get_commit(sha)
        return githubtools.create_commit_model(commit)

    @githubtools.handle_github_errors("Failed to list commits")
    def list_commits(
        self,
        branch: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        author: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        kwargs = {
            "since": since,
            "until": until,
            "author": author,
            "path": path,
            "sha": branch,
        }
        # Filter out None values
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        commits = self._repo.get_commits(**kwargs)  # type: ignore
        results = commits[:max_results] if max_results else commits
        return [githubtools.create_commit_model(c) for c in results]

    @githubtools.handle_github_errors("Failed to get workflow {workflow_id}")
    def get_workflow(self, workflow_id: str) -> Workflow:
        workflow = self._repo.get_workflow(workflow_id)
        return githubtools.create_workflow_model(workflow)

    @githubtools.handle_github_errors("Failed to list workflows")
    def list_workflows(self) -> list[Workflow]:
        workflows = self._repo.get_workflows()
        return [githubtools.create_workflow_model(w) for w in workflows]

    @githubtools.handle_github_errors("Failed to get workflow run {run_id}")
    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        run = self._repo.get_workflow_run(int(run_id))
        return githubtools.create_workflow_run_model(run)

    @githubtools.handle_github_errors("Failed to download file {path}")
    def download(
        self,
        path: str | os.PathLike[str],
        destination: str | os.PathLike[str],
        recursive: bool = False,
    ):
        user_name = self._gh.get_user().login if TOKEN else None
        return githubtools.download_from_github(
            org=self._owner,
            repo=self._name,
            path=path,
            destination=destination,
            username=user_name,
            token=TOKEN,
            recursive=recursive,
        )

    @githubtools.handle_github_errors("Failed to search commits")
    def search_commits(
        self,
        query: str,
        branch: str | None = None,
        path: str | None = None,
        max_results: int | None = None,
    ) -> list[Commit]:
        # Build the search query
        search_query = f"{query} repo:{self._owner}/{self._name}"
        # Add branch qualifier if specified
        if branch:
            search_query += f" ref:{branch}"
        # Add path qualifier if specified
        if path:
            search_query += f" path:{path}"
        kwargs = {"query": search_query}
        # kwargs = {"query": f"{self._owner}/{self._name}+{query}"}
        # if branch:
        #     kwargs["ref"] = branch
        # if path:
        #     kwargs["path"] = path
        results = self._gh.search_commits(**kwargs)
        commits = list(results[:max_results] if max_results else results)
        return [self.get_commit(c.sha) for c in commits]

    @githubtools.handle_github_errors("Failed to list files for {path}")
    def iter_files(
        self,
        path: str = "",
        ref: str | None = None,
        pattern: str | None = None,
    ) -> Iterator[str]:
        contents = self._repo.get_contents(path, ref=ref or self.default_branch)
        assert isinstance(contents, list)
        kwargs = {"ref": ref} if ref else {}
        while contents:
            content = contents.pop(0)
            if content.type == "dir":
                c = self._repo.get_contents(content.path, **kwargs)
                assert isinstance(c, list)
                contents.extend(c)
            elif not pattern or fnmatch.fnmatch(content.path, pattern):
                yield content.path

    @githubtools.handle_github_errors("Failed to get repository owner info")
    def get_repo_user(self) -> User:
        """Get user (repository owner) information."""
        return githubtools.create_user_model(self.user)

    @githubtools.handle_github_errors("Failed to get contributors")
    def get_contributors(
        self,
        sort_by: Literal["commits", "name", "date"] = "commits",
        limit: int | None = None,
    ) -> list[User]:
        contributors = list(self._repo.get_contributors())
        if sort_by == "name":
            contributors = sorted(contributors, key=lambda c: c.login)
        elif sort_by == "date":
            contributors = sorted(contributors, key=lambda c: c.created_at)
        contributors = contributors[:limit] if limit else contributors
        return [u for c in contributors if (u := githubtools.create_user_model(c))]

    @githubtools.handle_github_errors("Failed to get languages")
    def get_languages(self) -> dict[str, int]:
        return self._repo.get_languages()

    @githubtools.handle_github_errors("Failed to compare branches")
    def compare_branches(
        self,
        base: str,
        head: str,
        include_commits: bool = True,
        include_files: bool = True,
        include_stats: bool = True,
    ) -> dict[str, Any]:
        comp = self._repo.compare(base, head)
        result: dict[str, Any] = {"ahead_by": comp.ahead_by, "behind_by": comp.behind_by}
        if include_commits:
            result["commits"] = [self.get_commit(c.sha) for c in comp.commits]
        if include_files:
            result["files"] = [f.filename for f in comp.files]
        if include_stats:
            result["stats"] = {
                "additions": comp.total_commits,
                "deletions": comp.total_commits,
                "changes": len(comp.files),
            }
        return result

    @githubtools.handle_github_errors("Failed to get latest release")
    def get_latest_release(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
    ) -> Release:  # Changed from dict[str, Any] to Release
        releases = self._repo.get_releases()
        # Filter releases based on parameters
        filtered = [
            release
            for release in releases
            if (include_drafts or not release.draft)
            and (include_prereleases or not release.prerelease)
        ]
        if not filtered:
            msg = "No matching releases found"
            raise ResourceNotFoundError(msg)
        latest = filtered[0]  # Releases are returned in chronological order
        return githubtools.create_release_model(latest)

    @githubtools.handle_github_errors("Failed to list releases")
    def list_releases(
        self,
        include_drafts: bool = False,
        include_prereleases: bool = False,
        limit: int | None = None,
    ) -> list[Release]:
        filtered_releases = (
            release
            for release in self._repo.get_releases()
            if (include_drafts or not release.draft)
            and (include_prereleases or not release.prerelease)
        )
        return [
            githubtools.create_release_model(release)
            for release in (
                list(filtered_releases)[:limit] if limit else filtered_releases
            )
        ]

    @githubtools.handle_github_errors("Failed to get release {tag}")
    def get_release(self, tag: str) -> Release:
        release = self._repo.get_release(tag)
        return githubtools.create_release_model(release)

    @githubtools.handle_github_errors("Failed to get tag {name}")
    def get_tag(self, name: str) -> Tag:
        """Get a specific tag by name."""
        from github.GithubException import GithubException

        try:
            tag = self._repo.get_git_ref(f"tags/{name}")
            tag_obj = self._repo.get_git_tag(tag.object.sha)
            return githubtools.create_tag_model(tag_obj)
        except GithubException as e:
            if e.status == HTML_ERROR_CODE:  # Might be lightweight tag
                commit = self._repo.get_commit(name)
                return githubtools.create_tag_model(commit)
            raise

    @githubtools.handle_github_errors("Failed to list tags")
    def list_tags(self) -> list[Tag]:
        """List all repository tags."""
        return [githubtools.create_tag_model(tag) for tag in self._repo.get_tags()]

    @githubtools.handle_github_errors("Failed to create pull request")
    def create_pull_request(
        self,
        title: str,
        body: str,
        head_branch: str,
        base_branch: str,
        draft: bool = False,
    ) -> PullRequest:
        pr = self._repo.create_pull(
            title=title,
            body=body,
            head=head_branch,
            base=base_branch,
            draft=draft,
        )
        return githubtools.create_pull_request_model(pr)

    @githubtools.handle_github_errors("Failed to create pull request from diff")
    def create_pull_request_from_diff(
        self,
        title: str,
        body: str,
        base_branch: str,
        diff: str,
        head_branch: str | None = None,
        draft: bool = False,
    ) -> PullRequest:
        """Create a pull request from a diff string."""
        from datetime import datetime
        import uuid

        from github import InputGitTreeElement
        from github.GithubException import GithubException

        from githarbor.core.filechanges import parse_diff
        from githarbor.exceptions import GitHarborError

        # Generate unique branch name if not provided
        if head_branch is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_id = str(uuid.uuid4())[:8]
            head_branch = f"patch/{timestamp}_{unique_id}"

        # Get the base branch's last commit
        base_ref = self._repo.get_git_ref(f"refs/heads/{base_branch}")
        base_commit = self._repo.get_git_commit(base_ref.object.sha)

        # Create a new branch
        try:
            head_ref = self._repo.get_git_ref(f"refs/heads/{head_branch}")
            msg = f"Branch {head_branch} already exists"
            raise GitHarborError(msg)
        except GithubException:
            ref = f"refs/heads/{head_branch}"
            head_ref = self._repo.create_git_ref(ref=ref, sha=base_ref.object.sha)

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
                blob = self._repo.create_git_blob(
                    content=change.content, encoding="utf-8"
                )

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
        base_tree = self._repo.get_git_tree(base_commit.tree.sha)
        tree = self._repo.create_git_tree(new_tree, base_tree)

        # Create a commit
        msg = f"Changes for {title}"
        commit = self._repo.create_git_commit(msg, tree=tree, parents=[base_commit])

        # Update the reference
        head_ref.edit(commit.sha, force=True)

        # Create the pull request
        pr = self._repo.create_pull(
            title=title,
            body=body,
            base=base_branch,
            head=head_branch,
            draft=draft,
        )
        return githubtools.create_pull_request_model(pr)

    @githubtools.handle_github_errors("Failed to create branch")
    def create_branch(
        self,
        name: str,
        base_commit: str,
    ) -> Branch:
        """Create a new branch at the specified commit."""
        # Create reference with full ref name
        _ref = self._repo.create_git_ref(
            ref=f"refs/heads/{name}",
            sha=base_commit,
        )
        # Get the branch to return proper Branch model
        branch = self._repo.get_branch(name)
        return githubtools.create_branch_model(branch)

    @githubtools.handle_github_errors("Failed to add pull request comment")
    def add_pull_request_comment(
        self,
        number: int,
        body: str,
    ) -> Comment:
        pr = self._repo.get_pull(number)
        comment = pr.create_issue_comment(body)
        return githubtools.create_comment_model(comment)

    @githubtools.handle_github_errors("Failed to add pull request review comment")
    def add_pull_request_review_comment(
        self,
        number: int,
        body: str,
        commit_id: str,
        path: str,
        position: int,
    ) -> Comment:
        pr = self._repo.get_pull(number)
        commit = self._repo.get_commit(commit_id)
        comment = pr.create_review_comment(body, commit, path, position)
        return githubtools.create_comment_model(comment)


if __name__ == "__main__":
    repo = GitHubRepository.from_url("https://github.com/phil65/mknodes")
    commits = repo.search_commits("implement")
    print(commits)
    # print(repo.list_workflows())
    branch = repo.get_branch("main")
    print(branch)
