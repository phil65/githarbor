from __future__ import annotations

from datetime import datetime, timedelta
import os
from typing import TYPE_CHECKING

import pytest

from githarbor.repositories import create_repository


if TYPE_CHECKING:
    from githarbor.core.proxy import Repository


GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_URL = "https://github.com/phil65/mknodes"
SKIP_MSG = "GITHUB_TOKEN not set"


@pytest.fixture
def github_repo() -> Repository:
    if not GITHUB_TOKEN:
        pytest.skip(SKIP_MSG)
    return create_repository(GITHUB_URL, token=GITHUB_TOKEN)


@pytest.mark.integration
@pytest.mark.skipif(not GITHUB_TOKEN, reason=SKIP_MSG)
def test_basic_repo_info(github_repo: Repository):
    """Test basic repository information retrieval."""
    assert github_repo.name == "mknodes"
    assert isinstance(github_repo.default_branch, str)
    assert len(github_repo.get_languages()) > 0


@pytest.mark.integration
@pytest.mark.skipif(not GITHUB_TOKEN, reason=SKIP_MSG)
def test_commit_operations(github_repo: Repository):
    """Test commit-related operations."""
    # Get commits from last week
    since = datetime.now() - timedelta(days=7)
    commits = github_repo.list_commits(since=since, max_results=5)
    assert len(commits) <= 5  # noqa: PLR2004
    if commits:
        # Test single commit retrieval
        commit = github_repo.get_commit(commits[0].sha)
        assert commit.sha == commits[0].sha
        assert commit.message

    # Test commit search
    search_results = github_repo.search_commits("fix", max_results=3)
    assert isinstance(search_results, list)
    assert len(search_results) <= 3  # noqa: PLR2004


@pytest.mark.integration
@pytest.mark.skipif(not GITHUB_TOKEN, reason=SKIP_MSG)
def test_branch_operations(github_repo: Repository):
    """Test branch-related operations."""
    branch = github_repo.get_branch(github_repo.default_branch)
    assert branch.name == github_repo.default_branch
    assert branch.sha
    assert isinstance(branch.protected, bool)


@pytest.mark.integration
@pytest.mark.skipif(not GITHUB_TOKEN, reason=SKIP_MSG)
def test_workflow_operations(github_repo: Repository):
    """Test workflow-related operations."""
    workflows = github_repo.list_workflows()
    assert isinstance(workflows, list)
    if workflows:
        workflow = github_repo.get_workflow(workflows[0].id)
        assert workflow.id == workflows[0].id
        assert workflow.name == workflows[0].name


@pytest.mark.integration
@pytest.mark.skipif(not GITHUB_TOKEN, reason=SKIP_MSG)
def test_release_operations(github_repo: Repository):
    """Test release-related operations."""
    releases = github_repo.list_releases(limit=3)
    assert isinstance(releases, list)
    assert len(releases) <= 3  # noqa: PLR2004

    if releases:
        release = github_repo.get_release(releases[0].tag_name)
        assert release.tag_name == releases[0].tag_name
        assert isinstance(release.draft, bool)
        assert isinstance(release.prerelease, bool)


@pytest.mark.integration
@pytest.mark.skipif(not GITHUB_TOKEN, reason=SKIP_MSG)
def test_contributor_operations(github_repo: Repository):
    """Test contributor-related operations."""
    contributors = github_repo.get_contributors(limit=5)
    assert isinstance(contributors, list)
    assert len(contributors) <= 5  # noqa: PLR2004
    if contributors:
        assert contributors[0].username
        assert isinstance(contributors[0].avatar_url, str)


@pytest.mark.integration
@pytest.mark.skipif(not GITHUB_TOKEN, reason=SKIP_MSG)
def test_activity_stats(github_repo: Repository):
    """Test recent activity statistics."""
    activity = github_repo.get_recent_activity(days=7)
    assert isinstance(activity, dict)
    assert "commits" in activity
    assert "issues" in activity
    assert "pull_requests" in activity
    assert all(isinstance(v, int) for v in activity.values())
