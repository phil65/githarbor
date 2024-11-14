from __future__ import annotations

from datetime import datetime

import pytest

from githarbor.core.models import (
    Branch,
    Comment,
    Commit,
    Issue,
    Label,
    PullRequest,
    User,
    Workflow,
    WorkflowRun,
)


@pytest.fixture
def mock_user():
    return User(
        username="test-user",
        name="Test User",
        email="test@example.com",
        avatar_url="https://example.com/avatar.png",
        created_at=datetime(2023, 1, 1),
        bio="Test bio",
        location="Test Location",
        company="Test Company",
    )


@pytest.fixture
def mock_label():
    return Label(
        name="bug",
        color="#ff0000",
        description="Bug label",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 1),
    )


@pytest.fixture
def mock_comment(mock_user):
    return Comment(
        id="1",
        body="Test comment",
        author=mock_user,
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 1),
        reactions={"thumbs_up": 1, "heart": 2},
        reply_to=None,
    )


@pytest.fixture
def mock_commit(mock_user):
    return Commit(
        sha="abc123def456",
        message="Test commit message",
        author=mock_user,
        created_at=datetime(2023, 1, 1),
        committer=mock_user,
        url="https://github.com/owner/repo/commit/abc123def456",
        stats={"additions": 10, "deletions": 5},
        parents=["xyz789"],
    )


@pytest.fixture
def mock_issue(mock_user, mock_label):
    return Issue(
        title="Test Issue",
        number=42,
        description="Test issue description",
        state="open",
        author=mock_user,
        assignee=mock_user,
        labels=[mock_label],
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 1),
        closed_at=None,
        closed=False,
    )


@pytest.fixture
def mock_workflow():
    return Workflow(
        id="workflow-1",
        name="CI Pipeline",
        path=".github/workflows/ci.yml",
        state="active",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 1),
        description="Continuous Integration workflow",
        triggers=["push", "pull_request"],
        disabled=False,
        last_run_at=datetime(2023, 1, 1),
    )


@pytest.fixture
def mock_workflow_run():
    return WorkflowRun(
        id="run-1",
        name="CI Pipeline Run",
        workflow_id="workflow-1",
        status="completed",
        conclusion="success",
        branch="main",
        commit_sha="abc123def456",
        url="https://github.com/owner/repo/actions/runs/1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 1),
        started_at=datetime(2023, 1, 1),
        completed_at=datetime(2023, 1, 1),
    )


@pytest.fixture
def mock_branch() -> Branch:
    return Branch(
        name="feature-branch",
        sha="abc123",
        protected=False,
        default=False,
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 1),
    )


@pytest.fixture
def mock_pull_request(mock_user, mock_label, mock_comment) -> PullRequest:
    return PullRequest(
        number=1,
        title="Test PR",
        description="Test description",
        source_branch="feature-branch",
        target_branch="main",
        state="open",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 1),
        merged_at=None,
        closed_at=None,
        author=mock_user,
        assignees=[mock_user],
        labels=[mock_label],
        comments=[mock_comment],
    )
