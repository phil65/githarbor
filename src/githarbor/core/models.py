from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from datetime import datetime


@dataclass
class User:
    username: str
    """Username of the user."""
    name: str | None = None
    """Full name of the user."""
    email: str | None = None
    """Email address of the user."""
    avatar_url: str | None = None
    """URL of the user's avatar image."""
    created_at: datetime | None = None
    """Date and time when the user account was created."""
    bio: str | None = None
    """Biography/description of the user."""
    location: str | None = None
    """Geographic location of the user."""
    company: str | None = None
    """Company/organization the user belongs to."""
    state: str | None = None
    """State of the user (active / inactive)."""
    locked: bool | None = None
    """Whether user is locked."""


@dataclass
class Label:
    name: str
    """Name of the label."""
    color: str = ""  # Hex color code
    """Hex color code for the label."""
    description: str = ""
    """Description of what the label represents."""
    created_at: datetime | None = None
    """Date and time when the label was created."""
    updated_at: datetime | None = None
    """Date and time when the label was last updated."""


@dataclass
class Comment:
    id: str
    """Unique identifier for the comment."""
    body: str
    """Content of the comment."""
    author: User
    """User who wrote the comment."""
    created_at: datetime
    """Date and time when the comment was created."""
    updated_at: datetime | None = None
    """Date and time when the comment was last updated."""
    reactions: dict[str, int] = field(default_factory=dict)
    """Dictionary of reaction types and their counts."""
    reply_to: str | None = None  # ID of parent comment if this is a reply
    """ID of parent comment if this is a reply."""


@dataclass
class Branch:
    name: str
    """Name of the branch."""
    sha: str
    """SHA hash of the latest commit on this branch."""
    protected: bool = False
    """Whether the branch has protection rules enabled."""
    default: bool = False
    """Whether this is the default branch of the repository."""
    created_at: datetime | None = None
    """Date and time when the branch was created."""
    updated_at: datetime | None = None
    """Date and time when the branch was last updated."""


@dataclass
class PullRequest:
    title: str
    """Title of the pull request."""
    source_branch: str
    """Branch containing the changes to merge."""
    target_branch: str
    """Branch to merge the changes into."""
    description: str = ""
    """Description of the changes in the pull request."""
    state: str = "open"
    """Current state of the pull request."""
    number: int | None = None
    """Unique identifier for the pull request."""
    created_at: datetime | None = None
    """Date and time when the pull request was created."""
    updated_at: datetime | None = None
    """Date and time when the pull request was last updated."""
    merged_at: datetime | None = None
    """Date and time when the pull request was merged."""
    closed_at: datetime | None = None
    """Date and time when the pull request was closed."""
    author: User | None = None
    """User who created the pull request."""
    assignees: list[User] = field(default_factory=list)
    """List of users assigned to review the pull request."""
    labels: list[Label] = field(default_factory=list)
    """List of labels attached to the pull request."""
    comments: list[Comment] = field(default_factory=list)
    """List of comments on the pull request."""


@dataclass
class Issue:
    number: int
    """Unique identifier for the issue."""
    title: str
    """Title of the issue."""
    description: str = ""
    """Description of the issue."""
    state: str = "open"
    """State of the issue."""
    author: User | None = None
    """User who created the issue."""
    assignee: User | None = None
    """User who is assigned to the issue."""
    labels: list[Label] = field(default_factory=list)
    """List of labels attached to the issue."""
    created_at: datetime | None = None
    """Date and time when the issue was created."""
    updated_at: datetime | None = None
    """Date and time when the issue was last updated."""
    closed_at: datetime | None = None
    """Date and time when the issue was closed."""
    closed: bool = False
    """Indicates if the issue is closed."""


@dataclass
class Commit:
    sha: str
    """Unique identifier for the commit."""
    message: str
    """Commit message."""
    author: User
    """User who authored the commit."""
    created_at: datetime
    """Date and time when the commit was authored."""
    committer: User | None = None
    """User who committed the changes."""
    url: str | None = None
    """URL to the commit details."""
    stats: dict[str, int] = field(default_factory=dict)
    """Commit statistics."""
    parents: list[str] = field(default_factory=list)
    """List of parent commit SHAs."""


@dataclass
class Workflow:
    id: str
    """Unique identifier for the workflow."""
    name: str
    """Name of the workflow."""
    path: str
    """Path to the workflow file in the repository."""
    state: str
    """State of the workflow."""
    created_at: datetime
    """Date and time when the workflow was created."""
    updated_at: datetime | None = None
    """Date and time when the workflow was last updated."""
    description: str = ""
    """Description of the workflow."""
    triggers: list[str] = field(default_factory=list)
    """List of triggers that can start the workflow."""
    disabled: bool = False
    """Indicates if the workflow is disabled."""
    last_run_at: datetime | None = None
    """Date and time when the workflow was last run."""
    badge_url: str | None = None
    """URL to the badge image for the workflow."""
    definition: str | None = None
    """Content of the workflow definition file."""


@dataclass
class WorkflowRun:
    id: str
    """Unique identifier for the workflow run."""
    name: str
    """Name of the workflow run."""
    workflow_id: str
    """ID of the parent workflow."""
    status: str
    """Current status of the workflow run."""
    conclusion: str | None
    """Final conclusion of the workflow run."""
    branch: str | None = None
    """Branch the workflow was run on."""
    commit_sha: str | None = None
    """SHA of the commit that triggered the workflow."""
    url: str | None = None
    """URL to view the workflow run."""
    created_at: datetime | None = None
    """Date and time when the workflow run was created."""
    updated_at: datetime | None = None
    """Date and time when the workflow run was last updated."""
    started_at: datetime | None = None
    """Date and time when the workflow run started."""
    completed_at: datetime | None = None
    """Date and time when the workflow run completed."""


@dataclass
class Release:
    """Model representing a repository release."""

    tag_name: str
    """Tag name for the release."""
    name: str
    """Name/title of the release."""
    description: str = ""
    """Description/body of the release."""
    created_at: datetime | None = None
    """Date and time when the release was created."""
    published_at: datetime | None = None
    """Date and time when the release was published."""
    draft: bool = False
    """Whether this is a draft release."""
    prerelease: bool = False
    """Whether this is a pre-release."""
    author: User | None = None
    """User who created the release."""
    assets: list[dict[str, Any]] = field(default_factory=list)
    """List of assets attached to the release."""
    url: str | None = None
    """URL to view the release."""
    target_commitish: str | None = None
    """The branch/tag/commit the release targets."""
