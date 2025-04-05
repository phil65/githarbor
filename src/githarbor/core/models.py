"""Base model for Githarbor."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from datetime import datetime


@dataclass
class User:
    """GitHub user model."""

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
    followers: int | None = None
    """Number of followers."""
    following: int | None = None
    """Number of users being followed."""
    public_repos: int | None = None
    """Number of public repositories."""
    url: str | None = None
    """URL to user's profile."""
    blog: str | None = None
    """Personal blog of this user"""
    twitter_username: str | None = None
    """X / Twitter username of this user"""
    hireable: bool | None = None
    """Whether user is available for hiring"""
    gravatar_id: str | None = None
    """Gravatar id for this user."""
    is_admin: bool | None = None
    """Whether the user has admin privileges."""
    last_activity_on: datetime | None = None
    """Date of last activity."""
    linkedin: str | None = None
    """LinkedIn username."""
    skype: str | None = None
    """Skype username."""


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
    url: str | None = None
    """URL to the label."""


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
    url: str | None = None
    """URL to the comment."""


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
    last_commit_date: datetime | None = None
    """Date of the last commit."""
    last_commit_message: str | None = None
    """Message of the last commit."""
    last_commit_author: User | None = None
    """Author of the last commit."""
    protection_rules: dict[str, Any] | None = None
    """Branch protection settings if any."""


@dataclass
class PullRequest:
    """Pull request model."""

    number: int
    """Pull request number."""
    title: str
    """Title of the pull request."""
    source_branch: str
    """Branch containing the changes."""
    target_branch: str
    """Branch to merge into."""
    description: str = ""
    """Description/body of the pull request."""
    state: str | None = None
    """Current state (open/closed/merged)."""
    created_at: datetime | None = None
    """Creation timestamp."""
    updated_at: datetime | None = None
    """Last update timestamp."""
    merged_at: datetime | None = None
    """Merge timestamp if merged."""
    closed_at: datetime | None = None
    """Closing timestamp if closed."""
    author: User | None = None
    """PR creator."""
    assignees: list[User] = field(default_factory=list)
    """Assigned reviewers."""
    labels: list[Label] = field(default_factory=list)
    """Applied labels."""
    comments: list[Comment] = field(default_factory=list)
    """PR comments."""
    merged_by: User | None = None
    """User who merged the PR."""
    review_comments_count: int | None = None
    """Number of review comments."""
    commits_count: int | None = None
    """Number of commits."""
    additions: int | None = None
    """Lines added."""
    deletions: int | None = None
    """Lines deleted."""
    changed_files: int | None = None
    """Number of files changed."""
    mergeable: bool | None = None
    """Whether PR can be merged."""
    url: str | None = None
    """URL to the PR."""


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
    comments_count: int = 0
    """Number of comments."""
    url: str | None = None
    """URL to the issue."""
    milestone: str | None = None
    """Associated milestone."""


@dataclass
class Commit:
    sha: str
    """Unique identifier for the commit."""
    message: str
    """Commit message."""
    author: User | None = None
    """User who authored the commit."""
    created_at: datetime | None = None
    """Date and time when the commit was authored."""
    committer: User | None = None
    """User who committed the changes."""
    url: str | None = None
    """URL to the commit details."""
    stats: dict[str, int] = field(default_factory=dict)
    """Commit statistics."""
    parents: list[str] = field(default_factory=list)
    """List of parent commit SHAs."""
    verified: bool = False
    """Signature verification status."""
    files_changed: list[str] = field(default_factory=list)
    """Changed file paths."""


@dataclass
class Tag:
    """Model representing a repository tag."""

    name: str
    """Name of the tag."""
    sha: str
    """SHA hash of the commit this tag points to."""
    message: str | None = None
    """Tag message if provided."""
    created_at: datetime | None = None
    """Date and time when the tag was created."""
    author: User | None = None
    """User who created the tag."""
    url: str | None = None
    """URL to view the tag."""
    verified: bool = False
    """Whether the tag signature is verified."""


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
    created_at: datetime | None = None
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
    runs_count: int = 0
    """Total number of runs."""
    success_count: int = 0
    """Successful runs count."""


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
    run_number: int | None = None
    """Sequential run number."""
    jobs_count: int | None = None
    """Total jobs count."""
    logs_url: str | None = None
    """Logs URL."""


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
    assets: list[Asset] = field(default_factory=list)
    """List of assets attached to the release."""
    url: str | None = None
    """URL to view the release."""
    target_commitish: str | None = None
    """The branch/tag/commit the release targets."""
    download_count: int = 0
    """Total downloads."""


@dataclass
class Asset:
    """Model representing a release asset."""

    name: str
    """Name of the asset."""
    url: str
    """Download URL for the asset."""
    size: int
    """Size of the asset in bytes."""
    download_count: int = 0
    """Number of times the asset has been downloaded."""
    created_at: datetime | None = None
    """Date and time when the asset was created."""
    updated_at: datetime | None = None
    """Date and time when the asset was last updated."""
