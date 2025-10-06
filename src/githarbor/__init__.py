"""GitHarbor: main package.

Unified client for GitHub, GitLab and BitBucket.
"""

from __future__ import annotations

from importlib.metadata import version

__version__ = version("githarbor")
__title__ = "GitHarbor"

__author__ = "Philipp Temminghoff"
__author_email__ = "philipptemminghoff@googlemail.com"
__copyright__ = "Copyright (c) 2024 Philipp Temminghoff"
__license__ = "MIT"
__url__ = "https://github.com/phil65/githarbor"

from githarbor.core.base import BaseRepository
from githarbor.core.models import (
    Branch,
    Comment,
    Commit,
    Issue,
    Label,
    PullRequest,
    Release,
    User,
    Workflow,
    WorkflowRun,
)
from githarbor.exceptions import (
    AuthenticationError,
    GitHarborError,
    OperationNotAllowedError,
    ProviderNotConfiguredError,
    RateLimitError,
    RepositoryNotFoundError,
    ResourceNotFoundError,
)
from githarbor.repositories import create_repository


__all__ = [
    "AuthenticationError",
    # Base
    "BaseRepository",
    # Models
    "Branch",
    "Comment",
    "Commit",
    # Exceptions
    "GitHarborError",
    "Issue",
    "Label",
    "OperationNotAllowedError",
    "ProviderNotConfiguredError",
    "PullRequest",
    "RateLimitError",
    "Release",
    "RepositoryNotFoundError",
    "ResourceNotFoundError",
    "User",
    "Workflow",
    "WorkflowRun",
    "__version__",
    # Factory
    "create_repository",
]
