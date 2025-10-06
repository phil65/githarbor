from __future__ import annotations

from importlib.metadata import version

__version__ = version("githarbor")

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
