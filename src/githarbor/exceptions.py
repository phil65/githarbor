from __future__ import annotations


class GitHarborError(Exception):
    """Base exception for GitHarbor."""


class RepositoryNotFoundError(GitHarborError):
    """Raised when a repository cannot be found."""


class AuthenticationError(GitHarborError):
    """Raised when authentication fails."""


class ResourceNotFoundError(GitHarborError):
    """Raised when a requested resource is not found."""


class OperationNotAllowedError(GitHarborError):
    """Raised when an operation is not allowed."""


class ProviderNotConfiguredError(GitHarborError):
    """Raised when provider is not properly configured (missing token etc)."""


class RateLimitError(GitHarborError):
    """Raised when API rate limit is exceeded."""


class FeatureNotSupportedError(GitHarborError):
    """Raised when a feature is not supported by the implementation."""


class OwnerNotFoundError(GitHarborError):
    """Raised when an owner cannot be found."""
