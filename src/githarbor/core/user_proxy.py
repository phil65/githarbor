"""Module containing owner proxy implementation."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from githarbor.core.base import BaseOwner, BaseRepository


if TYPE_CHECKING:
    from collections.abc import Callable

    from githarbor.core.models import User


class Owner(BaseOwner):
    """Proxy class that forwards all method calls to an owner instance."""

    def __init__(self, owner: BaseOwner) -> None:
        """Initialize proxy with owner instance.

        Args:
            owner: Owner instance to forward calls to.
        """
        self._owner = owner
        self.owner_type = type(owner).__name__.removesuffix("Owner")

    def __repr__(self):
        return f"<{self.owner_type} {self.owner.name}>"

    @property
    def owner(self) -> BaseOwner:
        """Return wrapped owner instance."""
        return self._owner

    def list_repositories(self) -> list[BaseRepository]:
        """List repositories owned by this user.

        Returns:
            List of repositories.
        """
        if self._owner.is_async:
            return asyncio.run(self._owner.list_repositories_async())
        return self._owner.list_repositories()

    def create_repository(
        self,
        name: str,
        description: str = "",
        private: bool = False,
    ) -> BaseRepository:
        """Create a new repository.

        Args:
            name: Repository name
            description: Repository description
            private: Whether to create a private repository

        Returns:
            Newly created repository.
        """
        if self._owner.is_async:
            return asyncio.run(
                self._owner.create_repository_async(name, description, private)
            )
        return self._owner.create_repository(name, description, private)

    def get_user(self) -> User:
        """Get user information.

        Returns:
            User information.
        """
        if self._owner.is_async:
            return asyncio.run(self._owner.get_user_async())
        return self._owner.get_user()

    def delete_repository(self, name: str) -> None:
        """Delete a repository.

        Args:
            name: Name of repository to delete
        """
        if self._owner.is_async:
            return asyncio.run(self._owner.delete_repository_async(name))
        return self._owner.delete_repository(name)

    async def list_repositories_async(self) -> list[BaseRepository]:
        """List repositories asynchronously."""
        if self._owner.is_async:
            return await self._owner.list_repositories_async()
        return await asyncio.to_thread(self._owner.list_repositories)

    async def create_repository_async(
        self,
        name: str,
        description: str = "",
        private: bool = False,
    ) -> BaseRepository:
        """Create repository asynchronously."""
        if self._owner.is_async:
            return await self._owner.create_repository_async(
                name,
                description,
                private,
            )
        return await asyncio.to_thread(
            self._owner.create_repository,
            name,
            description,
            private,
        )

    async def get_user_async(self) -> User:
        """Get user information asynchronously."""
        if self._owner.is_async:
            return await self._owner.get_user_async()
        return await asyncio.to_thread(self._owner.get_user)

    async def delete_repository_async(self, name: str) -> None:
        """Delete repository asynchronously."""
        if self._owner.is_async:
            await self._owner.delete_repository_async(name)
        await asyncio.to_thread(self._owner.delete_repository, name)

    def get_sync_methods(self) -> list[Callable]:
        """Return list of all synchronous methods."""
        return [
            self.list_repositories,
            self.create_repository,
            self.get_user,
            self.delete_repository,
        ]

    def get_async_methods(self) -> list[Callable]:
        """Return list of all asynchronous methods."""
        return [
            self.list_repositories_async,
            self.create_repository_async,
            self.get_user_async,
            self.delete_repository_async,
        ]
