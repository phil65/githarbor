"""Data types for Githarbor."""

from __future__ import annotations

import reprlib
from typing import TypeVar


T = TypeVar("T")


class NiceReprList(list[T]):
    def __repr__(self):
        return reprlib.repr(list(self))
