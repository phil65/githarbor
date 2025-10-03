"""Data types for Githarbor."""

from __future__ import annotations

import reprlib


class NiceReprList[T](list[T]):
    def __repr__(self):
        return reprlib.repr(list(self))
