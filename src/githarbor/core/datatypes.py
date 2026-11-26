"""Data types for Githarbor."""

import reprlib


class NiceReprList[T](list[T]):
    def __repr__(self):
        return reprlib.repr(list(self))
