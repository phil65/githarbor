from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass
class FileChange:
    """Represents a file change in a diff."""

    path: str
    content: str | None  # None means file deletion
    mode: Literal["add", "modify", "delete"]
    old_path: str | None = None  # For renamed files


def parse_diff(diff_str: str) -> list[FileChange]:
    """Parse a unified diff string into a list of file changes.

    Uses the unidiff library for robust diff parsing.

    Args:
        diff_str: Unified diff string

    Returns:
        List of FileChange objects
    """
    import unidiff

    patch_set = unidiff.PatchSet(diff_str.splitlines(keepends=True))
    changes: list[FileChange] = []

    for patched_file in patch_set:
        if patched_file.is_rename:
            # Handle renamed files
            changes.append(
                FileChange(
                    path=patched_file.target_file,
                    old_path=patched_file.source_file,
                    content=patched_file.target_file[1:],  # Remove leading /
                    mode="modify",
                )
            )
            continue

        # Determine the change type
        if patched_file.is_added_file:
            mode = "add"
        elif patched_file.is_removed_file:
            mode = "delete"
        else:
            mode = "modify"

        # For deletions, we don't need content
        if mode == "delete":
            change = FileChange(path=patched_file.path, content=None, mode="delete")
            changes.append(change)
            continue
        # Reconstruct the final content
        lines = [ln.value for hunk in patched_file for ln in hunk if not ln.is_removed]
        change = FileChange(patched_file.path, content="".join(lines), mode=mode)  # type: ignore
        changes.append(change)

    return changes
