from __future__ import annotations

from pathlib import Path, PurePosixPath

BASE_PATH = Path("data/raw")

def is_hidden_or_metadata_path(path: Path) -> bool:
    """
    Return True for files users almost never intend to upload, especially on macOS:
    - .DS_Store
    - AppleDouble resource forks like ._foo.jpg
    - any path segment starting with "."
    - Windows thumbnail DBs
    """
    parts = [p for p in path.parts if p]
    if any(part.startswith(".") for part in parts):
        return True
    base = path.name
    if base in {".DS_Store", "Thumbs.db"}:
        return True
    if base.startswith("._"):
        return True
    return False


def _safe_segment(value: str, field_name: str) -> str:
    if not value:
        raise ValueError(f"Empty {field_name}")
    if "\x00" in value:
        raise ValueError(f"Invalid {field_name}")
    if "/" in value or "\\" in value:
        raise ValueError(f"{field_name} must be a single path segment")
    if value in (".", ".."):
        raise ValueError(f"Invalid {field_name}")
    return value

def _safe_relpath(filename: str) -> Path:
    """
    Convert an uploaded filename into a safe, relative filesystem path.

    Supports folder uploads where the client sends filenames like:
      "my_folder/subdir/file.parquet"

    Rejects absolute paths and path traversal ("..").
    """
    if not filename:
        raise ValueError("Empty filename")

    # Browsers / clients may send backslashes; normalize to POSIX separators.
    normalized = filename.replace("\\", "/")
    p = PurePosixPath(normalized)

    # Disallow absolute paths and path traversal.
    if p.is_absolute():
        raise ValueError(f"Absolute paths not allowed: {filename!r}")
    if any(part in ("..", "") for part in p.parts):
        # "" can happen with leading/trailing slashes or 'a//b'
        raise ValueError(f"Invalid path: {filename!r}")

    return Path(*p.parts)


def save_files(partner_id: str, dataset_name: str, files) -> Path:
    partner_id = _safe_segment(partner_id, "partner_id")
    dataset_name = _safe_segment(dataset_name, "dataset_name")

    root = BASE_PATH / partner_id / dataset_name
    root.mkdir(parents=True, exist_ok=True)

    for f in files:
        rel = _safe_relpath(f.filename)
        if is_hidden_or_metadata_path(rel):
            # Skip OS metadata / hidden files so they don't pollute datasets.
            continue
        dest = root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as out:
            out.write(f.file.read())

    return root
