from __future__ import annotations

import json
import random
import subprocess
import sys
from io import BytesIO
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from db import SessionLocal
from models import Dataset
from storage import is_hidden_or_metadata_path

router = APIRouter()


def _get_dataset_by_partner_and_name_or_404(partner_id: str, dataset_name: str) -> Dataset:
    """
    Lookup dataset by partner + name. If multiple matches exist, returns the most recently uploaded.
    """
    db = SessionLocal()
    d = (
        db.query(Dataset)
        .filter(Dataset.partner_id == partner_id, Dataset.name == dataset_name)
        .order_by(Dataset.uploaded_at.desc())
        .first()
    )
    if not d:
        raise HTTPException(status_code=404, detail="Dataset not found for partner_id + dataset_name")
    return d


@router.get("/datasets")
def list_datasets():
    db = SessionLocal()
    datasets = db.query(Dataset).all()

    return [
        {
            "id": d.id,
            "partner_id": d.partner_id,
            "name": d.name,
            "file_count": d.file_count,
            "total_size_bytes": d.total_size_bytes,
            "uploaded_at": d.uploaded_at,
        }
        for d in datasets
    ]


@router.get("/datasets/by-name")
def dataset_by_name(partner_id: str, dataset_name: str):
    """
    Convenience lookup so you can browse using partner_id + dataset_name (no dataset_id needed).
    """
    d = _get_dataset_by_partner_and_name_or_404(partner_id, dataset_name)
    return {
        "id": d.id,
        "partner_id": d.partner_id,
        "name": d.name,
        "file_count": d.file_count,
        "total_size_bytes": d.total_size_bytes,
        "uploaded_at": d.uploaded_at,
        "path": d.path,
    }


def _safe_relpath(rel: str) -> Path:
    if not rel:
        raise HTTPException(status_code=400, detail="path is required")
    rel_norm = rel.replace("\\", "/")
    p = Path(rel_norm)
    if p.is_absolute():
        raise HTTPException(status_code=400, detail="Absolute paths not allowed")
    if ".." in p.parts:
        raise HTTPException(status_code=400, detail="Invalid path")
    return Path(*p.parts)


def _safe_join_dataset(root: Path, rel: str) -> Path:
    root_resolved = root.resolve()
    full = (root / _safe_relpath(rel)).resolve()
    if full != root_resolved and root_resolved not in full.parents:
        raise HTTPException(status_code=400, detail="Path escapes dataset root")
    return full


@router.get("/datasets/tree")
def dataset_tree(
    partner_id: str,
    dataset_name: str,
):
    """
    Return a directory tree for a dataset.
    """
    path = ""
    max_depth = 5
    max_entries = 2000

    d = _get_dataset_by_partner_and_name_or_404(partner_id, dataset_name)
    root = Path(d.path)

    if not root.exists():
        raise HTTPException(status_code=404, detail="Dataset path missing on disk")

    start = root if not path else _safe_join_dataset(root, path)
    if not start.exists():
        raise HTTPException(status_code=404, detail="Path not found")
    if not start.is_dir():
        raise HTTPException(status_code=400, detail="Path must be a directory")

    truncated = False
    entries_seen = 0

    def _rel(p: Path) -> str:
        return "" if p == root else str(p.relative_to(root))

    def _build_dir_node(dir_path: Path, depth: int) -> dict[str, Any]:
        nonlocal truncated, entries_seen
        node: dict[str, Any] = {
            "type": "directory",
            "name": dir_path.name if dir_path != root else "",
            "path": _rel(dir_path),
            "children": [],
        }

        if depth >= max_depth or truncated:
            return node

        children = sorted(
            dir_path.iterdir(),
            key=lambda p: (not p.is_dir(), p.name.lower()),
        )
        for child in children:
            rel_child = child.relative_to(root)
            if is_hidden_or_metadata_path(rel_child):
                continue
            if entries_seen >= max_entries:
                truncated = True
                break
            entries_seen += 1

            if child.is_dir():
                node["children"].append(_build_dir_node(child, depth + 1))
            else:
                node["children"].append(
                    {
                        "type": "file",
                        "name": child.name,
                        "path": str(rel_child),
                        "size_bytes": child.stat().st_size,
                    }
                )

        return node

    tree = _build_dir_node(start, 0)
    return {
        "partner_id": partner_id,
        "dataset_name": dataset_name,
        "dataset_id": d.id,
        "root_path": _rel(start),
        "max_depth": max_depth,
        "max_entries": max_entries,
        "entries_returned": entries_seen,
        "truncated": truncated,
        "tree": tree,
    }


def _parquet_preview_subprocess(parquet_path: Path, n: int) -> dict[str, Any]:
    """
    Generate a parquet preview in a separate Python process.

    This avoids taking down the web server in case the local pyarrow installation
    is misconfigured and crashes the interpreter.
    """
    code = r"""
import json
import sys

parquet_path = sys.argv[1]
n = int(sys.argv[2])

import pyarrow as pa
import pyarrow.parquet as pq

pf = pq.ParquetFile(parquet_path)
batch = next(pf.iter_batches(batch_size=n), None)
if batch is None:
    print(json.dumps({"rows": [], "columns": [], "returned": 0}))
    raise SystemExit(0)

table = pa.Table.from_batches([batch])
print(
    json.dumps(
        {"rows": table.to_pylist(), "columns": table.schema.names, "returned": table.num_rows},
        default=str,
    )
)
"""
    proc = subprocess.run(
        [sys.executable, "-c", code, str(parquet_path), str(n)],
        capture_output=True,
        text=True,
        timeout=20,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        raise RuntimeError(stderr or f"preview subprocess failed (exit={proc.returncode})")
    out = (proc.stdout or "").strip()
    if not out:
        return {"rows": [], "columns": [], "returned": 0}
    return json.loads(out)


@router.get("/preview")
def preview_dataset(
    partner_id: str,
    dataset_name: str,
    parquet_rows: int = Query(5, ge=1, le=200),
):
    """
    High-level preview: given partner_id + dataset_name, returns previews for all parquet and images.
    - parquet: first N rows for each file
    - images: thumbnail as a data URL (base64)
    """
    d = _get_dataset_by_partner_and_name_or_404(partner_id, dataset_name)
    root = Path(d.path)

    if not root.exists():
        raise HTTPException(status_code=404, detail="Dataset path missing on disk")

    errors: list[dict[str, Any]] = []

    # --- parquet previews ---
    parquet_previews: list[dict[str, Any]] = []
    max_parquet_files = 25
    max_image_files = 50
    thumbnail_max_size = 256

    parquet_files = sorted(root.rglob("*.parquet"))[:max_parquet_files]
    for f in parquet_files:
        rel = str(f.relative_to(root))
        try:
            preview = _parquet_preview_subprocess(f, parquet_rows)
            parquet_previews.append(
                {
                    "path": rel,
                    "rows": preview.get("rows", []),
                    "returned": preview.get("returned", 0),
                    "columns": preview.get("columns", []),
                }
            )
        except Exception as e:
            errors.append({"type": "parquet", "path": rel, "error": str(e)})

    # --- image thumbnails (as URLs) ---
    image_previews: list[dict[str, Any]] = []
    try:
        from PIL import Image  # type: ignore
    except Exception:
        raise HTTPException(status_code=500, detail="Pillow not installed (pip install pillow)")

    image_exts = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tif", ".tiff", ".heic"}
    image_files = [p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in image_exts]
    image_files = sorted(image_files)[:max_image_files]

    for f in image_files:
        rel = str(f.relative_to(root))
        try:
            with Image.open(f) as img:
                image_previews.append(
                    {
                        "path": rel,
                        "width": img.size[0],
                        "height": img.size[1],
                        "thumbnail_url": f"/thumbnail?partner_id={partner_id}&dataset_name={dataset_name}&path={rel}",
                    }
                )
        except Exception as e:
            errors.append({"type": "image", "path": rel, "error": str(e)})

    return {
        "partner_id": partner_id,
        "dataset_name": dataset_name,
        "dataset_id": d.id,
        "parquet": {"count": len(parquet_files), "previews": parquet_previews},
        "images": {"count": len(image_files), "previews": image_previews},
        "errors": errors,
    }


@router.get("/thumbnail")
def thumbnail(
    partner_id: str,
    dataset_name: str,
    path: str | None = None,
):
    """
    Return an actual image thumbnail response (image/jpeg or image/png).
    This is much easier to view than base64 blobs in Swagger.
    """
    d = _get_dataset_by_partner_and_name_or_404(partner_id, dataset_name)
    root = Path(d.path)

    image_exts = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tif", ".tiff", ".heic"}
    if path:
        full = _safe_join_dataset(root, path)
    else:
        image_files = [
            p
            for p in root.rglob("*")
            if p.is_file()
            and p.suffix.lower() in image_exts
            and not is_hidden_or_metadata_path(p.relative_to(root))
        ]
        if not image_files:
            raise HTTPException(status_code=404, detail="No image files found in dataset")
        full = random.choice(image_files)

    if not full.exists() or not full.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    try:
        from PIL import Image  # type: ignore
    except Exception:
        raise HTTPException(status_code=500, detail="Pillow not installed (pip install pillow)")

    thumbnail_max_size = 256

    try:
        with Image.open(full) as img:
            img.thumbnail((thumbnail_max_size, thumbnail_max_size))

            has_alpha = (
                img.mode in ("RGBA", "LA")
                or (img.mode == "P" and "transparency" in (img.info or {}))
            )

            out = BytesIO()
            if has_alpha:
                img.save(out, format="PNG")
                media_type = "image/png"
            else:
                if img.mode not in ("RGB", "L"):
                    img = img.convert("RGB")
                img.save(out, format="JPEG", quality=85, optimize=True)
                media_type = "image/jpeg"
            out.seek(0)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to read image: {e}")

    return StreamingResponse(out, media_type=media_type)
