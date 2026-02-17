import uuid
from fastapi import APIRouter, UploadFile, File, HTTPException
from pathlib import Path
from db import SessionLocal
from models import Dataset
from storage import is_hidden_or_metadata_path, save_files

router = APIRouter()

@router.post("/upload")
def upload(partner_id: str, dataset_name: str, files: list[UploadFile] = File(...)):
    dataset_id = str(uuid.uuid4())

    try:
        # save files to disk
        path = save_files(partner_id, dataset_name, files)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # dataset metadata
    root = Path(path)
    all_files = [
        f
        for f in root.rglob("*")
        if f.is_file() and not is_hidden_or_metadata_path(f.relative_to(root))
    ]

    file_count = len(all_files)
    total_size = sum(f.stat().st_size for f in all_files)

    if file_count == 0:
        raise HTTPException(status_code=400, detail="No uploadable files found (hidden/metadata files were skipped)")

    db = SessionLocal()

    dataset = Dataset(
        id=dataset_id,
        partner_id=partner_id,
        name=dataset_name,
        type="mixed",
        path=str(path),
        file_count=file_count,
        total_size_bytes=total_size,
    )

    db.add(dataset)
    db.commit()

    return {"dataset_id": dataset_id}
