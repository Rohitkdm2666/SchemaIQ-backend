from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, UploadFile

from db.connection import set_db_url
from db.loader import load_file

router = APIRouter(tags=["upload"])

UPLOAD_DIR = Path(__file__).resolve().parent.parent / "uploads"
SHARED_SQLITE_PATH = UPLOAD_DIR / "uploaded_data.sqlite"


@router.post("/upload")
async def upload(file: UploadFile):
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = UPLOAD_DIR / file.filename

    content = await file.read()
    dest.write_bytes(content)

    suffix = dest.suffix.lower()
    try:
        if suffix == ".csv":
            loaded = load_file(str(dest), sqlite_out_path=str(SHARED_SQLITE_PATH), append=True)
        else:
            loaded = load_file(str(dest), sqlite_out_path=str(UPLOAD_DIR / f"{dest.stem}.sqlite"), append=False)

        set_db_url(loaded["db_url"], source='file', display_name=file.filename)

        return {
            "status": "uploaded",
            "path": str(dest),
            "db_url": loaded["db_url"],
            "sqlite_path": loaded.get("sqlite_path"),
        }
    except Exception as e:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {str(e)}")
