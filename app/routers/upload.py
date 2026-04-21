"""
LegalVision API - Deed Upload Router
POST /api/v1/upload/deeds
"""

from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from typing import List
import tempfile
import shutil
from pathlib import Path
import logging

from app.services.deed_uploader import DeedUploader
from app.core.config import settings

logger = logging.getLogger(__name__)
router = APIRouter()

ALLOWED_EXTENSIONS = {".docx", ".txt"}
MAX_BYTES = settings.MAX_UPLOAD_SIZE_MB * 1024 * 1024

_uploader = DeedUploader()   # single instance, reused across requests


@router.post("/deeds", summary="Upload deed files to the knowledge base")
async def upload_deeds(files: List[UploadFile] = File(...)):
    """
    Upload one or many deed files (.docx or .txt).

    Each file is processed through the full pipeline:
    1. Text extraction
    2. NER extraction  
    3. Loaded into Neo4j knowledge graph

    Temporary files are deleted after processing.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files provided.")

    # Validate extensions and sizes up front
    for f in files:
        ext = Path(f.filename).suffix.lower()
        if ext not in ALLOWED_EXTENSIONS:
            raise HTTPException(
                status_code=415,
                detail=f"'{f.filename}' is not supported. Only .docx and .txt are accepted."
            )

    # Save uploads to a short-lived temp dir
    tmp_dir = Path(tempfile.mkdtemp(prefix="lv_upload_"))
    saved_paths: List[str] = []

    try:
        for f in files:
            dest = tmp_dir / f.filename
            content = await f.read()

            if len(content) > MAX_BYTES:
                raise HTTPException(
                    status_code=413,
                    detail=f"'{f.filename}' exceeds the {settings.MAX_UPLOAD_SIZE_MB}MB limit."
                )

            dest.write_bytes(content)
            saved_paths.append(str(dest))
            logger.info(f"Saved upload: {dest.name} ({len(content):,} bytes)")

        # Run the pipeline
        result = _uploader.upload_files(saved_paths)

    finally:
        # Always clean up the upload copies
        shutil.rmtree(tmp_dir, ignore_errors=True)

    if not result["success"]:
        raise HTTPException(status_code=422, detail={
            "message": "Pipeline failed",
            "errors": result["errors"]
        })

    return JSONResponse(status_code=200, content={
        "message": f"Successfully processed {result['files_received']} file(s).",
        "files_received": result["files_received"],
        "deeds_extracted": result["txt_files_created"],
        "neo4j_stats": result["neo4j_stats"],
        "warnings": result["errors"],   # non-fatal issues
    })