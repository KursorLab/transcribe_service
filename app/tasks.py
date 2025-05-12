# app/tasks.py

import os
import pkgutil
import importlib
from celery import Celery
from fastapi import HTTPException
from dotenv import load_dotenv

from .processors.base import BaseProcessor
from app.utils.sync_s3 import S3_CLIENT_SYNC

# 1) Load environment
load_dotenv()

# 2) Celery setup with both broker and backend
CELERY_BROKER_URL     = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", CELERY_BROKER_URL)

celery = Celery(
    "tasks",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
)

# 3) Dynamically load processors
PROCESSORS = []
for _, name, _ in pkgutil.iter_modules(['app/processors']):
    mod = importlib.import_module(f"app.processors.{name}")
    for cls in mod.__dict__.values():
        if isinstance(cls, type) and issubclass(cls, BaseProcessor) and cls is not BaseProcessor:
            PROCESSORS.append(cls())

@celery.task(bind=True, name="extract_job")
def extract_job(self, s3_bucket: str, s3_key: str, mime: str, ext: str):
    local_path = f"/tmp/{os.path.basename(s3_key)}"

    # Download
    try:
        S3_CLIENT_SYNC.download_file(s3_key, local_path)
    except Exception as e:
        raise HTTPException(500, f"Error downloading '{s3_key}': {e}")

    # Process & reupload
    for proc in PROCESSORS:
        if proc.can_handle(mime, ext):
            text = proc.process(local_path)
            out_key = f"{s3_key}.txt"
            try:
                S3_CLIENT_SYNC.put_object(
                    object_key=out_key,
                    body=text.encode("utf-8"),
                    content_type="text/plain"
                )
            except Exception as e:
                raise HTTPException(500, f"Error uploading '{out_key}': {e}")
            return {"result_key": out_key}

    raise HTTPException(415, f"No processor for {mime}/{ext}")