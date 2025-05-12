# app/main.py

import uuid
from fastapi import FastAPI, UploadFile, File, HTTPException
from .utils.async_s3 import s3_client
from .tasks import extract_job

app = FastAPI()
BUCKET = s3_client.bucket_name  # pulled from your AsyncS3Client

@app.post("/v1/extract")
async def submit(file: UploadFile = File(...)):
    # derive extension & mime
    ext  = file.filename.rsplit(".", 1)[-1].lower()
    mime = file.content_type

    # generate unique S3 key
    job_id = str(uuid.uuid4())
    key    = f"uploads/{job_id}.{ext}"

    # read file payload
    body = await file.read()
    if not body:
        raise HTTPException(400, "Empty file")

    # upload to S3 via your AsyncS3Client
    try:
        await s3_client.put_object(
            object_key=key,
            body=body,
            content_type=mime
        )
    except Exception as e:
        # logged inside AsyncS3Client; return a 500 to client
        raise HTTPException(500, f"Could not upload to S3: {e}")

    # enqueue your Celery job, passing the same bucket/key
    extract_job.apply_async(
        args=[BUCKET, key, mime, ext],
        task_id=job_id
    )

    return {"job_id": job_id}


@app.get("/v1/extract/{job_id}")
def status(job_id: str):
    async_result = extract_job.AsyncResult(job_id)

    if async_result.state == "PENDING":
        return {"status": "pending"}

    if async_result.state == "SUCCESS":
        return {
            "status":     "done",
            "result_key": async_result.result["result_key"]
        }

    if async_result.state == "FAILURE":
        # bubble up the error message
        raise HTTPException(500, str(async_result.result))

    return {"status": async_result.state}