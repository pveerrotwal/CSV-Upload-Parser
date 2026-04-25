from pathlib import Path
from datetime import datetime, timedelta, timezone
from uuid import UUID

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import settings
from app.database import Base, engine, get_db_session
from app.models import Job, JobStatus
from app.queue import job_queue
from app.schemas import JobListItem, JobStatusResponse, UploadResponse
from app.storage import save_upload_and_hash
from app.worker_logic import process_job

app = FastAPI(title="CSV Upload Processor")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")
PROCESSING_STALE_AFTER = timedelta(seconds=60)


def mark_stale_processing_jobs(db: Session) -> None:
    now = datetime.now(timezone.utc)
    processing_jobs = db.query(Job).filter(Job.status == JobStatus.processing.value).all()

    dirty = False
    for job in processing_jobs:
        if not job.heartbeat_at:
            continue
        if now - job.heartbeat_at > PROCESSING_STALE_AFTER:
            job.status = JobStatus.failed.value
            job.error_message = "Worker heartbeat timed out. Please re-upload the file."
            dirty = True

    if dirty:
        db.commit()


def parse_job_id_or_404(job_id: str) -> UUID:
    try:
        return UUID(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Job not found") from exc


@app.on_event("startup")
def startup() -> None:
    Base.metadata.create_all(bind=engine)
    Path(settings.data_dir, "input").mkdir(parents=True, exist_ok=True)
    Path(settings.data_dir, "output").mkdir(parents=True, exist_ok=True)


@app.get("/")
def root() -> FileResponse:
    return FileResponse("static/index.html")


@app.post("/upload", response_model=UploadResponse)
async def upload_csv(
    file: UploadFile = File(...), db: Session = Depends(get_db_session)
) -> UploadResponse:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    saved = await save_upload_and_hash(file)

    existing = db.query(Job).filter(Job.file_hash == saved.file_hash).first()
    if existing:
        return UploadResponse(job_id=str(existing.id), status=existing.status, deduplicated=True)

    job = Job(
        file_hash=saved.file_hash,
        original_filename=file.filename,
        input_path=saved.input_path,
        status=JobStatus.queued.value,
    )
    db.add(job)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = db.query(Job).filter(Job.file_hash == saved.file_hash).first()
        if not existing:
            raise HTTPException(status_code=500, detail="Could not create job") from None
        return UploadResponse(job_id=str(existing.id), status=existing.status, deduplicated=True)

    db.refresh(job)

    job_queue.enqueue(process_job, str(job.id), job_id=str(job.id))

    return UploadResponse(job_id=str(job.id), status=job.status, deduplicated=False)


@app.get("/status/{job_id}", response_model=JobStatusResponse)
def get_job_status(job_id: str, db: Session = Depends(get_db_session)) -> JobStatusResponse:
    mark_stale_processing_jobs(db)
    job_uuid = parse_job_id_or_404(job_id)
    job = db.get(Job, job_uuid)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    output_url = (
        f"/download/{job.id}" if job.status == JobStatus.done.value and job.output_path else None
    )

    return JobStatusResponse(
        job_id=str(job.id),
        status=job.status,
        total_rows=job.total_rows,
        processed_rows=job.processed_rows,
        valid_rows=job.valid_rows,
        invalid_rows=job.invalid_rows,
        invalid_reasons=job.invalid_reasons or {},
        output_url=output_url,
        error_message=job.error_message,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


@app.get("/jobs", response_model=list[JobListItem])
def list_jobs(db: Session = Depends(get_db_session)) -> list[JobListItem]:
    mark_stale_processing_jobs(db)
    jobs = db.query(Job).order_by(Job.created_at.desc()).all()
    result = []
    for job in jobs:
        output_url = (
            f"/download/{job.id}"
            if job.status == JobStatus.done.value and job.output_path
            else None
        )
        result.append(
            JobListItem(
                job_id=str(job.id),
                status=job.status,
                total_rows=job.total_rows,
                processed_rows=job.processed_rows,
                valid_rows=job.valid_rows,
                invalid_rows=job.invalid_rows,
                invalid_reasons=job.invalid_reasons or {},
                output_url=output_url,
                error_message=job.error_message,
                created_at=job.created_at,
                updated_at=job.updated_at,
            )
        )
    return result


@app.get("/download/{job_id}")
def download_output(job_id: str, db: Session = Depends(get_db_session)) -> FileResponse:
    job_uuid = parse_job_id_or_404(job_id)
    job = db.get(Job, job_uuid)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != JobStatus.done.value or not job.output_path:
        raise HTTPException(status_code=400, detail="Job is not finished")

    output_path = Path(job.output_path)
    if not output_path.exists():
        raise HTTPException(status_code=404, detail="Output file missing")

    return FileResponse(
        path=str(output_path),
        filename=f"processed-{job_id}.csv",
        media_type="text/csv",
    )
