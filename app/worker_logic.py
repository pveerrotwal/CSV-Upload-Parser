from pathlib import Path
from datetime import datetime, timezone

from app.config import settings
from app.database import SessionLocal
from app.models import Job, JobStatus
from app.processing import count_data_rows, process_csv


def process_job(job_id: str) -> None:
    db = SessionLocal()

    try:
        job = db.get(Job, job_id)
        if not job:
            return

        if job.status == JobStatus.done.value:
            return

        job.status = JobStatus.processing.value
        job.error_message = None
        job.heartbeat_at = datetime.now(timezone.utc)
        db.commit()

        total_rows = count_data_rows(job.input_path)
        job.total_rows = total_rows
        db.commit()

        output_dir = Path(settings.data_dir) / "output"
        output_path = output_dir / f"{job.id}.csv"

        def progress_callback(processed_rows: int) -> None:
            progress_session = SessionLocal()
            try:
                fresh = progress_session.get(Job, job.id)
                if not fresh:
                    return
                fresh.processed_rows = processed_rows
                fresh.heartbeat_at = datetime.now(timezone.utc)
                progress_session.commit()
            finally:
                progress_session.close()

        summary = process_csv(job.input_path, str(output_path), progress_callback=progress_callback)

        refreshed = db.get(Job, job.id)
        if not refreshed:
            return
        refreshed.status = JobStatus.done.value
        refreshed.output_path = str(output_path)
        refreshed.processed_rows = summary["processed_rows"]
        refreshed.valid_rows = summary["valid_rows"]
        refreshed.invalid_rows = summary["invalid_rows"]
        refreshed.invalid_reasons = summary["invalid_reasons"]
        refreshed.heartbeat_at = datetime.now(timezone.utc)
        db.commit()
    except Exception as exc:  # noqa: BLE001
        failed = db.get(Job, job_id)
        if failed:
            failed.status = JobStatus.failed.value
            failed.error_message = str(exc)
            failed.heartbeat_at = datetime.now(timezone.utc)
            db.commit()
    finally:
        db.close()
