from datetime import datetime

from pydantic import BaseModel


class UploadResponse(BaseModel):
    job_id: str
    status: str
    deduplicated: bool


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    total_rows: int
    processed_rows: int
    valid_rows: int
    invalid_rows: int
    invalid_reasons: dict[str, int]
    output_url: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime


class JobListItem(JobStatusResponse):
    pass
