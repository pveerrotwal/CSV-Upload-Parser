import hashlib
from pathlib import Path

from fastapi import UploadFile

from app.config import settings


class FileSaveResult:
    def __init__(self, file_hash: str, input_path: str):
        self.file_hash = file_hash
        self.input_path = input_path


async def save_upload_and_hash(upload: UploadFile) -> FileSaveResult:
    data_root = Path(settings.data_dir)
    input_dir = data_root / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    hasher = hashlib.sha256()

    temp_path = input_dir / f"tmp-{upload.filename or 'upload'}.csv"
    with temp_path.open("wb") as temp_file:
        while chunk := await upload.read(1024 * 1024):
            hasher.update(chunk)
            temp_file.write(chunk)

    file_hash = hasher.hexdigest()
    final_path = input_dir / f"{file_hash}.csv"

    if final_path.exists():
        temp_path.unlink(missing_ok=True)
    else:
        temp_path.rename(final_path)

    return FileSaveResult(file_hash=file_hash, input_path=str(final_path))
