import csv
import re
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

REQUIRED_COLUMNS = ["date", "description", "amount", "category"]


def _normalize_date(value: str) -> str:
    text = (value or "").strip()
    formats = ["%d/%m/%y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError("invalid date")


def _normalize_amount(value: str) -> str:
    text = (value or "").strip()
    cleaned = re.sub(r"[^0-9\-.]", "", text)
    if cleaned in {"", "-", ".", "-."}:
        raise ValueError("invalid amount")
    try:
        decimal_value = Decimal(cleaned)
    except InvalidOperation as exc:
        raise ValueError("invalid amount") from exc
    return f"{decimal_value:.2f}"


def _clean_text(value: str) -> str:
    text = (value or "").strip()
    if not text:
        raise ValueError("blank field")
    return text


def count_data_rows(input_path: str) -> int:
    with Path(input_path).open("r", newline="", encoding="utf-8") as source:
        reader = csv.reader(source)
        rows = list(reader)
    if not rows:
        return 0
    return max(0, len(rows) - 1)


def process_csv(input_path: str, output_path: str, progress_callback=None):
    input_file = Path(input_path)
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    invalid_reasons = Counter()
    processed_rows = 0
    valid_rows = 0
    invalid_rows = 0

    with input_file.open("r", newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        fieldnames = [name.strip().lower() for name in (reader.fieldnames or [])]

        if not set(REQUIRED_COLUMNS).issubset(fieldnames):
            missing = sorted(set(REQUIRED_COLUMNS) - set(fieldnames))
            raise ValueError(f"missing required columns: {', '.join(missing)}")

        with output_file.open("w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=REQUIRED_COLUMNS)
            writer.writeheader()

            for raw_row in reader:
                processed_rows += 1

                row = {k.strip().lower(): (v or "") for k, v in raw_row.items()}

                try:
                    clean_row = {
                        "date": _normalize_date(row.get("date", "")),
                        "description": _clean_text(row.get("description", "")),
                        "amount": _normalize_amount(row.get("amount", "")),
                        "category": _clean_text(row.get("category", "")),
                    }
                    writer.writerow(clean_row)
                    valid_rows += 1
                except ValueError as exc:
                    invalid_rows += 1
                    invalid_reasons[str(exc)] += 1

                if progress_callback:
                    progress_callback(processed_rows)

    return {
        "processed_rows": processed_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "invalid_reasons": dict(invalid_reasons),
    }
