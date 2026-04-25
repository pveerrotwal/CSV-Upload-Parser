from pathlib import Path

from app.processing import process_csv


def test_process_csv_counts_valid_and_invalid_rows(tmp_path: Path):
    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "output.csv"

    input_file.write_text(
        "date,description,amount,category\n"
        "01/01/24,Salary,95000,Income\n"
        "02/01/24,Uber,NOT_A_NUM,Transport\n"
        "03/01/24,,100,Food\n",
        encoding="utf-8",
    )

    summary = process_csv(str(input_file), str(output_file))

    assert summary["processed_rows"] == 3
    assert summary["valid_rows"] == 1
    assert summary["invalid_rows"] == 2

    output_rows = output_file.read_text(encoding="utf-8").splitlines()
    assert output_rows[0] == "date,description,amount,category"
    assert output_rows[1].startswith("2024-01-01,Salary,95000.00,Income")


def test_process_csv_fails_when_required_columns_missing(tmp_path: Path):
    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "output.csv"

    input_file.write_text("date,description,amount\n01/01/24,Salary,1\n", encoding="utf-8")

    try:
        process_csv(str(input_file), str(output_file))
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "missing required columns" in str(exc)
