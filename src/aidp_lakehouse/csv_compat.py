"""Byte and ordered-field verification for the CRLF-to-LF transport-only change."""
import csv
import io
from pathlib import Path

from .generator import digest


def compare_csv_terminators(original: Path, replacement: Path) -> dict:
    old, new = original.read_bytes(), replacement.read_bytes()
    old_rows = list(csv.reader(io.StringIO(old.decode("utf-8"), newline="")))
    new_rows = list(csv.reader(io.StringIO(new.decode("utf-8"), newline="")))
    if old_rows != new_rows:
        raise ValueError("CSV ordered records/headers/field whitespace differ")
    if old.replace(b"\r\n", b"\n") != new:
        raise ValueError("CSV bytes differ beyond CRLF-to-LF replacement")
    if b"\r" in new or not new.endswith(b"\n"):
        raise ValueError("Expected explicit LF records without carriage returns")
    # The synthetic extracts contain no embedded newlines in fields.
    if new.count(b"\n") != len(new_rows):
        raise ValueError("Unexpected multiline CSV records in generated extract")
    return dict(data_rows=len(new_rows)-1, ordered_records_equal=True,
                field_whitespace_preserved=True, only_record_terminators_changed=True,
                original_bytes=len(old), lf_bytes=len(new),
                original_crlf_count=old.count(b"\r\n"), lf_count=new.count(b"\n"),
                lf_carriage_return_count=new.count(b"\r"),
                original_sha256=digest(original), lf_sha256=digest(replacement))
