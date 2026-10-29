"""AIDP-specific DDL and read-only Bronze verification, without local credentials.

The supplied Spark session must belong to an authenticated AIDP notebook.
Importing this module does not require PySpark or make a cloud connection.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import re

CSV_COLUMNS = {
    "customers": "customer_id customer_name region segment source_created_at source_updated_at row_version".split(),
    "orders": (
        "order_id customer_id accepted_at promised_delivery_at_acceptance "
        "customer_segment_at_acceptance customer_region_at_acceptance priority_at_acceptance "
        "inventory_fill_rate_at_acceptance warehouse_load_at_acceptance planned_carrier_at_acceptance "
        "transit_distance_km line_count units current_priority current_status "
        "source_created_at source_updated_at row_version"
    ).split(),
    "deliveries": (
        "delivery_id order_id dispatched_at delivered_at actual_carrier "
        "source_created_at source_updated_at row_version"
    ).split(),
}
EVENT_COLUMNS = [
    ("event_at", "string"), ("event_id", "string"), ("event_type", "string"),
    ("order_id", "string"), ("payload", "map<string,string>"),
    ("row_version", "bigint"), ("source_created_at", "string"),
    ("source_updated_at", "string"),
]
ENTITIES = ("customers", "orders", "deliveries", "events")
CORRUPT = "_corrupt_record"


def identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ValueError("Use a lowercase SQL identifier without punctuation")
    return value


def literal(value: str) -> str:
    # Escaping backslashes makes quote/escape reader options unambiguous in SQL.
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


@dataclass(frozen=True)
class BronzeConfig:
    catalog: str = "lakehouse_demo"
    namespace: str = "frgpslcqn7sm"
    bucket: str = "CristinaVaras-bucket-AIDP"
    prefix: str = "demos/lakehouse-to-ai/source-data/v1/seed-42/reference-2026-10-28"
    reference_date: str = "2026-10-28"
    seed: int = 42
    batch_id: str = "initial"

    def __post_init__(self):
        identifier(self.catalog)
        if self.reference_date != "2026-10-28" or self.batch_id != "initial":
            raise ValueError("This milestone is fixed to the 2026-10-28 initial batch")
        if not re.fullmatch(r"[a-z0-9]+", self.namespace):
            raise ValueError("Use the verified Object Storage namespace, not the tenancy name")
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", self.bucket):
            raise ValueError("Invalid bucket name")
        if (not self.prefix or self.prefix != self.prefix.strip("/") or
                any(not re.fullmatch(r"[A-Za-z0-9_.=-]+", part) or part in (".", "..")
                    for part in self.prefix.split("/"))):
            raise ValueError("Use a literal object prefix without wildcards or traversal")

    @property
    def root_uri(self) -> str:
        return f"oci://{self.bucket}@{self.namespace}/{self.prefix}"

    def table(self, entity: str) -> str:
        if entity not in ENTITIES:
            raise ValueError("Unknown Bronze entity")
        return f"{self.catalog}.bronze.{entity}"

    def location(self, entity: str) -> str:
        self.table(entity)
        return f"{self.root_uri}/{entity}/initial/"

    def file_uri(self, entity: str) -> str:
        suffix = "jsonl" if entity == "events" else "csv"
        return self.location(entity) + f"{entity}.{suffix}"

    def control_uri(self, filename: str) -> str:
        if filename not in ("manifest.json", "ingestion_metadata.json"):
            raise ValueError("Unknown control document")
        return f"{self.root_uri}/_control/initial/{filename}"


def columns(entity: str) -> list[tuple[str, str]]:
    if entity == "events":
        raw = list(EVENT_COLUMNS)
    elif entity in CSV_COLUMNS:
        raw = [(name, "string") for name in CSV_COLUMNS[entity]]
    else:
        raise ValueError("Unknown Bronze entity")
    return raw + [(CORRUPT, "string")]


def reader_options(entity: str) -> dict[str, str]:
    result = dict(encoding="UTF-8", multiLine="false", mode="PERMISSIVE",
                  columnNameOfCorruptRecord=CORRUPT, recursiveFileLookup="false")
    if entity == "events":
        result.update(allowComments="false", allowSingleQuotes="false")
    elif entity in CSV_COLUMNS:
        result.update(header="true", inferSchema="false", delimiter=",", quote='"', escape='"',
                      enforceSchema="false", ignoreLeadingWhiteSpace="false", ignoreTrailingWhiteSpace="false")
    else:
        raise ValueError("Unknown Bronze entity")
    return result


def catalog_ddl(config: BronzeConfig) -> list[str]:
    # Oracle's current sample uses this minimal standard-catalog form; plain
    # Apache Spark has no CREATE CATALOG statement. Do not replace with databases.
    return [f"CREATE CATALOG IF NOT EXISTS {config.catalog}"] + [
        f"CREATE SCHEMA IF NOT EXISTS {config.catalog}.{schema}"
        for schema in ("bronze", "silver", "gold")
    ]


def table_ddl(config: BronzeConfig, entity: str) -> str:
    fields = ",\n  ".join(f"{name} {dtype.upper()}" for name, dtype in columns(entity))
    options = ",\n  ".join(f"{key} = {literal(value)}" for key, value in reader_options(entity).items())
    provider = "JSON" if entity == "events" else "CSV"
    return (f"CREATE EXTERNAL TABLE IF NOT EXISTS {config.table(entity)} (\n  {fields}\n)\n"
            f"USING {provider}\nOPTIONS (\n  {options}\n)\nLOCATION {literal(config.location(entity))}")


def _details(rows) -> dict[str, str]:
    return {str(row[0]).strip().lower(): str(row[1]).strip() for row in rows
            if len(row) >= 2 and row[0] is not None and row[1] is not None}


def require_standard_catalog(spark, config: BronzeConfig):
    rows = spark.sql(f"DESCRIBE CATALOG {config.catalog}").collect()
    detail = _details(rows)
    kind = detail.get("type", detail.get("catalog type", "")).lower().replace("_", " ")
    if kind not in ("standard catalog", "standard", "catalog"):
        raise ValueError("Catalog is not confirmed Standard; stop and inspect it in Master Catalog")
    return rows


def ensure_catalog(spark, config: BronzeConfig) -> None:
    statements = catalog_ddl(config)
    spark.sql(statements[0])
    require_standard_catalog(spark, config)
    for statement in statements[1:]:
        spark.sql(statement)


def read_control_documents(spark, config: BronzeConfig) -> tuple[dict, dict]:
    def read_one(name):
        # Read exactly the metadata object as text, never register a control table.
        rows = spark.read.option("wholetext", "true").text(config.control_uri(name)).collect()
        if len(rows) != 1:
            raise ValueError("Expected one complete control document")
        return json.loads(rows[0]["value"])
    manifest, metadata = read_one("manifest.json"), read_one("ingestion_metadata.json")
    validate_control_documents(config, manifest, metadata)
    return manifest, metadata


def validate_control_documents(config: BronzeConfig, manifest: dict, metadata: dict) -> None:
    if (manifest["batch_id"] != config.batch_id or metadata["batch_id"] != config.batch_id or
            manifest["config"]["reference_date"] != config.reference_date or
            manifest["config"]["seed"] != config.seed or manifest.get("defects") != []):
        raise ValueError("Control documents do not describe the configured clean initial batch")
    if manifest["extracted_at"] != metadata["extracted_at"]:
        raise ValueError("Control extraction timestamps disagree")
    for entity in ENTITIES:
        name = config.file_uri(entity).rsplit("/", 1)[1]
        if (manifest["row_counts"][name] != metadata["files"][name]["rows"] or
                manifest["sha256"][name] != metadata["files"][name]["sha256"]):
            raise ValueError("Control row counts/checksums disagree")


def inspect_table(spark, config: BronzeConfig, entity: str) -> dict[str, str]:
    detail = _details(spark.sql(f"DESCRIBE TABLE EXTENDED {config.table(entity)}").collect())
    expected_provider = "json" if entity == "events" else "csv"
    if (detail.get("type", "").upper() != "EXTERNAL" or
            detail.get("provider", "").lower() != expected_provider or
            detail.get("location", "").rstrip("/") != config.location(entity).rstrip("/")):
        raise ValueError(f"Existing {entity} type/provider/location differs; preserved without modification")
    schema = spark.table(config.table(entity)).schema
    actual = [(field.name, field.dataType.simpleString()) for field in schema.fields]
    if actual != columns(entity):
        raise ValueError(f"Existing {entity} raw column schema differs; preserved without modification")
    # A conflicting DROPMALFORMED option would undermine raw Bronze semantics.
    stored = detail.get("storage properties", "")
    required = {"mode": "PERMISSIVE", "multiLine": "false"}
    if entity != "events":
        required.update(header="true", inferSchema="false")
    for key, value in required.items():
        pattern = r"(?:^|\[|,\s*)" + re.escape(key) + r"\s*=\s*" + re.escape(value) + r"(?=,|\]|$)"
        if not re.search(pattern, stored, re.IGNORECASE):
            raise ValueError(f"Cannot confirm {entity} reader option {key}; inspect DESCRIBE output, do not replace the table")
    return detail


def register_bronze(spark, config: BronzeConfig) -> list[dict]:
    require_standard_catalog(spark, config)
    existing = set()
    for row in spark.sql(f"SHOW TABLES IN {config.catalog}.bronze").collect():
        values = {key.lower(): value for key, value in row.asDict().items()}
        if "tablename" not in values:
            raise ValueError("Unexpected SHOW TABLES response; inspect it before registering tables")
        existing.add(values["tablename"])
    # Inspect all existing target tables before creating any absent ones.
    for entity in ENTITIES:
        if entity in existing:
            inspect_table(spark, config, entity)
    result = []
    for entity in ENTITIES:
        if entity not in existing:
            spark.sql(table_ddl(config, entity))
        inspect_table(spark, config, entity)
        result.append(dict(table=config.table(entity), location=config.location(entity),
                           action="PRESERVED" if entity in existing else "REGISTERED"))
    return result


def verify_bronze(spark, config: BronzeConfig, manifest: dict) -> list[dict]:
    """Compare raw counts and exact file provenance; no casts, row filters or writes."""
    results = []
    for entity in ENTITIES:
        inspect_table(spark, config, entity)
        key = "event_id" if entity == "events" else CSV_COLUMNS[entity][0]
        row = spark.sql(
            f"SELECT COUNT(*) AS row_count, COUNT({key}) AS non_null_business_keys, "
            f"SUM(CASE WHEN {CORRUPT} IS NOT NULL THEN 1 ELSE 0 END) AS malformed_rows, "
            f"MIN(source_created_at) AS first_source_created_at, "
            f"MAX(source_updated_at) AS last_source_updated_at FROM {config.table(entity)}"
        ).collect()[0].asDict()
        name = config.file_uri(entity).rsplit("/", 1)[1]
        expected = manifest["row_counts"][name]
        row.update(table=config.table(entity), expected_rows=expected, batch_id=config.batch_id,
                   reference_date=config.reference_date, extracted_at=manifest["extracted_at"])
        if row["row_count"] != expected or row["malformed_rows"] != 0:
            raise ValueError(f"Raw count or parser corruption differs for {entity}; no records were cleaned")
        source_rows = spark.sql(
            f"SELECT input_file_name() AS source_file, COUNT(*) AS row_count "
            f"FROM {config.table(entity)} GROUP BY input_file_name()"
        ).collect()
        if len(source_rows) != 1 or source_rows[0]["source_file"] != config.file_uri(entity):
            raise ValueError(f"Unexpected source files for {entity}; inspect the registration without rewriting sources")
        results.append(row)
    return results


def provenance_sample_sql(config: BronzeConfig, entity: str, extracted_at: str) -> str:
    return (f"SELECT {literal(config.batch_id)} AS _batch_id_from_path, "
            f"{literal(config.reference_date)} AS _reference_date, "
            f"{literal(extracted_at)} AS _extracted_at_from_manifest, "
            f"input_file_name() AS _source_file, source_created_at, source_updated_at, row_version "
            f"FROM {config.table(entity)} LIMIT 5")
