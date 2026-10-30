"""Explicit prepare/apply/restore phases for external Bronze metadata only.

No function runs on import. Preparation performs read-only Spark operations and
writes an exact runtime snapshot/restore script to a driver-visible directory.
Apply/restore must be invoked separately; neither writes Object Storage data.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import re

from .aidp_bronze import (BronzeConfig, ENTITIES, _details, columns, literal, reader_options,
                         require_standard_catalog, table_ddl, _reader_metadata_options)

V1 = "demos/lakehouse-to-ai/source-data/v1/seed-42/reference-2026-10-28"
V2 = V1.replace("/v1/", "/v2/")
TARGET = BronzeConfig(prefix=V2)
EXPECTED = dict(zip(ENTITIES, (600, 30600, 30000, 94572)))
PROBE = "lakehouse_demo.bronze.customers_lf_probe"
NAMES = {TARGET.table(entity): entity for entity in ENTITIES} | {PROBE: "customers"}


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def _identity(spark):
    require_standard_catalog(spark, TARGET)
    rows = spark.sql("SELECT current_user() AS migration_principal").collect()
    return dict(principal=rows[0]["migration_principal"], application_id=spark.sparkContext.applicationId,
                connection_label="lakehouse_to_ai", catalog=TARGET.catalog,
                namespace=TARGET.namespace, bucket=TARGET.bucket)


def _names(spark):
    return {row.asDict()["tableName"] for row in spark.sql("SHOW TABLES IN lakehouse_demo.bronze").collect()}


def inspect_external(spark, name):
    if name not in NAMES:
        raise ValueError("Table is outside the exact migration allowlist")
    entity = NAMES[name]
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {name}").collect()
    detail = _details(rows)
    _reader_metadata_options(rows)  # Reject conflicting aliases even on old definitions.
    if detail.get("type", "").upper() != "EXTERNAL":
        raise ValueError(f"{name} is not confirmed EXTERNAL; no metadata removal is permitted")
    if detail.get("provider", "").lower() != ("json" if entity == "events" else "csv"):
        raise ValueError("Unexpected provider; preserve the existing object")
    schema = [(field.name,field.dataType.simpleString()) for field in spark.table(name).schema.fields]
    if schema != columns(entity):
        raise ValueError("Unexpected raw schema; preserve the existing object")
    allowed = {TARGET.location(entity).rstrip("/")}
    if name != PROBE: allowed.add(BronzeConfig(prefix=V1).location(entity).rstrip("/"))
    if detail.get("location", "").rstrip("/") not in allowed:
        raise ValueError("Location is outside the exact v1/v2 entity initial directories")
    # Check qualified identity whenever these DESCRIBE fields are exposed.
    for key,expected in (("catalog","lakehouse_demo"),("database","bronze"),("table",name.rsplit(".",1)[1])):
        if key in detail and detail[key] != expected:
            raise ValueError("DESCRIBE identity disagrees with the requested qualified table")
    return dict(name=name, entity=entity, detail=detail, schema=[list(x) for x in schema],
                describe_rows=[list(row) for row in rows])


def _exact_ddl(spark, name):
    # No guessed recreation from a presumed v1 template. If this runtime cannot
    # export DDL, stop here; obtain exact DDL through ask-aidp before proceeding.
    rows = spark.sql(f"SHOW CREATE TABLE {name}").collect()
    ddl = "\n".join(str(row[0]) for row in rows).strip().rstrip(";")
    plain = ddl.replace("`", "")
    pattern = r"^CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?" + re.escape(name) + r"(?=\s|\()"
    if not re.match(pattern, plain, re.IGNORECASE):
        raise ValueError("Exported DDL does not identify the exact allowlisted qualified table")
    # Spark SHOW CREATE may omit EXTERNAL for a LOCATION-backed external table.
    # The existing type was independently confirmed EXTERNAL; make it explicit.
    ddl = re.sub(r"^CREATE\s+TABLE\b", "CREATE EXTERNAL TABLE", ddl, count=1, flags=re.IGNORECASE)
    return ddl


def compare_v2(spark, entity, *, table=None):
    from .bronze_validation import compare_table
    return compare_table(spark, TARGET, entity, expected_count=EXPECTED[entity], table=table)


def _check_sources(spark):
    # Read all four v2 sources before mutation; validate counts/corruption/schema
    # through a direct reader only (the probe supplies the known customer result).
    from pyspark.sql import functions as F
    for entity in ENTITIES:
        schema = ", ".join(f"{key} {dtype}" for key,dtype in columns(entity))
        frame = (spark.read.schema(schema).options(**reader_options(entity)).format("json" if entity=="events" else "csv")
                 .load(TARGET.location(entity)).withColumn("_source_file",F.input_file_name()).cache())
        try:
            if frame.count() != EXPECTED[entity] or frame.filter(F.col("_corrupt_record").isNotNull()).count():
                raise ValueError("Direct v2 source failed preparation checks")
            if [r["_source_file"] for r in frame.select("_source_file").distinct().collect()] != [TARGET.file_uri(entity)]:
                raise ValueError("Direct v2 source path differs")
        finally: frame.unpersist()

def prepare(spark, directory):
    """Read-only platform inspection; durable restore files precede every mutation."""
    directory = Path(directory).expanduser().resolve()
    if any((parent/".git").exists() for parent in [directory, *directory.parents]):
        raise ValueError("Backup must be outside any Git working tree")
    if directory.exists(): raise ValueError("Use a new backup directory; never overwrite a previous snapshot")
    identity = _identity(spark)
    visible = _names(spark)
    if not set(ENTITIES) <= visible: raise ValueError("All four canonical tables must exist before preparation")
    snapshots = {}
    for name in NAMES:
        if name.rsplit(".",1)[1] not in visible: continue
        snapshot = inspect_external(spark,name)
        snapshot["ddl"] = _exact_ddl(spark,name)
        snapshots[name] = snapshot
    if PROBE in snapshots:
        compare_v2(spark,"customers",table=PROBE)
        if snapshots[TARGET.table("customers")]["detail"]["location"].rstrip("/") == TARGET.location("customers").rstrip("/"):
            raise ValueError("Canonical customers and probe both claim v2; inspect overlapping registrations first")
    _check_sources(spark)
    plan = dict(format_version=1, identity=identity, snapshots=snapshots,
                target_config=asdict(TARGET), expected_counts=EXPECTED)
    plan["sha256"] = _hash(plan)
    directory.mkdir(mode=0o700,parents=True)
    (directory/"snapshot.json").write_text(json.dumps(plan,indent=2)+"\n")
    # Exact recorded DDL only; safe removal guards live in restore(), not blind SQL.
    statements = ["-- Recreate only after guarded metadata removal by restore(). No PURGE or data writes."]
    statements += [snapshots[TARGET.table(e)]["ddl"]+";" for e in ENTITIES]
    if PROBE in snapshots: statements.append(snapshots[PROBE]["ddl"]+";")
    (directory/"restore_definitions.sql").write_text("\n\n".join(statements)+"\n")
    (directory/"restore.py").write_text("# Run in the original AIDP session after reviewing snapshot.json.\n"
        "from aidp_lakehouse.bronze_migration import restore\n"
        f"restore(spark, {str(directory)!r})\n")
    for file in directory.iterdir(): file.chmod(0o600)
    return dict(backup_directory=str(directory),snapshot_sha256=plan["sha256"],
                inspected_tables=list(snapshots),platform_changes=0)


def _load(spark, directory):
    directory = Path(directory).expanduser().resolve()
    plan = json.loads((directory/"snapshot.json").read_text())
    checksum = plan.pop("sha256")
    if _hash(plan) != checksum or plan["identity"] != _identity(spark):
        raise ValueError("Snapshot changed or runtime principal/application identity differs; re-prepare")
    if plan["target_config"] != asdict(TARGET) or set(plan["snapshots"])-set(NAMES):
        raise ValueError("Snapshot target scope differs")
    required_sql = "\n\n".join(["-- Recreate only after guarded metadata removal by restore(). No PURGE or data writes."] +
        [plan["snapshots"][TARGET.table(e)]["ddl"]+";" for e in ENTITIES] +
        ([plan["snapshots"][PROBE]["ddl"]+";"] if PROBE in plan["snapshots"] else []))+"\n"
    if (directory/"restore_definitions.sql").read_text() != required_sql:
        raise ValueError("Restoration definitions changed or are missing")
    return directory,plan


def _same_snapshot(spark, snapshot):
    current = inspect_external(spark,snapshot["name"])
    if _hash(current) != _hash({key:value for key,value in snapshot.items() if key != "ddl"}):
        raise ValueError("Table definition changed after preparation; stop and re-prepare")


def migrate(spark, directory):
    """Explicit invocation only. Metadata DDL is not a cross-table transaction."""
    directory,plan = _load(spark,directory)
    visible = _names(spark)
    if ("customers_lf_probe" in visible) != (PROBE in plan["snapshots"]):
        raise ValueError("Probe registration changed after preparation; re-prepare")
    for snapshot in plan["snapshots"].values(): _same_snapshot(spark,snapshot)
    journal = []
    def record(action,name):
        journal.append(dict(action=action,table=name))
        (directory/"migration_journal.json").write_text(json.dumps(journal,indent=2)+"\n")
    if PROBE in plan["snapshots"]:
        inspect_external(spark,PROBE)
        spark.sql(f"DROP TABLE {PROBE}");record("DROP_EXTERNAL_METADATA",PROBE)
    results = []
    for entity in ENTITIES:
        name = TARGET.table(entity)
        state = inspect_external(spark,name)
        if state["detail"]["location"].rstrip("/") != TARGET.location(entity).rstrip("/"):
            # Recheck exact type/name/path immediately before metadata-only removal.
            _same_snapshot(spark,plan["snapshots"][name])
            spark.sql(f"DROP TABLE {name}");record("DROP_EXTERNAL_METADATA",name)
            spark.sql(table_ddl(TARGET,entity));record("CREATE_V2_EXTERNAL_METADATA",name)
        results.append(compare_v2(spark,entity))
    (directory/"verification.json").write_text(json.dumps(results,indent=2)+"\n")
    return results


def restore(spark, directory):
    """Manual rollback only: restore actual captured definitions, not guessed v1 DDL."""
    directory,plan = _load(spark,directory)
    visible = _names(spark)
    if "customers_lf_probe" in visible and PROBE not in plan["snapshots"]:
        raise ValueError("An unexpected probe appeared; preserve it and inspect manually")
    # Preserve original definitions. Only replace exact known v2 external targets.
    to_restore = []
    for name,snapshot in plan["snapshots"].items():
        if name.rsplit(".",1)[1] not in visible:
            to_restore.append(name);continue
        state = inspect_external(spark,name)
        if _hash(state) == _hash({key:value for key,value in snapshot.items() if key != "ddl"}): continue
        if name == PROBE or state["detail"]["location"].rstrip("/") != TARGET.location(NAMES[name]).rstrip("/"):
            raise ValueError("Restore found an unexpected object; preserve it and inspect manually")
        # Require our full v2 contract/options before authorizing metadata removal.
        from .aidp_bronze import inspect_table
        inspect_table(spark,TARGET,NAMES[name])
        to_restore.append(name)
    # Remove all changed canonical external metadata first to free probe's v2 path.
    for name in to_restore:
        if name.rsplit(".",1)[1] in visible:
            inspect_external(spark,name);spark.sql(f"DROP TABLE {name}")
    for name in to_restore:
        spark.sql(plan["snapshots"][name]["ddl"])
    return dict(restored_definitions=to_restore,source_objects_modified=0)
