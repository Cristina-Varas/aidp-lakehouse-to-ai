"""Spark/Delta adapter for the bounded initial Silver quality engine.

No Spark or Delta imports occur until an execution function is called. Local
tests can exercise the same publisher in a two-part Spark namespace; AIDP uses
the three-part catalog.silver namespace and AIDP-managed placement.
"""
from __future__ import annotations

from dataclasses import dataclass
import json

from .aidp_bronze import (BronzeConfig, ENTITIES, _details, identifier, inspect_table,
                         read_control_documents, require_standard_catalog, verify_bronze)
from .silver import CONTRACTS, KEYS, PROVENANCE, QUARANTINE_SCHEMA, SilverResult, transform_initial


@dataclass(frozen=True)
class SilverConfig:
    bronze: BronzeConfig = BronzeConfig()
    max_driver_rows: int = 250_000  # Operational memory guard, not a business rejection rule.


def output_contract(entity):
    return QUARANTINE_SCHEMA if entity == "quarantine" else CONTRACTS[entity] + PROVENANCE


def spark_schema(entity):
    from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, LongType,
                                   TimestampType, DecimalType, MapType, ArrayType)
    types = {"string":StringType(), "int":IntegerType(), "bigint":LongType(), "timestamp":TimestampType(),
             "decimal(6,3)":DecimalType(6,3), "map<string,string>":MapType(StringType(),StringType()),
             "array<string>":ArrayType(StringType())}
    return StructType([StructField(name,types[dtype],True) for name,dtype in output_contract(entity)])


def result_frames(spark, result: SilverResult):
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Set spark.sql.session.timeZone=UTC before constructing Silver frames")
    return {entity:spark.createDataFrame(result.quarantine if entity == "quarantine" else result.tables[entity],
                                         schema=spark_schema(entity))
            for entity in (*ENTITIES, "quarantine")}


def managed_ddl(namespace, entity):
    for part in namespace.split("."): identifier(part)
    fields = ", ".join(f"{name} {dtype.upper()}" for name,dtype in output_contract(entity))
    return f"CREATE TABLE IF NOT EXISTS {namespace}.{identifier(entity)} ({fields}) USING DELTA"


def _assert_schema(frame, entity):
    actual = [(field.name,field.dataType.simpleString()) for field in frame.schema.fields]
    if actual != output_contract(entity):
        raise ValueError(f"Incompatible {entity} schema; existing table preserved")


def _signatures(frame, entity):
    from pyspark.sql import functions as F
    names = [name for name,_ in output_contract(entity)]
    fields = []
    for name in names:
        value = F.col(name)
        if name == "payload": value = F.map_from_entries(F.array_sort(F.map_entries(value)))
        fields.append(value.alias(name))
    key = "rejection_id" if entity == "quarantine" else KEYS[entity]
    return frame.select(F.col(key).alias("_key"),
                        F.to_json(F.struct(*fields),options={"ignoreNullFields":"false"}).alias("_signature"))


def _preflight_rows(existing, planned, entity):
    """Allow an exact compatible subset (partial-run recovery), never replace drift."""
    from pyspark.sql import functions as F
    have = _signatures(existing,entity)
    want = _signatures(planned,entity)
    if have.filter(F.col("_key").isNull()).limit(1).count() or have.groupBy("_key").count().filter("count <> 1").limit(1).count():
        raise ValueError(f"Existing {entity} has null/duplicate keys; preserved")
    if have.join(want,on=["_key","_signature"],how="left_anti").limit(1).count():
        raise ValueError(f"Existing {entity} contains changed or extra rows; initial-only publication stopped")


def publish_initial(spark, result: SilverResult, namespace: str, *, forbidden_source_root=None):
    """Insert-only managed Delta publication; preflight all targets before any writes.

    Requires a single writer for this initial snapshot. Delta commits are per
    table; recovery of a partial run inserts only missing identical records.
    """
    for part in namespace.split("."): identifier(part)
    from delta.tables import DeltaTable
    frames = result_frames(spark,result)
    existing = set()
    for row in spark.sql(f"SHOW TABLES IN {namespace}").collect():
        values = {key.lower():value for key,value in row.asDict().items()}
        existing.add(values["tablename"])
    before = {}
    # Validate every definition and every existing row before creating any table.
    for entity, planned in frames.items():
        before[entity] = 0
        if entity not in existing: continue
        name = f"{namespace}.{entity}"
        detail = _details(spark.sql(f"DESCRIBE TABLE EXTENDED {name}").collect())
        if detail.get("type", "").upper() != "MANAGED" or detail.get("provider", "").lower() != "delta":
            raise ValueError(f"Existing {entity} is not managed Delta; preserved")
        if forbidden_source_root and detail.get("location", "").rstrip("/").startswith(forbidden_source_root.rstrip("/")):
            raise ValueError("Managed target overlaps source data; publication stopped")
        target = spark.table(name)
        _assert_schema(target,entity)
        _preflight_rows(target,planned,entity)
        before[entity] = target.count()
    output = {}
    for entity, planned in frames.items():
        name = f"{namespace}.{entity}"
        if entity not in existing: spark.sql(managed_ddl(namespace,entity))
        target = spark.table(name)
        _assert_schema(target,entity)
        key = "rejection_id" if entity == "quarantine" else KEYS[entity]
        DeltaTable.forName(spark,name).alias("t").merge(planned.alias("s"),f"t.{key} = s.{key}") \
            .whenNotMatchedInsertAll().execute()
        actual = spark.table(name)
        _preflight_rows(actual,planned,entity)
        count = actual.count()
        expected = len(result.quarantine) if entity == "quarantine" else len(result.tables[entity])
        if count != expected:
            raise ValueError(f"Post-publication {entity} count differs; inspect partial run without resetting tables")
        output[entity] = dict(resulting_silver_rows=count, inserted_rows=count-before[entity],
                              data_types=dict(output_contract(entity)))
    return output


def build_initial_silver(spark, config: SilverConfig):
    """AIDP entry point: inspect/read initial Bronze, transform, validate, publish."""
    from pyspark.sql import functions as F
    cfg = config.bronze
    require_standard_catalog(spark,cfg)
    manifest, _ = read_control_documents(spark,cfg)
    verify_bronze(spark,cfg,manifest)
    total = sum(manifest["row_counts"].values())
    if total > config.max_driver_rows:
        raise ValueError("Initial batch exceeds the configured driver-memory row guard; use a distributed implementation")
    spark.conf.set("spark.sql.session.timeZone","UTC")
    raw = {}
    for entity in ENTITIES:
        inspect_table(spark,cfg,entity)
        # Cap the collection even if sources change after manifest verification.
        rows = spark.table(cfg.table(entity)).withColumn("_source_file",F.input_file_name()) \
            .limit(config.max_driver_rows+1).collect()
        expected = manifest["row_counts"][cfg.file_uri(entity).rsplit("/",1)[1]]
        if len(rows) != expected or any(row["_source_file"] != cfg.file_uri(entity) for row in rows):
            raise ValueError("Bronze changed after verification; no Silver writes attempted")
        raw[entity] = [row.asDict(recursive=True) for row in rows]
    result = transform_initial(raw,batch_id=cfg.batch_id,extracted_at=manifest["extracted_at"])
    if any(s["rejected_rows"] or s["duplicate_removals"] for s in result.summary.values()):
        preview = dict(summary=result.summary,quarantine_preview=result.quarantine[:5])
        raise ValueError("Unexpected initial source-quality differences; investigate before publication: " + json.dumps(preview))
    published = publish_initial(spark,result,f"{cfg.catalog}.silver",forbidden_source_root=cfg.root_uri)
    summary = {entity:{**result.summary[entity],**published[entity]} for entity in ENTITIES}
    return dict(summary=summary,quarantine=published["quarantine"],business_fingerprints=result.business_fingerprints,
                rejected_source_rows=sum(row["occurrence_count"] for row in result.quarantine),
                reconciliation="raw_rows = duplicate_removals + rejected_rows + accepted_rows")
