"""Full-row, duplicate-aware external/direct comparison; no data or metadata writes."""
from .aidp_bronze import columns, reader_options, _details, _reader_metadata_options

def comparison_frame(frame, entity):
    from pyspark.sql import functions as F
    # Spark exceptAll does not support MAP columns. A sorted array of map entries
    # is a lossless comparison representation, including null/empty maps and values.
    fields = []
    for name,_ in columns(entity):
        value = F.col(name)
        if name == "payload": value = F.array_sort(F.map_entries(value))
        fields.append(value.alias(name))
    return frame.select(*fields)


def compare_table(spark, config, entity, *, expected_count, table=None):
    from pyspark.sql import functions as F
    name = table or config.table(entity)
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {name}").collect()
    detail = _details(rows)
    if (detail.get("type", "").upper() != "EXTERNAL" or
            detail.get("provider", "").lower() != ("json" if entity == "events" else "csv")):
        raise ValueError("Comparison requires an EXTERNAL table with the correct provider")
    if detail.get("location", "").rstrip("/") != config.location(entity).rstrip("/"):
        raise ValueError("Comparison requires an already-registered exact configured location")
    options = _reader_metadata_options(rows)
    required = {"mode": "permissive", "multiline": "false"}
    if entity != "events":
        required.update(header="true", inferschema="false", enforceschema="false")
    for key, value in required.items():
        if options.get(key) != value:
            raise ValueError(f"Missing/wrong reader option: {key}")
    schema = ", ".join(f"{key} {dtype}" for key,dtype in columns(entity))
    provider = "json" if entity == "events" else "csv"
    direct = (spark.read.schema(schema).options(**reader_options(entity)).format(provider)
              .load(config.location(entity)).withColumn("_source_file",F.input_file_name()).cache())
    registered = spark.table(name).withColumn("_source_file",F.input_file_name()).cache()
    try:
        counts = [direct.count(), registered.count()]
        if counts != [expected_count]*2: raise ValueError("Direct/registered row counts differ")
        for frame in (direct,registered):
            if [(f.name,f.dataType.simpleString()) for f in frame.schema.fields if f.name != "_source_file"] != columns(entity):
                raise ValueError("Direct/registered raw schema differs")
            if frame.filter(F.col("_corrupt_record").isNotNull()).count():
                raise ValueError("Parser-corrupt rows found")
            expected_version = 1 if entity == "events" else "1"
            if frame.filter(F.col("row_version").isNull() | (F.col("row_version") != F.lit(expected_version))).count():
                raise ValueError("Initial row_version differs, including possible CR contamination")
            files = [r["_source_file"] for r in frame.select("_source_file").distinct().collect()]
            if files != [config.file_uri(entity)]: raise ValueError("Exact source-file provenance differs")
        a,b = comparison_frame(registered,entity),comparison_frame(direct,entity)
        differences = [a.exceptAll(b).count(),b.exceptAll(a).count()]
        if differences != [0,0]: raise ValueError("Full-row duplicate-aware comparison failed")
        return dict(table=name,location=config.location(entity),rows=counts[0],corrupt_rows=0,
                    registered_minus_direct=0,direct_minus_registered=0,
                    source_file=config.file_uri(entity),schema=columns(entity))
    finally:
        direct.unpersist();registered.unpersist()
