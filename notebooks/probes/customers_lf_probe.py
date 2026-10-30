# Standalone Python cell: paste the complete file into an authenticated AIDP notebook.
# AIDP runtime verification is pending until this cell actually passes there.
# This creates only the isolated customers_lf_probe external metadata, never main tables.
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import re

TABLE = 'lakehouse_demo.bronze.customers_lf_probe'
LOCATION = ('oci://CristinaVaras-bucket-AIDP@frgpslcqn7sm/'
            'demos/lakehouse-to-ai/source-data/v2/seed-42/reference-2026-10-28/customers/initial/')
EXPECTED_FILE = LOCATION + 'customers.csv'
EXPECTED_COUNT = 600
SOURCE_COLUMNS = ['customer_id', 'customer_name', 'region', 'segment',
                  'source_created_at', 'source_updated_at', 'row_version']
RAW_COLUMNS = SOURCE_COLUMNS + ['_corrupt_record']
SCHEMA = StructType([StructField(name, StringType(), True) for name in RAW_COLUMNS])
OPTIONS = {'header': 'true', 'enforceSchema': 'false', 'multiLine': 'false',
           'inferSchema': 'false', 'mode': 'PERMISSIVE', 'encoding': 'UTF-8',
           'delimiter': ',', 'quote': '"', 'escape': '"',
           'columnNameOfCorruptRecord': '_corrupt_record',
           'ignoreLeadingWhiteSpace': 'false', 'ignoreTrailingWhiteSpace': 'false',
           'recursiveFileLookup': 'false'}

# Quote SQL literals only; no data value is stripped, trimmed or converted.
def sql_literal(value):
    return "'" + value.replace('\\', '\\\\').replace("'", "''") + "'"

fields = ', '.join(name + ' STRING' for name in RAW_COLUMNS)
options_sql = ', '.join(key + '=' + sql_literal(value) for key, value in OPTIONS.items())
ddl = (f'CREATE EXTERNAL TABLE IF NOT EXISTS {TABLE} ({fields}) USING CSV '
       f'OPTIONS ({options_sql}) LOCATION {sql_literal(LOCATION)}')
print(ddl)
spark.sql(ddl)

# Existing probe metadata must agree; no DROP, ALTER, REPLACE or main-table changes.
metadata_rows = spark.sql(f'DESCRIBE TABLE EXTENDED {TABLE}').collect()
spark.sql(f'DESCRIBE TABLE EXTENDED {TABLE}').show(100, truncate=False)
metadata = [(str(row[0]).strip().lower().replace('_', ' '), str(row[1]).strip())
            for row in metadata_rows if len(row) >= 2 and row[0] is not None and row[1] is not None]
details = dict(metadata)
assert details.get('type', '').upper() == 'EXTERNAL', 'Probe must be external'
assert details.get('provider', '').lower() == 'csv', 'Probe provider differs'
assert details.get('location', '').rstrip('/') == LOCATION.rstrip('/'), 'Existing probe location differs'
probe_schema = [(f.name, f.dataType.simpleString()) for f in spark.table(TABLE).schema.fields]
assert probe_schema == [(name, 'string') for name in RAW_COLUMNS], 'Existing probe schema differs'

# Validate required options in either property section, including option.* aliases.
required = {'mode': 'permissive', 'multiline': 'false', 'header': 'true',
            'inferschema': 'false', 'enforceschema': 'false'}
seen = {key: [] for key in required}
for attribute, value in metadata:
    if attribute not in ('table properties', 'storage properties'):
        continue
    for key in required:
        pattern = r'(?:^|\[|,\s*)(?:option\.)?' + key + r'\s*=\s*([^,\]]*)'
        seen[key].extend(match.group(1).strip().lower() for match in re.finditer(pattern, value, re.IGNORECASE))
for key, expected in required.items():
    assert seen[key] and set(seen[key]) == {expected}, f'Missing/wrong/conflicting probe option: {key}'

# Materialize both full raw frames so corruption checks do not query only the raw
# parser-corrupt field (a Spark restriction). No row_version cleanup is permitted.
direct = (spark.read.schema(SCHEMA).options(**OPTIONS).csv(LOCATION)
          .withColumn('_source_file', F.input_file_name()).cache())
registered = (spark.table(TABLE).select(*RAW_COLUMNS)
              .withColumn('_source_file', F.input_file_name()).cache())
try:
    direct_count, table_count = direct.count(), registered.count()
    print('Counts:', {'direct': direct_count, 'registered': table_count, 'expected': EXPECTED_COUNT})
    assert direct_count == table_count == EXPECTED_COUNT
    for label, frame in [('direct', direct), ('registered', registered)]:
        corrupt = frame.filter(F.col('_corrupt_record').isNotNull()).count()
        bad_versions = frame.filter(F.col('row_version').isNull() | (F.col('row_version') != F.lit('1'))).count()
        files = [row['_source_file'] for row in frame.select('_source_file').distinct().collect()]
        print(label, 'corrupt rows:', corrupt, 'unexpected row_version values:', bad_versions, 'source files:', files)
        assert corrupt == 0 and bad_versions == 0
        assert files == [EXPECTED_FILE], 'Probe read an unexpected source object'
        for row in frame.select('customer_id', 'row_version').orderBy('customer_id').limit(5).collect():
            print(label, row['customer_id'], 'repr(row_version)=', repr(row['row_version']))
    # Both directions, all source fields plus corruption field, duplicate-aware.
    left_only = registered.select(*RAW_COLUMNS).exceptAll(direct.select(*RAW_COLUMNS)).count()
    right_only = direct.select(*RAW_COLUMNS).exceptAll(registered.select(*RAW_COLUMNS)).count()
    print('Full-row multiset differences:', {'registered_minus_direct': left_only, 'direct_minus_registered': right_only})
    assert left_only == right_only == 0
    print('LF customers probe PASSED in this AIDP session. Main Bronze tables have not been migrated.')
finally:
    direct.unpersist()
    registered.unpersist()
