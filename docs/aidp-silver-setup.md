# Reproducible initial Silver — version 0.2.0

## 1. Execution evidence and boundaries

The user reported successful **live AIDP Bronze verification** in this conversation:
600 customers, 30,600 orders, 30,000 deliveries, 94,572 events; all counts matched
the uploaded initial manifest, parser-corrupt rows were zero, and source-file
provenance checks passed. These are user-supplied observations, not AIDP queries
executed by this agent. The exact runtime execution timestamp was not supplied.

This milestone independently tests the quality engine against local initial
extracts and tests Spark/Delta locally. **Live AIDP Silver verification remains
pending.** No AIDP catalog/table/data write, Autonomous operation, Gold, ML, agent
or incremental upload/processing was performed by this work.

The existing Bronze fixes are consolidated in the package, with regression tests:
direct `spark.read.text(uri, wholetext=True)`, options from both Table/Storage
Properties, plain/`option.*` aliases, conflicting-value rejection, and INTERNAL
catalog metadata support. No notebook monkeypatch is needed.

## 2. Inputs, outputs and implementation

Run [02_build_silver.ipynb](../notebooks/02_build_silver.ipynb) after the existing
catalog and Bronze notebooks. Inputs are exactly
`lakehouse_demo.bronze.{customers,orders,deliveries,events}`, the verified initial
sources documented in [Object Storage execution](object-storage-execution.md).
Neither source objects nor Bronze definitions/data are changed.

Outputs are AIDP-managed Delta tables:

- `lakehouse_demo.silver.customers`
- `lakehouse_demo.silver.orders`
- `lakehouse_demo.silver.deliveries`
- `lakehouse_demo.silver.events`
- `lakehouse_demo.silver.quarantine`

[silver.py](../src/aidp_lakehouse/silver.py) is a deterministic standard-library
quality engine; [silver_spark.py](../src/aidp_lakehouse/silver_spark.py) handles
Spark schemas, initial Bronze collection, managed-table inspection and Delta
publication. The notebook only configures, calls and verifies those modules.

This is deliberately a **bounded demo implementation**, not a distributed ETL
engine. It collects the four verified initial inputs to the driver, limited by
`MAX_DRIVER_ROWS=250000` total manifest rows (the actual batch has 155,772).
Collection is also capped per entity and checked again against exact manifest
counts and source paths. The row guard is an operational memory limit, not a
business-quality threshold. Use adequate driver memory for the source dictionaries
and typed copies. Port the same contracts to distributed transformations before
scaling beyond this dataset; do not silently increase the guard for production.

## 3. Explicit Silver contracts

Business columns retain the names/order in the generator and
[source contract](source-data.md). Definitions are available programmatically as
`CONTRACTS` and `output_contract(entity)`.

| Fields | Silver type and validation |
| --- | --- |
| Business keys and textual source attributes | STRING; mandatory and nonblank, no trimming or invented identifier rewrite |
| source_created_at, source_updated_at | TIMESTAMP, timezone-aware input normalized to UTC |
| accepted_at, promised_delivery_at_acceptance | TIMESTAMP, original acceptance semantics retained |
| dispatched_at, delivered_at, event_at | TIMESTAMP in UTC where present |
| row_version | BIGINT, positive; signed 64-bit representation bound |
| line_count | INT, 1–12 |
| units | INT, 5–400 |
| transit_distance_km | INT, 50–2400 |
| inventory_fill_rate_at_acceptance | DECIMAL(6,3), 0.450–1.000 |
| warehouse_load_at_acceptance | DECIMAL(6,3), 0.350–1.300 |
| events.payload | MAP<STRING,STRING>; retained, including empty maps |

Numeric ranges are the generator's actual ranges, not inferred commercial risk
thresholds. Decimal input must be finite and representable without rounding at
three decimal places; integer input cannot be silently truncated from a float.
Timestamp text must be a full ISO timestamp with seconds and a timezone (`Z` or
an explicit offset). Invalid calendar dates are rejected, not converted to null.
Spark's session timezone is set to UTC before constructing/writing typed frames.

Enums match generator domains: four regions, STANDARD/PREMIUM segments,
NORMAL/HIGH/URGENT priorities, CARRIER_A/B/C, OPEN/DELIVERED statuses, and
ACCEPTED/DISPATCHED/DELIVERED/INCIDENT_OPENED events. Incident payloads require
the documented CAPACITY/STOCK/TRANSPORT exception type; additional string-valued
map entries are retained. All source contract fields are mandatory; an empty
payload map is valid. A populated Bronze `_corrupt_record` is rejected.

Source creation must not follow source update, and source updates must not be
after the manifest extraction cutoff. Orders require creation <= acceptance <=
source update and acceptance < original promise. Deliveries require creation <=
dispatch <= completion <= update. Events require creation <= event time <= update.
There is no fixed delivery-lead threshold: the generator's open-order promises
do not share the closed cohort's 4–12-day construction rule.

Relationships are validated after local conversion/version checks:

- Orders reference accepted customers.
- OPEN orders have no completed delivery; DELIVERED orders have exactly one.
- Deliveries reference accepted orders; acceptance <= dispatch <= completion <=
  the order's source update. Multiple delivery keys for one order are rejected.
- Events reference accepted orders. Acceptance event times match order acceptance;
  dispatch/delivery events match the corresponding delivery timestamps. Other
  outcome events require a delivery and cannot occur after completion or before
  acceptance.
- Every order needs an acceptance event; completed orders also need dispatch and
  delivered events. Rejection propagates to a fixed point so accepted outputs
  have no dangling references or inconsistent lifecycle dependencies.

Checks operate on the current complete initial snapshot. They do not use later
events as model features, and no model dataset is created. All immutable generator
acceptance features (`FEATURES`, including original priority/promise, inventory,
workload, planned carrier, quantities, distance, customer snapshots and accepted_at)
plus customer_id are compared across valid order versions and cannot change.
Original source creation time is also invariant across versions of each key.

Accepted outputs add `_batch_id`, `_source_file` and `_record_hash`. These are
deterministic provenance/snapshot fields, not invented source or ingestion times.
Business keys, source timestamps, row versions and the events map are preserved.

## 4. Deterministic version selection and quarantine

First, intrinsically invalid records are rejected. Remaining records are grouped
by entity/business key. Precedence is **row_version descending, then normalized
source_updated_at descending**. Identical raw records at the same precedence
collapse; older valid versions are counted as version/duplicate removals. Source
file and canonical raw serialization provide a deterministic representative only
when business content is identical; they never decide a content conflict.

If different raw contents share equal precedence anywhere in a key's valid
history, **all otherwise valid versions of that key are quarantined**. No older
fallback or arbitrary last row is selected. A changed immutable attribute has
the same fail-closed outcome. Relationships are then checked on the selected
snapshot. If a winner is rejected by relationship validation, all its candidate
versions receive those reasons rather than silently reviving an older version.
Intrinsic failures remain separate rejected records, not duplicate removals.

Quarantine contains entity, business_key, batch_id, source_file, raw_record,
rejection_reasons, occurrence_count and rejection_id. `raw_record` is canonical
JSON of the original **Bronze field values**, including the parser-corrupt field
if present; it is not a claim to preserve CSV byte/line formatting. Bronze/source
files remain the immutable byte-level record. Reasons are a sorted unique array,
not exploded into one row per reason.

Identical rejected records from the same source are grouped with an
`occurrence_count`; their stable rejection_id hashes entity, batch, source and
raw record. Thus SUM(occurrence_count) equals rejected source rows even when the
same bad source record is repeated or has several rejection reasons. Quarantine
table row count can be smaller than rejected source-row count. This grouping
avoids unstable file row numbers or nondeterministic Spark IDs on reruns.

For every entity the summary asserts:

`raw_rows = duplicate_removals + rejected_rows + accepted_rows`

Accepted rows are unique per business key; a clean initial build retains all
verified counts and produces an empty quarantine. The AIDP entry point stops
before publication with an explicit summary/quarantine preview if this initial
batch unexpectedly has any rejects or removals. Investigate source/runtime
differences rather than lowering validation or silently publishing a partial
initial snapshot. The pure engine and lower-level publisher support rejected
fixture results, but notebook 02 intentionally enforces the clean initial contract.

## 5. Managed Delta safety and reruns

The publisher inspects **all five existing targets before any write**. A target
must be MANAGED, provider DELTA, with exactly the documented columns/types, and
must not overlap the external source root. Existing null/duplicate keys, changed
rows, unrelated batches or extra rows stop the build. Schema auto-evolution,
ALTER, DROP, replacement tables and blanket overwrite are not used.

Existing contents may be empty or an exact subset of the planned initial
snapshot. This allows recovery from a partial run. Source/target comparisons use
all columns, with maps sorted before JSON comparison; a stale hash cannot hide
a changed business value. New tables use AIDP's `CREATE TABLE IF NOT EXISTS ...
USING DELTA` with explicit types and **no external LOCATION**. Publication uses
Delta MERGE with only `WHEN NOT MATCHED INSERT`: no matched updates/deletes.
Final key, count and full-row checks must match the complete planned snapshot.

Run with a **single writer** for this initial snapshot. Each table has its own
Delta transaction; there is no atomic five-table commit. A later failure can
leave compatible partial output. Rerun inserts only missing identical records;
it never resets targets. Notebook 02 runs the full entry point twice and checks
identical business fingerprints/dispositions, identical final counts and zero
inserts on the second pass.

Future incremental support is intentionally **not implemented**. It must supply
prior accepted state/history, validate immutable acceptance values against that
state, resolve versions across batches, and use a separate reviewed update policy
and quality ledger. The current initial-only publisher rejects extra/changed
target rows instead of overwriting a future incremental state. The input batch
guard rejects `incremental`; no incremental cloud files are read in this work.

## 6. Installation and AIDP execution

Install the new wheel on the same authenticated AIDP compute as Bronze. Continue
using the [existing workspace/IAM/module instructions](aidp-bronze-setup.md).
Additional permissions are CREATE_TABLE on Silver and the permissions needed
for Delta inserts/queries on existing targets. AIDP-managed storage needs its
normal service policies; never point managed Silver placement at source data.

```python
%pip install --no-deps --force-reinstall /Workspace/<folder>/aidp_lakehouse_to_ai-0.2.0-py3-none-any.whl
```

Restart the Python session so older imported helper functions are not cached.
Import/open notebook 02 and edit only its configuration cell to match 00/01.
Run it top to bottom. It prints the installed version, consolidated validation
summary, inferred target definitions/types and samples, and then verifies a
second complete initial build inserted no rows. No third-party packages are
needed for the pure engine; AIDP supplies its Spark/Delta runtime.

Oracle's [AIDP SQL Grammar](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/sql-grammar.html)
documents managed table syntax and currently lists Spark 3.5/Delta 3.2.
The insert-only operation follows the [Delta MERGE API](https://docs.delta.io/delta-update/).
No local Spark CREATE DATABASE operation substitutes for an AIDP catalog: that
command appears only in isolated real-Spark test setup, not in notebook 02.

## 7. Local checks and evidence

Pure tests use `tests/fixtures/silver_initial.json` and copies mutated in memory,
separate from cloud objects and the actual incremental extract. Cases include
invalid dates, invalid/non-finite numeric values, range failures, missing fields,
unknown references/cascades, duplicates, equal-precedence conflicts, changed
acceptance snapshots, UTC offsets and deterministic ordering. These are actual
Python transformations. Existing Bronze guard tests use simulated AIDP metadata;
they do not execute AIDP SQL.

Real local integration tests use Python 3.12, temporary JDK 17, Spark 3.5.6 and
Delta 3.2.0 in an isolated environment under `/private/tmp`; the system Java 26
and project Python 3.14 environment are preserved. They execute actual managed
Delta creates, insert-only MERGE, typed reads, initial rerun, drift rejection,
incompatible-table rejection and quarantine frames. Spark binds locally and no
Object Storage/AIDP connection is used. Default unit discovery skips these two
tests unless explicitly enabled.

Commands from the repository root:

```sh
.venv/bin/python -m unittest discover -s tests -v

# For a prepared Python 3.12 / JDK 17 test environment and local Delta JARs:
JAVA_HOME=/private/tmp/aidp-silver-jdk17/Contents/Home \
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_PYTHON=/private/tmp/aidp-silver-spark-env/bin/python \
AIDP_RUN_SPARK_TESTS=1 \
AIDP_DELTA_JARS=/private/tmp/aidp-silver-jars/delta-spark.jar,/private/tmp/aidp-silver-jars/delta-storage.jar \
PYTHONPATH=src /private/tmp/aidp-silver-spark-env/bin/python \
  -m unittest discover -s tests -p test_silver_spark.py -v

.venv/bin/python -m pip wheel . --no-deps --wheel-dir dist/
```

The pinned Spark/Delta Python dependencies are available as `.[spark-tests]`;
use a compatible test Python/JDK rather than installing them into the Python
3.14 notebook-validation environment. Local Delta JARs can be obtained from the
matching Maven artifacts `io.delta:delta-spark_2.12:3.2.0` and
`io.delta:delta-storage:3.2.0`, or supplied through the standard Delta test setup.
Temporary environments/artifacts can be removed by the OS and are not deployed.

The complete existing initial extract was transformed twice locally with identical
business fingerprints. Real Spark typed-frame counts are recorded in
[silver-local-validation.json](silver-local-validation.json), including the
output data types. These local checks are not a claim of live AIDP Silver success.

Final results: default discovery ran 44 tests, with **42 passed and 2 opt-in
Spark tests skipped**. Those two tests were then run separately on real local
Spark/Delta and both passed. Twelve Silver contract/notebook tests also passed
when importing the modules directly from the built wheel in an isolated Python
process. The complete initial typed-frame check matched all four live-reported
Bronze counts and empty quarantine, with equal business fingerprints across
two local transformations.

The work started from commit `5273190`; a fetch confirmed local and remote
main matched. No in-progress tracked changes existed. The unrelated `.DS_Store`
was preserved and excluded from the commit. Consolidated Bronze code was reused
unchanged, not replaced with notebook patches.

The verified local artifact is
`dist/aidp_lakehouse_to_ai-0.2.0-py3-none-any.whl` inside the repository on the
user's Mac. It contains the current `aidp_bronze.py`, `silver.py` and
`silver_spark.py` bytes and no datasets or credentials. SHA-256:
`2f75070f2f4178a7b1a4b6f748c345bcaa83dbd7d3f9c50bf503979fddcdaff0`.
The `dist/` folder stays ignored by Git; source/notebooks/docs are committed.
