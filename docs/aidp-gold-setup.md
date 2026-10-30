# Initial Gold — release 0.3.0

## 1. Scope, prerequisites and execution

The user reported successful live Silver counts 600 / 30,600 / 30,000 / 94,572,
zero rejects, empty quarantine, zero second-run inserts and unchanged fingerprints.
See [attributed evidence](aidp-silver-execution.md). The agent has not run AIDP
Gold or inspected an AIDP job. **Live Gold execution remains pending.**

Use the existing authenticated **lakehouse_to_ai** AIDP workspace and Spark/Delta
compute. Install `dist/aidp_lakehouse_to_ai-0.3.0-py3-none-any.whl` as a compute
library and restart the Python session. The wheel contains all reusable modules;
no source checkout, notebook monkeypatch or optional OCI dependency is required.
Run [03_build_gold.ipynb](../notebooks/03_build_gold.ipynb) top to bottom in a new
session. Its one configuration cell uses the verified namespace `frgpslcqn7sm`,
bucket `CristinaVaras-bucket-AIDP`, seed 42, initial batch, fixed reference date
**2026-10-28**, and LF v2 source prefix. Do not put credentials in a notebook.

Required: Standard/INTERNAL `lakehouse_demo`, existing `silver` and `gold` schemas,
query/DESCRIBE/HISTORY access to all five managed Silver Delta tables, read access
to the exact v2 `_control/initial/` objects, and CREATE TABLE / SELECT / insert
permissions for managed Gold. The gold schema was created by setup notebook 00
or the equivalent setup in notebook 01; Gold does not reset or recreate catalogs.
Notebook 01 is the normal complete Bronze entry point; 00 is optional catalog-only
setup. Notebook 02 produces Silver; notebook 03 consumes that Silver.

Gold uses AIDP-managed Delta placement, with no explicit LOCATION. Oracle's
[AIDP SQL grammar](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/sql-grammar.html)
documents `CREATE TABLE IF NOT EXISTS ... USING DELTA`; the
[table documentation](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/tables.html)
distinguishes managed and external storage. The publisher uses the
[Delta insert-only MERGE API](https://docs.delta.io/delta-update/).
No generic local CREATE DATABASE substitutes for an AIDP catalog: it appears
only in isolated local fixture setup.

## 2. Outputs and grain

All outputs are managed Delta under `lakehouse_demo.gold`. Contracts, identifiers
and feature roles live in [gold.py](../src/aidp_lakehouse/gold.py); distributed
transforms/validation/publication live in [gold_spark.py](../src/aidp_lakehouse/gold_spark.py).
No business rows are collected to the driver. Only small metadata/aggregate
summaries are returned; joins, grouping, uniqueness, relationship checks and
full-row comparisons execute in Spark. Notebook sample displays are limited to
three rows per table.

| Output | Grain / expected count |
| --- | --- |
| order_operations | One row per accepted order: **30,600** |
| delivery_kpis | One row per UTC completion month, acceptance region, actual carrier; count depends on observed groups |
| late_delivery_training | One row per completed order: **30,000** |
| late_delivery_scoring | One row per open order: **600** |

`order_operations` retains original order acceptance snapshots plus operational
current status/priority, current customer display attributes (explicitly named
`customer_region_current` / `customer_segment_current`), delivery details and
source timestamps/versions/files. Those current/outcome attributes are **not**
model predictors. Events are grouped by order **before** joining: event count,
incident count, sorted unique incident types, first/last incident times and last
event time. Several incidents therefore do not duplicate orders. A missing
lifecycle is rejected instead of silently interpreted as zero events.

`is_late` is BOOLEAN: `delivered_at > promised_delivery_at_acceptance` for a
completed delivery, otherwise NULL. Equality is on time. Open orders retain null
delivery/outcome fields; zero incidents use an empty incident_types array and
NULL first/last incident times. Model scoring has no is_late column.

## 3. KPI dimensions and denominators

- **period_start**: first date of the UTC month containing **delivered_at**,
  not order acceptance, dispatch or the current wall-clock month.
- **customer_region_at_acceptance**: original region snapshot; current customer
  master updates do not rewrite historical groups.
- **actual_carrier**: observed completing carrier, appropriate for descriptive
  operational KPIs only. It is excluded from model predictors.
- **deliveries**: count of completed orders in the group, the sole denominator.
- **late_deliveries**: completed orders where is_late=true.
- **late_delivery_pct**: `100 * late_deliveries / deliveries`, DECIMAL(9,6),
  rounded to six decimal places. No empty denominator groups are manufactured.

Open orders contribute neither to deliveries nor to on-time counts. Summed KPI
deliveries must equal 30,000 and summed late_deliveries must equal the training
label total. Validation also compares every dimensional KPI row, counts and rate
against a fresh aggregation of the Silver inputs. Percentages are not averaged
across groups; a global rate uses the summed numerator/denominator.

## 4. Prediction contract and temporal metadata

Prediction time is **accepted_at**. The explicit predictor allowlist is:

1. customer_segment_at_acceptance — STRING
2. customer_region_at_acceptance — STRING
3. priority_at_acceptance — STRING
4. inventory_fill_rate_at_acceptance — DECIMAL(6,3)
5. warehouse_load_at_acceptance — DECIMAL(6,3)
6. planned_carrier_at_acceptance — STRING
7. transit_distance_km — INT
8. line_count — INT
9. units — INT
10. promised_delivery_at_acceptance — TIMESTAMP UTC
11. accepted_at — TIMESTAMP UTC (also the evaluation/prediction-time metadata)

This matches the generator's original acceptance_features exactly. No feature is
replaced with a current customer value, current priority/status, event, source
update/version, delivery timestamp or actual carrier. Gold does not encode,
normalize or invent predictors. The code selects the explicit names, never
SELECT * or “all columns except the label”.

| Role | Columns |
| --- | --- |
| Identifiers, excluded from predictors | order_id, customer_id |
| Predictors | Exactly the 11 original acceptance attributes above |
| Training label | is_late BOOLEAN, mandatory for every training row |
| Outcome availability metadata, excluded from predictors | outcome_available_at TIMESTAMP |
| Snapshot metadata, excluded from predictors | _batch_id='initial', _reference_date=2026-10-28 |

Training contains identifiers + predictors + label + outcome_available_at +
snapshot metadata. Scoring contains exactly identifiers + the same predictor
names/types + snapshot metadata. It contains **no label, outcome time or invented
probability**. An existing target with extra model columns is incompatible.

`outcome_available_at` is a **conservative source-system availability proxy**:
maximum of delivered_at, the delivery record's source_updated_at, and the
DELIVERED event's source_updated_at. Both lifecycle records must exist. This is
not an invented ingestion timestamp or a claim of actual consumer visibility.
For the generator's initial records it is delivery completion plus one hour.
Later unrelated incident/priority changes do not become predictors. A later
version of a delivery can conservatively postpone availability. Future temporal
evaluation must filter accepted_at into the evaluation window **and** ensure
outcome_available_at is at or before the training cutoff; never split using the
availability timestamp as a predictor. No train/test split or model is built now.

## 5. Validation and safe initial reruns

The runtime checks Standard catalog identity, reads/validates exact v2 control
objects, verifies all five Silver inputs are managed Delta with exact contracts,
and requires an empty quarantine. It pins each input's Delta version before
reading, avoiding changes during lazy evaluation. Run with a single upstream
writer paused: the five input versions are not a cross-table atomic snapshot.
Every source file must be the exact corresponding v2 entity/initial object;
batch_id must remain initial. Gold does not read incremental or source documents.

Distributed checks validate mandatory fields, unique entity keys, one delivery
per order, known customers/orders, source and lifecycle chronology, required
acceptance/dispatch/completion events, labels and allowed nulls. A completed order
without a matching delivery/event, or an open order with an outcome, fails.
No record is silently removed or quarantined in Gold. Silver owns cleaning.

Outputs require unique natural keys and exact ordered names/types. Acceptance
attributes and delivery outcomes are compared back to Silver. Operational order
keys match Silver orders; training keys match deliveries; scoring keys match open
orders. Every row's label, incident aggregation, outcome chronology and snapshot
metadata is checked. Counts reconcile the 30,600 = 30,000 + 600 partition, every
event/incident, and all dimensional KPI totals/rates.

All four existing Gold definitions and contents are inspected **before any
write**. Existing targets must be MANAGED DELTA with exact schemas and unique
non-null keys. Existing rows must be an exact subset of the planned initial
snapshot. Changed, duplicate, extra or incompatible rows stop the run. Managed
locations may not overlap any source-data version. Targets are rechecked before
MERGE. There is no DROP, ALTER, REPLACE, schema evolution, matched-row update,
delete or blanket overwrite. Only missing identical rows are inserted, using
natural keys (all three KPI dimensions for delivery_kpis).

A single writer is required. Delta transactions are per table, not atomic across
four targets. A failed partial run may leave a compatible subset; a rerun inserts
only the missing rows. This is initial-only: future incremental updates need a
separate reviewed history/update policy. Existing future state cannot be reset
by this notebook.

Notebook 03 runs the complete build twice. Every published table must equal the
planned result via exceptAll in both directions; changed business data cannot
hide behind a hash. The second build requires identical validation summaries,
identical distributed order-independent fingerprints and zero new inserted rows.
Fingerprints are diagnostic counts plus four exact decimal sums of SHA-256
64-bit limbs, computed in Spark without collecting row hashes. They are not a
cryptographic substitute for the authoritative full-row comparisons.

## 6. Tests, artifact and attribution

Default tests cover explicit roles, LF v2/reference-date guards and clean notebook
structure. Real local Spark/Delta fixture tests cover multiple incidents, null
open outcomes, exact-promise/early/late labels, actual-vs-planned carrier and
current-vs-original priority, unknown references, duplicates, invalid nulls,
full-row reruns, insert-only managed publication and incompatible-target safety.
Fixtures are separate from cloud and incremental sources. Local CREATE DATABASE,
fixture table writes and negative-test mutations touch temporary local storage
only. None is a live AIDP validation.

Commands from the repository root:

```sh
.venv/bin/python -m unittest discover -s tests -v

JAVA_HOME=/private/tmp/aidp-silver-jdk17/Contents/Home \
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_PYTHON=/private/tmp/aidp-silver-spark-env/bin/python \
AIDP_RUN_SPARK_TESTS=1 \
AIDP_DELTA_JARS=/private/tmp/aidp-silver-jars/delta-spark.jar,/private/tmp/aidp-silver-jars/delta-storage.jar \
PYTHONPATH=src /private/tmp/aidp-silver-spark-env/bin/python \
  -m unittest discover -s tests -p test_gold_spark.py -v

# Offline build using the existing local backend, without modifying the OCI CLI:
PYTHONPATH=/opt/homebrew/Cellar/oci-cli/3.94.0/libexec/lib/python3.14/site-packages \
.venv/bin/python -c "from setuptools.build_meta import build_wheel; print(build_wheel('dist'))"
```

Wheel: `dist/aidp_lakehouse_to_ai-0.3.0-py3-none-any.whl`, on the user's local Mac.
The dist directory, generated datasets and credentials remain outside Git.
Training, executing model scoring, agents, Gold-to-Autonomous publication and
incremental ingestion are future milestones. Creating the scoring **dataset**
does not run inference. Bronze/Silver business logic and all source objects are
preserved. See the separate local verification record for actual executed results.

Actual executed checks and complete local counts are recorded in
[Gold execution](aidp-gold-execution.md), with the machine-readable
[aggregate report](gold-local-validation.json). Those results are local; the
only live AIDP evidence supplied in this milestone concerns Silver.
