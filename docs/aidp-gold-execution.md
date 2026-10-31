> Update, 31 October 2026: the user reports live AIDP Gold reconciliation PASS,
> unchanged fingerprints on two executions, zero extra inserts, counts
> 30,600 / 296 / 30,000 / 600 and 9,816 late deliveries. The agent subsequently
> corroborated the four counts in the approved read-only diagnostic.
> See [runtime evidence](aidp-training-connection.md). Current wheel **0.4.0**
> adds training and the notebook 03 PASS-state guard; Gold business contracts
> and transforms remain unchanged. Historical 0.3.0 checks below retain attribution.

# Initial Gold execution record — 30 October 2026

## Attribution

Silver live results are **user-reported**, including the second-run zero inserts
and equal fingerprints. See [Silver evidence](aidp-silver-execution.md).
The agent executed only **local** checks for Gold. No AIDP session, cloud source
read/write, Autonomous operation, model training/inference or agent was executed.
**Live Gold verification remains pending.**

## Executed local checks

- Default project suite: **69 tests**, **61 passed**, **8 opt-in Spark tests
  skipped**. This includes existing Bronze/Silver/generator regression tests,
  Gold contracts and a top-to-bottom notebook run in a fresh **simulated**
  session. That simulation checks orchestration only, not AIDP SQL.
- Gold suite: **4 tests passed using real local Spark 3.5.6 / Delta 3.2.0**,
  Python 3.12 and JDK 17. Temporary fixture schemas have 4 orders (3 delivered,
  1 open) and multiple incidents for one order. Tests verify strict late labels,
  equality/early boundary cases, null open outcomes, explicit model projections,
  exclusion of current priority/customer attributes and actual carrier, invalid
  relationships/duplicates/nulls, managed Delta publication, identical rebuilds,
  zero second-run inserts, changed-row rejection and all-target incompatible
  definition preflight. The Silver reader test uses **real Delta HISTORY and
  uncached versionAsOf reads**; only the AIDP catalog/control lookup is simulated.
- Complete existing local LF v2 initial batch: **real Spark** transforms loaded
  from the built **0.3.0 wheel**, with the existing Silver quality engine used
  to prepare typed local inputs. No full-size Delta tables were published.

| Gold output | Verified local rows |
| --- | ---: |
| order_operations | 30,600 |
| delivery_kpis | 296 |
| late_delivery_training | 30,000 |
| late_delivery_scoring | 600 |

The completed cohort contains **9,816 late deliveries**, a global rate of
**32.72%** with denominator 30,000. Open orders are excluded from that denominator.
All 296 dimensional groups/counts were compared against an independent Python
oracle built from the typed local Silver dictionaries. Production Gold joins
and aggregations remain distributed. Every output passed exceptAll in both
directions across two independent transforms, with identical fingerprints.

See [gold-local-validation.json](gold-local-validation.json) for actual aggregate
results. Operational fingerprints include local source-file provenance and are
not expected AIDP fingerprint constants. Rerun equality must be checked within
the same environment, using full-row comparisons. Metadata fixture tests publish
only temporary local Delta tables; the full-size check writes only its aggregate
JSON report.

## Reproduce the complete local check

The source directory below is an already-generated dataset **outside Git**;
no generation/upload is performed by this command. A compatible Spark/JDK
fixture environment is required. Run from the repository root after building
the wheel as documented in [Gold setup](aidp-gold-setup.md).

```sh
JAVA_HOME=/private/tmp/aidp-silver-jdk17/Contents/Home \
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_PYTHON=/private/tmp/aidp-silver-spark-env/bin/python \
PYTHONPATH=dist/aidp_lakehouse_to_ai-0.3.0-py3-none-any.whl \
/private/tmp/aidp-silver-spark-env/bin/python tests/verify_gold_local.py \
  --source-dir /private/tmp/aidp-lakehouse-source-v2-lf-20261028/initial \
  --report /private/tmp/aidp-gold-local-validation.json
```

## Wheel verification and AIDP next step

Every packaged Python module was compared byte-for-byte with the repository
source. The wheel metadata version is **0.3.0** and both Gold modules are present.
The existing `silver.py`, `silver_spark.py`, `aidp_bronze.py` runtime fixes and
`generator.py` remain byte-identical to the previous published commit.
Bronze workflow and notebook version guards were updated to accept the new wheel;
no source files, Silver business logic or registered cloud objects changed.

The wheel is on the user's local Mac, outside Git tracking:

```text
/Users/cristinavarasmenadas/Library/CloudStorage/OneDrive-OracleCorporation/4. FY27/1. Customers/Workshops/14. Quistor/aidp-lakehouse-to-ai/dist/aidp_lakehouse_to_ai-0.3.0-py3-none-any.whl
```

SHA-256: `9653f0fa38ecb32e91940db7bd5e6f9a9bce9d6c37b9ec0487d7c51e7c543acb`.

Install that wheel on the existing AIDP compute, restart Python and execute
notebook 03 top to bottom. Require GOLD PASS and zero second-run inserts; capture
the actual job output/IDs separately. Current local tests do not establish live
Gold success. Training, model scoring, agents and Autonomous publication remain
future milestones.
