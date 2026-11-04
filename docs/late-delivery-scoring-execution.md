# Initial scoring verification — 1 November 2026

## Evidence attribution

The user reports **TRAINING PASS in AIDP**, registry model
`lakehouse_demo.gold.late_delivery_acceptance`, version **1**, run
`fc64485e353c41168cb90ee0f0bc5f71`, and verified exact-version reload. Reported
threshold is **0.25**, training cutoff **2026-04-01T00:00:00Z**; zero open orders
had been scored. These are user-provided runtime observations, not a new agent
query or rerun. The scoring notebook recovers/cross-checks the decision metadata
from the pipeline/run artifacts instead of configuring those numbers again.

**No AIDP scoring, predictions-table creation, registry write, cloud upload or
Autonomous operation was executed for this milestone.** Native version-1 artifact
verification, actual 600-open probabilities/alerts and live Delta publication
remain **pending execution of notebook 05** in the user's AIDP session.

## Checks actually executed locally

- Default suite: **96 tests; 72 passed, 24 optional tests skipped**. Includes
  notebook/configuration/pass-gate checks and the pending native three-part-name
  regression. A simulated failure on the second inference cannot return PASS.
- Built-wheel ML suite: **15 passed**, importing modules directly from the
  final 0.5.0 wheel. Real sklearn tests cover the fitted pipeline, saved feature
  and preprocessing contracts, unchanged inference after input reordering,
  missing/invalid inputs, invalid dates/ranges/domains, unknown metadata features,
  threshold/cutoff/run conflicts, and complete local MLflow registration/reload.
  Previous leakage/training regressions and real skops round-trip remain green.
  Reversed probability-column order and invalid model responses are **simulated**
  to test class lookup/finite/range/decision guards; they are not native outputs.
  The native binding preflight cases likewise simulate remote metadata/URIs.
- Final-wheel Spark integration: **1 passed using real local Spark/Delta,
  sklearn and SQLite MLflow**, including actual exact-version model reload,
  artifact verification, 600-row inference, managed Delta SQL MERGE and full-row
  equality in both directions. First publication inserts **600**, its repeat
  inserts **0**, and a later complete rerun inserts **0** again. Source version 0
  stays readable after a separate source commit; a deliberately invalid newer
  fixture version is rejected. Changed probabilities, incompatible views/schemas
  and a mismatched registry training run stop while existing content is preserved.
  **Only the AIDP Standard-catalog identity lookup is mocked.** The fitted model,
  run IDs, probabilities and 600 open records in this test are local fixtures,
  not the user's native version 1 or cloud source rows.
- All notebooks validate with nbformat, Python cells parse, and saved outputs and
  execution counts are empty. Every wheel Python module matches source bytes,
  including all three scoring modules, the qualified training name and preflight
  validation. Version metadata is 0.5.0; dist remains ignored.

Suites overlap; their counts should not be added as distinct tests. Default
optional skips are explicit; real ML and Spark coverage comes from the isolated
runs above. Gold/Silver/Bronze transformation modules, generator and bootstrap
scripts are unchanged. Existing notebook version guards accept the new wheel.

Local versions: Python **3.12**, Spark **3.5.6**, Delta **3.2.0**, sklearn **1.8.0**,
MLflow **3.16.0**, pandas **2.2.3**, NumPy **2.4.6**, skops **0.14.0**. Local Spark/
Python differ from AIDP's previously observed native runtime. No native dependency
or MLflow configuration was changed.

## Reproduction commands

Run from the repository root. The optional isolated environment, JDK17 and Delta
JARs below already exist outside Git; they were reused. Do not run these local
backend settings in AIDP. Use the source checkout for notebook tests; use the
wheel for the explicitly selected portable ML tests.

```sh
.venv/bin/python -m unittest discover -s tests -v

PYTHONPATH=/opt/homebrew/Cellar/oci-cli/3.94.0/libexec/lib/python3.14/site-packages \
.venv/bin/python -c "from setuptools.build_meta import build_wheel; print(build_wheel('dist'))"

PYTHONPATH=dist/aidp_lakehouse_to_ai-0.5.0-py3-none-any.whl:tests \
/private/tmp/aidp-training-env/bin/python -m unittest \
  test_scoring.ScoringTests test_training test_training_mlflow.MlflowTests \
  test_training_mlflow.NativeModelNameTests.test_native_preflight_rejects_unqualified_or_empty_parts_before_mlflow -v

JAVA_HOME=/private/tmp/aidp-silver-jdk17/Contents/Home \
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_PYTHON=/private/tmp/aidp-training-env/bin/python \
AIDP_RUN_SCORING_SPARK=1 \
AIDP_DELTA_JARS=/private/tmp/aidp-silver-jars/delta-spark.jar,/private/tmp/aidp-silver-jars/delta-storage.jar \
PYTHONPATH=dist/aidp_lakehouse_to_ai-0.5.0-py3-none-any.whl:tests:/private/tmp/aidp-silver-spark-env/lib/python3.12/site-packages \
/private/tmp/aidp-training-env/bin/python -m unittest test_scoring_spark -v

shasum -a 256 dist/aidp_lakehouse_to_ai-0.5.0-py3-none-any.whl
```

The integration test creates a new temporary SQLite/artifact/Delta directory
outside Git on every run. Its local fixture DDL and cleanup never target AIDP.
Production scoring contains no training call, registry write or destructive DDL.

## Wheel and next execution

The wheel is on the user's **local Mac**, not a remote execution environment:

```text
/Users/cristinavarasmenadas/Library/CloudStorage/OneDrive-OracleCorporation/4. FY27/1. Customers/Workshops/14. Quistor/aidp-lakehouse-to-ai/dist/aidp_lakehouse_to_ai-0.5.0-py3-none-any.whl
```

SHA-256: `f0792455d9f12df7f75c895ac4a6ceb56db7fc5f5143978dab5b14e46e02ad03`.

Follow [installation/execution](late-delivery-scoring.md) for notebook 05. Do not
rerun notebook 04 or register a different version. Retain the printed Gold input
Delta version/table ID, recovered decision metadata, source fingerprint, alert
count and complete two-execution summary as the next live execution evidence.
Only report live SCORING PASS after those checks actually finish in AIDP. Agents
and Autonomous publication remain future work.


## User update — 1 November 2026

The user supplied live output showing 600 predictions: 450 true and 150 false,
threshold 0.25, model version 1 and input_delta_version 1. The first ranked order
was O-00030255, customer C-000550, score 0.5999136971617619. After being asked to
check the final SCORING PASS cell, the user confirmed it was correct. This is
user-confirmed execution/repeat evidence, not an independently queried kernel
receipt. The prediction-table Delta version and exact successful connection
initialization output were not supplied. Do not infer the prediction-table
version from input_delta_version. Notebook 06 resolves it separately.

This update supersedes pending live scoring statements earlier in this record.
It does not establish that a separate workflow's native authorization was fixed,
or that an agent/KB has been created or tested.
