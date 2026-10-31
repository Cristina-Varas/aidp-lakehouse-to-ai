# Training verification — 31 October 2026

## Attribution and current AIDP status

Gold reconciliation PASS, equal fingerprints over two executions, zero extra
inserts and 9,816 late deliveries were reported by the user. The agent reused the
existing ask-aidp launcher and independently queried the four Gold counts through
the approved diagnostic: 30,600 / 296 / 30,000 / 600. See
[connection/runtime record](aidp-training-connection.md) and the
[sanitized live report](aidp-training-runtime.json).

The diagnostic notebook/workflow completed successfully. It did not train a
model, score an open order, modify a layer, install dependencies, change native
MLflow configuration or register a model. Native MLflow client initialization
failed in that workflow context because it resolved a filesystem backend in
maintenance mode. Actual AIDP training, MLflow writes, registration and model
version reload remain **pending**. Notebook 04 stops before fitting until native
preflight passes in its actual interactive session; no local fallback is enabled.
No Codex restart or additional MCP configuration was necessary for the inspection.

## Actual local checks

- Default suite: **85 tests, 67 passed, 18 optional tests skipped**. This includes
  Gold PASS-state regression tests and clean notebook checks. Gold/notebook
  orchestration simulations are not real Spark/AIDP execution.
- Isolated ML suite: **9 passed**. Tests use actual sklearn fitting/metrics,
  deterministic repeated training, test-label mutation to detect leakage,
  strict outcome boundary purging, preprocessing vocabulary/mean isolation,
  real local MLflow SQLite registration and exact-version sklearn/pyfunc reload,
  and real skops round-trip with the exact project-function allowlist. Three
  preflight cases simulate unsupported local/native URI and metadata conditions;
  they do not assert authenticated AIDP client success.
- Dedicated Gold reader integration: **1 passed using real local Spark/Delta**,
  30,000 typed training fixtures and 600 open fixtures. It checks managed Delta
  versions, UTC timestamps and aggregate-only open access. Only the AIDP catalog
  identity check is simulated because local Spark has no AIDP Standard catalog.
- Complete existing LF v2 initial batch: **real local Spark/Delta + sklearn +
  MLflow 3.16 + skops**, importing project modules from the built 0.4.0 wheel.
  The existing Silver quality engine and unchanged distributed Gold transforms
  construct local typed inputs; two local Gold tables are materialized as Delta
  version 0, then read through the production training adapter. Actual model
  fitting/evaluation and local SQLite registration/version reload complete, and
  an independent second fit produces identical reports/probabilities. No AIDP
  model registry success is inferred from this local check.

Local ML versions match the observed ML APIs: sklearn 1.8.0, MLflow 3.16.0,
skops 0.14.0, pandas 2.2.3, NumPy 2.4.6. Local Python is 3.12 and Spark/Delta are
3.5.6/3.2.0, distinct from native Python 3.11 and the Data Flow/Delta runtime.
The first MLflow 2.22 local test environment needed SQLAlchemy <2.1; this was a
local SQLite compatibility fix, not a native connection/configuration change.

## Complete local temporal results

All boundaries are UTC. Reference date stays **2026-10-28**, seed **42**.
The [aggregate report](training-local-validation.json) includes exact dates,
versions, metrics, candidate validation comparison and membership hashes.

| Pool | Rows | Late prevalence | Latest available outcome |
| --- | ---: | ---: | --- |
| Tuning train | 14,247 | 32.2805% | 2025-09-30 23:59 |
| Validation | 7,224 | 33.2364% | 2026-03-31 23:10 |
| Held-out test | 7,751 | 31.7894% | 2026-10-15 05:00 |
| Final pre-test training pool | 21,838 | 32.7915% | 2026-03-31 23:10 |

367 train and 411 validation labels are unavailable at their selection boundaries:
14,247 + 7,224 + 7,751 + **778 purged** = 30,000. Final_train is an overlapping
refit pool, not a fourth disjoint partition. No test row participates in fitting.
The final training cutoff **2026-04-01 00:00 UTC** precedes the first open
acceptance **2026-10-22 00:04 UTC**. These dates are local benchmark observations,
not an independent AIDP query of the earliest acceptance.

Validation selects **Random Forest, 64 trees, depth 8, min_samples_leaf=30**,
threshold **0.25**. No precision/quality minimum is forced.

| Held-out metric | Selected forest | Prevalence baseline |
| --- | ---: | ---: |
| ROC-AUC | 0.633306 | 0.500000 |
| Average precision | 0.447868 | 0.317894 |
| Precision | 0.350726 | 0.317894 |
| Recall | 0.872971 | 1.000000 |
| F1 | 0.500407 | 0.482428 |

Confusion matrices, actual rows / predicted columns [on-time, late]:

- Forest: **[[1305, 3982], [313, 2151]]**.
- Baseline: **[[0, 5287], [0, 2464]]**.

The baseline probability is final-training prevalence 0.3279146; its threshold
0.30 is selected on validation. Its test AP/precision equal test prevalence
because all test orders receive the same probability and are classified late.
The forest offers ranking gain and modest F1 gain with substantial false alerts;
these results are not evidence of production readiness. All 600 opens remain
unscored. The local registered version belongs to the SQLite test backend only.

## Exact local commands

Use an isolated environment, not the native compute or the project Python 3.14
notebook-validation environment. The existing Spark/JDK/JAR environment under
/private/tmp is reused. Example preparation:

```sh
/private/tmp/aidp-silver-spark-env/bin/python -m venv /private/tmp/aidp-training-env
/private/tmp/aidp-training-env/bin/python -m pip install \
  scikit-learn==1.8.0 pandas==2.2.3 numpy==2.4.6 mlflow==3.16.0 skops==0.14.0

.venv/bin/python -m unittest discover -s tests -v
PYTHONPATH=src:tests /private/tmp/aidp-training-env/bin/python \
  -m unittest test_training test_training_mlflow -v

JAVA_HOME=/private/tmp/aidp-silver-jdk17/Contents/Home \
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_PYTHON=/private/tmp/aidp-training-env/bin/python \
AIDP_RUN_TRAINING_SPARK=1 \
AIDP_DELTA_JARS=/private/tmp/aidp-silver-jars/delta-spark.jar,/private/tmp/aidp-silver-jars/delta-storage.jar \
PYTHONPATH=src:tests:/private/tmp/aidp-silver-spark-env/lib/python3.12/site-packages \
/private/tmp/aidp-training-env/bin/python -m unittest test_training_spark -v

PYTHONPATH=/opt/homebrew/Cellar/oci-cli/3.94.0/libexec/lib/python3.14/site-packages \
.venv/bin/python -c "from setuptools.build_meta import build_wheel; print(build_wheel('dist'))"

# Select a NEW work directory; the verifier refuses to overwrite prior local artifacts.
JAVA_HOME=/private/tmp/aidp-silver-jdk17/Contents/Home \
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_PYTHON=/private/tmp/aidp-training-env/bin/python \
AIDP_DELTA_JARS=/private/tmp/aidp-silver-jars/delta-spark.jar,/private/tmp/aidp-silver-jars/delta-storage.jar \
PYTHONPATH=dist/aidp_lakehouse_to_ai-0.4.0-py3-none-any.whl:tests:/private/tmp/aidp-silver-spark-env/lib/python3.12/site-packages \
/private/tmp/aidp-training-env/bin/python tests/verify_training_local.py \
  --source-dir /private/tmp/aidp-lakehouse-source-v2-lf-20261028/initial \
  --work-dir /private/tmp/aidp-training-wheel-20261031 \
  --report /private/tmp/aidp-training-wheel-validation.json
```

The displayed work directory has already been used; choose another new path for
a repeat. Models, MLflow database, source rows and credentials stay outside Git.

## Artifact and next step

Wheel, on the user's **local Mac**, not a remote execution container:

```text
/Users/cristinavarasmenadas/Library/CloudStorage/OneDrive-OracleCorporation/4. FY27/1. Customers/Workshops/14. Quistor/aidp-lakehouse-to-ai/dist/aidp_lakehouse_to_ai-0.4.0-py3-none-any.whl
```

SHA-256: `289232ee79e4af10c27047612f9d2a46524099f95f0ed2f9095d36fd86dbba39`.
Every packaged Python module matches source; required training modules and wheel
version metadata were verified. dist remains ignored. Generator and all
Bronze/Silver/Gold transformation modules are byte-identical to the previous
published commit; only notebook guards/version compatibility and the new ML
modules change behavior.

Before AIDP training, validate the native Experiments binding in the real notebook
session without overriding auth/URIs or enabling local fallback. Run notebook 04
after its preflight succeeds. Its successful result must provide an actual AIDP
registered model version and a verified exact-version reload. Later scoring
belongs in a separate notebook, using that saved cutoff/threshold; it is not run
in this milestone.
