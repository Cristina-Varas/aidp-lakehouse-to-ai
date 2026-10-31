# Acceptance-time late-delivery training — release 0.4.0

## 1. Evidence, scope and installation

**User-reported Gold validation, recorded 31 October 2026:** reconciliation PASS,
identical fingerprints over two executions and zero additional inserts;
30,600 operations / 296 KPIs / 30,000 training / 600 scoring; 9,816 late deliveries.
The agent subsequently queried the four counts independently through the
explicitly authorized diagnostic. Fingerprints/reconciliation and late total are
still attributed to the user, not an agent rerun of Gold. See
[connection/runtime evidence](aidp-training-connection.md).

Use [04_train_late_delivery.ipynb](../notebooks/04_train_late_delivery.ipynb)
in a fresh authenticated lakehouse_to_ai AIDP notebook with existing AMD/Intel
Spark compute. Install only the project wheel **0.4.0** as a compute library
(or `%pip install --no-deps <uploaded wheel path>`) and restart Python. Use the
existing native sklearn/pandas/MLflow integration; **do not install .[training]
over AIDP's built-in MLflow**. That extra is for independent local environments.
Native MLflow is mlflow-skinny 3.16.0, not a missing full mlflow distribution.

Notebook 04 has one non-secret configuration cell: LF v2 initial prefix, seed 42,
fixed reference date 2026-10-28, temporal cutoffs, memory guard, experiment and
registered-model names. It imports reusable modules; no state/functions from
other notebooks or monkeypatches are required. Bronze/Silver/Gold remain read-only.
The 600 open orders are **not scored**. No agent, endpoint, Autonomous publication,
incremental ingestion or notebook 05 is implemented/executed.

**Environment prerequisite:** native Python MLflow client initialization failed
in the read-only diagnostic workflow, despite successful MCP/REST connection.
Review its native Experiments binding in the actual notebook session. The first
cell checks supported APIs and authenticated read access before fitting and
stops on this problem. No URI/auth override, fallback server or local file-store
enabling is performed. AIDP training/registration/reload are not claimed complete.

## 2. Inputs and prediction contract

Read only version-pinned `lakehouse_demo.gold.late_delivery_training` and
`late_delivery_scoring`. Require MANAGED DELTA, exact ordered contracts, unique
order keys, 30,000 training rows and 600 opens. DESCRIBE HISTORY records each
Delta version before lazy reading. A single upstream writer should be paused;
these two versions are not a cross-table atomic snapshot.

The open table supplies only count and earliest accepted_at through aggregate
queries; no open predictor records are collected or passed to a model.
Training collection has an explicit **50,000-row memory guard**, with an exact
30,000-row initial contract. Timestamps are formatted as UTC strings in Spark
before bounded collect, then parsed with pandas utc=True. This avoids host-local
naive datetime conversion and private Spark-to-Pandas compatibility dependencies.
The guard is an operational limit, not a business-quality rule.

The raw model input is the exact explicit Gold `PREDICTORS` list. Identifiers,
is_late, outcome_available_at, batch/reference metadata, current priority/status,
events, dispatch/delivery and actual carrier are excluded. No SELECT * or
“all columns except label” feature selection is used.

Business feature derivation is limited to:

`promised_lead_days = (promised_delivery_at_acceptance - accepted_at).total_seconds() / 86400`

It preserves fractional days and uses only original acceptance information.
The two raw timestamps are not directly fitted as absolute clock values. No
calendar, customer-master, delivery, incident or later-state variable is added.
Four categorical inputs are segment/region/priority/planned carrier at acceptance;
five original numeric inputs are inventory fill, warehouse load, distance,
line count and units; promised_lead_days is the sixth numeric input.

The **full pipeline** includes that derivation, train-fitted StandardScaler,
train-fitted OneHotEncoder(handle_unknown='ignore'), estimator and sklearn's
public FixedThresholdClassifier. The final threshold is therefore preserved in
both native sklearn predict and MLflow pyfunc.predict, not lost to default 0.5.
Model-boundary numeric types are doubles and timestamp transport is ISO UTC text;
Gold's original decimals/ints/timestamps remain unchanged. Unknown categories
are ignored using the training vocabulary; no preprocessing is fitted on evaluation.

## 3. Temporal design and label availability

Configured UTC boundaries are fixed before any model/metric inspection:

| Phase | Acceptance interval | Outcome availability restriction |
| --- | --- | --- |
| Tuning train | accepted_at < 2025-10-01 | outcome_available_at < 2025-10-01 |
| Validation | 2025-10-01 <= accepted_at < 2026-04-01 | outcome_available_at < 2026-04-01 |
| Held-out test | 2026-04-01 <= accepted_at < first open acceptance | outcome_available_at <= 2026-10-28, retrospective evaluation only |
| Final training pool | accepted_at < 2026-04-01 | outcome_available_at < 2026-04-01 |

Boundary equality is intentionally excluded for training knowledge: an outcome
available exactly at the period start is not claimed known **before** it. Purged
train/validation boundary labels are counted explicitly; no Gold row is changed,
quarantined or silently moved to another evaluation period. The report reconciles
train + validation + test + purged = 30,000. The final_train summary overlaps
train/validation and must **not** be added to that reconciliation.

First fit all candidates on tuning train; choose hyperparameters/threshold using
validation only. Then refit the frozen winner on final_train, including eligible
validation rows and earlier boundary labels now known before test. Fit fresh
preprocessing only on that phase's training pool. Hold threshold fixed after
refit; do not recalibrate it using test. Evaluate this final pipeline once on
test, alongside a prevalence baseline fitted on the same final pool. No test
labels enter any fit, candidate selection, threshold choice or later refit.

The registered pipeline is **exactly this final evaluated pipeline**, with
training cutoff 2026-04-01. Assert that cutoff <= first acceptance of the 600
opens. It is deliberately conservative/stale rather than fitting all history
(including test) before scoring. Future retraining is a separate policy.
MLflow's actual run time remains the real execution time; data-knowledge cutoff
is not a fabricated historical training timestamp. This is retrospective as-of
simulation using the generator's acceptance snapshots and conservative Gold
source-system outcome-availability proxy, not proof of real ingestion visibility.

For every partition report count, first/last acceptance, last outcome availability,
positive count and prevalence. Both classes are required in training/validation.
Single-class test ROC-AUC is explicitly undefined; AP is undefined without any
positive label. No numerical quality floor is imposed.

## 4. Fixed search, selection and metrics

Candidates: empirical training-prevalence DummyClassifier, LogisticRegression
C=0.1 or 1.0, and RandomForestClassifier with 64 trees, max_depth=4 or 8,
min_samples_leaf=30. Seed 42, deterministic input ordering by accepted_at/order_id,
one forest worker. There is no test-driven sweep, random split or unbounded search.

Choose the model by validation **average precision**, then ROC-AUC, then fixed
candidate order (simpler candidate wins exact ties). Each candidate's threshold
uses validation F1 over **0.10, 0.15, ..., 0.90**; ties choose the higher threshold.
The baseline remains eligible to win; no expected superiority is forced.

Report ROC-AUC, exact sklearn average_precision_score (not trapezoidal PR AUC),
precision, recall, F1, threshold and the full confusion matrix. Matrix rows are
actual [on-time, late], columns predicted [on-time, late]: [[TN, FP], [FN, TP]].
Zero-division precision/recall/F1 are zero and undefined ROC/AP include a reason.
Validation compares all five candidates. Test compares only the frozen selected
model and the same-cutoff prevalence baseline, including metric differences.
The final baseline probability is final-training prevalence, not test prevalence.

## 5. MLflow integration and runtime adaptation

Preflight inspects actual sklearn/logging APIs, encoding support and native
serialization default before fitting. It checks tracking and registry read access,
rejects local file/SQLite fallback in production, and refuses to nest in another
active run. Native auth/URI settings are preserved. Scheme-less values are rejected even when the native auth marker exists:
actual diagnostics showed they resolve to FileStore. Remote/native backend
routing and authenticated client reads must both succeed. MLflow distribution metadata can be null
while mlflow-skinny/module 3.16 is correctly installed.

The observed native default is **skops**. Keep it: trust only the exact known
project function `aidp_lakehouse.training.derive_acceptance_features`; do not
blindly trust whatever types an arbitrary artifact reports, or downgrade native
serialization. sklearn's public FixedThresholdClassifier avoids an unnecessary
custom estimator type. Embed only project Python modules with code_paths, no
datasets/configuration/credentials. Full predictor input signature and pinned
installed library versions accompany the model. Record mlflow-skinny when that
is the actual distribution; do not substitute a different MLflow package.

One owned MLflow run covers fitting through registration/version verification.
A successful run logs candidate parameters/validation metrics, final selected and
baseline test metrics, partitions/dates/prevalence/purge counts, threshold search,
feature contract, input Delta versions, environment/API capability evidence and
full selected pipeline. Experiment and registered-model names are configurable.
Registration creates a new version; existing versions, aliases, deployments and
models are not replaced/deleted. Reload by exact models:/name/version, verify
its source run before loading, then compare probabilities/thresholded decisions
on five **historical held-out** rows. No open row is used for this check.
A failed registration/reload raises and cannot emit TRAINING PASS.

Oracle documents [Experiments](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/experiments.html),
[Models](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/models.html)
and [MLflow REST operations](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aiwap/api-mlops.html).
The pipeline/threshold behavior follows
[sklearn FixedThresholdClassifier](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.FixedThresholdClassifier.html)
and [MLflow sklearn](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.sklearn.html).
Local verification is separate from actual AIDP registration: see the execution record.

## 6. Gold notebook state fix

Notebook 03 uses reusable GoldVerification. Starting configuration/runtime or
another first build invalidates earlier results. Starting a second build revokes
previous comparison success. Failed or interrupted builds cannot retain PASS.
The final cell requires successful first build, successful second build, matching
four-table fingerprints/validation summaries and zero second-run inserts before
printing GOLD PASS. Skipping a phase or executing an old final cell after failure
is rejected. Gold transformation/publication business logic remains unchanged.

## 7. Verification and next step

[Training execution](late-delivery-training-execution.md) records actual tests,
full local metrics, wheel path and commands. Local sklearn/MLflow/Delta tests are
not simulations of model fitting. AIDP catalog-only test shims and notebook/state
simulations are separately identified. Local SQLite registration/version reload
is not presented as AIDP Model Registry success.

After native preflight passes, run notebook 04 top to bottom. Keep its exact model
version, feature contract, cutoff and threshold for a **separate future scoring
notebook**. No inference on the 600 opens is part of this milestone.
