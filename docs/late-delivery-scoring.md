# Initial open-order scoring — release 0.5.0

**Connection update 0.5.1:** before model loading, notebook 05 now verifies native
metadata routing. During the active diagnosis run only configuration and section
1a, then stop. Native connection restoration and scoring remain unverified. See
[native binding diagnosis](aidp-native-mlflow-binding.md).

## 1. Evidence and scope

**User-reported AIDP evidence, recorded 1 November 2026:** TRAINING PASS;
`lakehouse_demo.gold.late_delivery_acceptance`, version **1**, training run
`fc64485e353c41168cb90ee0f0bc5f71`; exact-version reload verified; selected
threshold **0.25**, training cutoff **2026-04-01T00:00:00Z**. No open orders had
been scored. These are user observations, not a new agent execution of AIDP.

Release 0.5.0 implements inference and managed Delta publication in
[05_score_open_orders.ipynb](../notebooks/05_score_open_orders.ipynb). It also
includes the pending three-part training model-name correction and its native
preflight regression tests. No retraining, source-layer changes, incremental
processing, agents, endpoint deployment or Autonomous publication is performed.
Live AIDP scoring/target publication remains **pending**.

## 2. Install and execute

Use workspace **lakehouse_to_ai**, instance **CristinaVaras-AIDPWorkbench**,
region **eu-frankfurt-1**, and existing authenticated Spark/Delta AMD/Intel
compute with native Experiments/Models. The account needs read access to the
exact registered model/run and its artifacts, Standard catalog metadata and
Gold input. It also needs CREATE TABLE/SELECT/INSERT on Gold predictions and
its managed storage. Keep native MLflow authentication and dependencies intact.
Oracle's [Models documentation](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/models.html)
describes loading registered models into notebooks for batch inference.

Upload the ignored local wheel
`dist/aidp_lakehouse_to_ai-0.5.1-py3-none-any.whl` as a compute library, or install
that project wheel with `%pip install --no-deps <uploaded-wheel-path>` and restart
Python. Do not install the local-test training extra over native MLflow.
Open/import notebook 05 and execute top to bottom in a new session. Notebook 04
need not run again. Logic lives in `aidp_lakehouse.scoring`, `scoring_mlflow` and
`scoring_spark`; the wheel makes all modules available.

The notebook has one configuration cell: v2 source metadata, exact model name,
version **"1"**, expected training run optional input Delta integer version, and non-secret verified native API bases.
There is **no threshold or cutoff setting**. An omitted Delta version is resolved
once, printed and reused for both inferences. Record that integer to replay in a
later session. Input is always `lakehouse_demo.gold.late_delivery_scoring`; output
is `lakehouse_demo.gold.late_delivery_predictions`.

## 3. Exact model and artifact verification

Only `models:/lakehouse_demo.gold.late_delivery_acceptance/1` is loaded with
[`mlflow.sklearn.load_model`](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.sklearn.html).
No latest alias, stage lookup, registration, fit, threshold search or calibration
occurs. Native initialization verifies the exact existing experiment/run/version before
activating any explicit route, preserving a working native binding. The following
preflight makes read-only capability checks and refuses local
backend fallback. The registry version must belong to the configured training
run, whose status must be FINISHED. Run artifacts are retrieved through
[`MlflowClient.download_artifacts`](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.client.html).

Required artifacts and checks:

- `feature_contract.json`: exact explicit Gold acceptance predictor names/order,
  feature roles and supported derivation; equality with the evaluation artifact
  and the complete saved pipeline's input columns and feature transformer.
- `evaluation_report.json`: selected threshold and final training cutoff;
  final-training acceptance and known-outcome maxima must precede the cutoff;
  held-out test isolation must be recorded.
- `registry_verification.json`: exact model name/version/run/URI and successful
  saved reload; threshold and cutoff must agree with evaluation/run parameters.
- `input_delta_versions.json`: recorded training/scoring Delta versions, schemas,
  table names, fixed reference **2026-10-28**, seed-42 LF v2 source prefix,
  explicit predictors and initial 600-open contract.

The complete pipeline must contain acceptance derivation, fitted preprocessing
and its saved `FixedThresholdClassifier`. Its threshold is authoritative and
must equal every saved threshold representation. Cutoff is recovered from the
training report and cross-checked against its boundary, registry artifact and
run parameter. Missing/conflicting artifacts stop before inference/publication.
No reported value is silently substituted as a fallback. This explicitly
preserves the recorded 0.25 rule without hardcoding it as an independent rule.

## 4. Input, inference and temporal safety

Read managed Delta at an explicit `versionAsOf` and capture its table ID.
Require the exact typed Gold scoring schema, 600 unique nonempty order IDs,
non-null fields, original numeric ranges/categorical domains reused from Silver, initial batch and fixed reference date. Record the version,
identity and complete source-row digest. Reject a table-identity change during
collection. Immutable snapshot reads, not a second current-table read, supply
both scoring executions.

Inference is deliberately **bounded to 600 rows on the driver**, using the same
sklearn pipeline as training. Joins/large-data transformation are not added.
Spark checks count/schema/keys before bounded collect; timestamps are formatted
in Spark UTC before transfer. Only explicit acceptance predictors enter the
pipeline; IDs, source metadata and any labels/outcomes stay outside. The only
business derivation remains promised lead time, inside the saved pipeline.
Require training cutoff <= every open acceptance <= fixed reference instant;
the original promise must follow acceptance. No model data fitting occurs.

Inspect `classes_` and find the column representing binary **1 = late**;
never assume probability column 1. Require two binary classes, a (600, 2) finite
probability matrix in [0,1], and row sums of one. Call the full pipeline's
`predict` separately and verify every decision equals late probability >= its
saved threshold, including equality. Outcomes for these opens remain unknown:
this is not an evaluation dataset and no accuracy or causal claim is made.

## 5. Predictions contract and safe publication

| Columns | Meaning |
| --- | --- |
| order_id, customer_id, accepted_at | Original identity and UTC prediction time |
| late_probability, predicted_late | Estimated late risk and saved-pipeline decision |
| decision_threshold | Recovered saved pipeline threshold |
| model_name, model_version, training_run_id | Exact registry/training provenance |
| training_cutoff | Recovered known-outcome cutoff |
| input_table, input_table_id, input_delta_version | Fixed Gold source identity/version |
| input_fingerprint | SHA-256 of sorted canonical complete source records |
| source_prefix, _batch_id, _reference_date | Initial LF v2 source contract |

Types/order are explicit in `PREDICTION_CONTRACT`. Probabilities/thresholds are
double, classification boolean, versions bigint/string as applicable, times UTC
timestamp, reference date date, and remaining identifiers string. All fields
are required. No wall-clock scoring timestamp is inserted into business results,
so the same input/model produces byte-equivalent canonical values on repetition.

Only create-if-absent and insert-only Delta SQL MERGE are used, with explicit
insert columns and a uniquely named, session-local temporary view (removed in
finally). Oracle documents [Standard-catalog Spark/Delta SQL](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/agent-flow-tools.html),
and [Delta's MERGE reference](https://docs.delta.io/delta-update/) supplies the
insert-only SQL form. Using SQL also avoids the local Delta Python API's
three-part-name parser limitation. Existing tables must
be managed Delta with the exact schema, unique keys, safe managed location and
no rows different from or additional to the proposed result. A compatible subset
can be completed; matched rows are never updated. A changed model, run, source
snapshot, probability or existing extra order causes failure, preserving contents.
No DROP, overwrite or delete is part of the production path. Do not run concurrent
writers: definition/content preflights and MERGE are not one cross-operation
transaction. For a future new cohort/model, define an explicit history/version
strategy instead of weakening this initial publication guard.

The workflow executes inference/publication twice against the same input and
model. Both complete results must match with `exceptAll` in both directions,
summaries must agree and the second MERGE must add zero rows. A failed operation
raises; notebook success is invalidated before configuration/runtime/input or
scoring re-execution. `SCORING PASS` requires all checks to complete.

## 6. Reading the output

Show alert counts, descriptive probability quantiles/decile counts and the top
20 scores ordered by descending probability, with `order_id` ascending to break
ties. This is a **risk ranking**, not commercial priority. No invented causal
explanations, customer actions or model probabilities are attached to input
records. Actual probabilities and alert count must come from execution.

See [local verification and pending AIDP execution](late-delivery-scoring-execution.md)
for commands and artifact details. Keep Bronze/Silver/Gold inputs, v1/v2 objects
and the registered training version unchanged. Agents and Autonomous publication
are subsequent milestones.
