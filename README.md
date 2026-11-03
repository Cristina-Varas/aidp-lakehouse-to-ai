# Autonomous AI Lakehouse to AI

The database bootstrap is complete. No business tables or data have been created.

Use the existing SQLcl MCP saved connection **`CristinaVaras-ADBLakehouse-FY27`**.
Do not substitute `ADB_FY27_ADMIN`; `aidp_demo_admin` is not the saved alias.

See [bootstrap instructions](sql/bootstrap/README.md) and the
[execution record](sql/bootstrap/EXECUTION.md) for the verified database identity,
account policy, secure local credential mechanism, and rerun procedure.

The source-data milestone is implemented in `src/aidp_lakehouse/`:
deterministic ERP extracts, separate initial/incremental batches, defect
manifests, acceptance-time feature boundaries, fictional policies, validation,
and a separate OCI upload command. Reusable Python modules own the logic; future
notebooks should stay thin.

Read the [architecture](docs/architecture.md), [source-data commands and contract](docs/source-data.md),
and [verification record](docs/source-data-verification.md).

```sh
export PYTHONPATH="$PWD/src"
.venv/bin/python -m aidp_lakehouse generate --seed 42 --reference-date 2026-10-28 \
  --historical-orders 30000 --open-orders 600 --output-dir /private/tmp/aidp-lakehouse-source-20261028
.venv/bin/python -m aidp_lakehouse validate --data-dir /private/tmp/aidp-lakehouse-source-20261028
.venv/bin/python -m aidp_lakehouse upload --data-dir /private/tmp/aidp-lakehouse-source-20261028 \
  --storage-config /private/tmp/aidp-lakehouse-storage/storage.local.json \
  --batch initial --dry-run
```

The displayed output directory has already been generated locally; generation
refuses to overwrite it. Datasets and credentials stay outside Git. The initial
batch and both policy documents are uploaded in the shared
`CristinaVaras-bucket-AIDP` bucket under
`demos/lakehouse-to-ai/source-data/v1/seed-42/reference-2026-10-28/`.
The target is tenancy `oraemeaaidata2`, namespace `frgpslcqn7sm`, region
`eu-frankfurt-1`, compartment `CristinaVaras`. See the
[storage execution record](docs/object-storage-execution.md) for local
configuration, remote object verification and exact commands. Incremental files
remain local. No Autonomous business tables, data loading, model training, or agents
are implemented by this milestone.

The AIDP catalog/Bronze notebooks are prepared and locally validated:
[00_setup_catalog.ipynb](notebooks/00_setup_catalog.ipynb) and
[01_register_bronze.ipynb](notebooks/01_register_bronze.ipynb).
See [AIDP Bronze setup](docs/aidp-bronze-setup.md) for workspace/compute,
permissions, package installation, execution order and expected source counts.
The user has reported [live Bronze verification](docs/aidp-bronze-execution.md)
with matching counts, zero corrupt rows and passing provenance checks. Those Bronze results are user-reported. Reusable initial Silver logic and the thin
[02_build_silver.ipynb](notebooks/02_build_silver.ipynb) notebook are implemented;
see [Silver setup](docs/aidp-silver-setup.md). The user reports successful live Silver execution and an idempotent rerun; see
[Silver evidence](docs/aidp-silver-execution.md).

CSV compatibility release **0.2.1** generates explicit LF source records. The
initial v2 files/control metadata/policies are uploaded separately; v1 is
preserved. Use the [standalone customers LF probe](notebooks/probes/customers_lf_probe.py)
and [verification guide](docs/csv-lf-compatibility.md). The user reports that the live LF probe passed. Version **0.2.2** prepares the
[separate guarded migration](docs/bronze-v2-migration.md) and consolidates
notebook 01 as the canonical v2 register-and-verify flow. The 0.2.2 preparation did not execute main-table migration; notebook
configurations select v2 and stop on existing v1 tables.
Silver business logic is preserved.

Release **0.3.0** implements distributed initial Gold and
[03_build_gold.ipynb](notebooks/03_build_gold.ipynb): operational order facts,
monthly delivery KPIs, 30,000 delivered training rows and 600 unlabeled scoring
rows. Predictors are explicitly restricted to original acceptance snapshots.
See [Gold contracts and execution](docs/aidp-gold-setup.md). Gold live success is now user-reported; see the runtime corroboration below.
The 0.3.0 milestone did not implement training, inference or agents.

Release **0.4.0** adds [temporal late-delivery training](docs/late-delivery-training.md)
and [notebook 04](notebooks/04_train_late_delivery.ipynb), with explicit acceptance
features, availability-aware chronological evaluation, baseline/logistic/forest
comparison and native MLflow registration/version reload. Gold live success is
user-reported and its four counts were independently corroborated in a read-only
AIDP diagnostic. That diagnostic workflow encountered a native MLflow binding failure; the later
user-reported TRAINING PASS below supersedes its pending interactive status.
Notebook 04 still stops before fitting if preflight is not ready. See
[connection/runtime evidence](docs/aidp-training-connection.md). No open order
was scored. The existing Bronze/Silver/Gold data and transformations are preserved.

Release **0.5.0** adds [open-order scoring](docs/late-delivery-scoring.md) and
[notebook 05](notebooks/05_score_open_orders.ipynb). The user reports TRAINING PASS
in AIDP and exact-version reload for `lakehouse_demo.gold.late_delivery_acceptance/1`
(run `fc64485e353c41168cb90ee0f0bc5f71`). Scoring loads only that version and
recovers its threshold/cutoff/contracts; it pins 600 Gold opens and publishes
managed Delta predictions with guarded insert-only reruns. This release includes
the pending three-part model-name fix. Live AIDP scoring is pending; see
[execution record](docs/late-delivery-scoring-execution.md). Existing layer
transformations are preserved; agents and Autonomous publication remain next.

Connection diagnosis **0.5.1** adds a metadata-only native binding stage before
notebook 05 loads any model. The existing ask-aidp connection independently
verifies experiment 1, the FINISHED training run and READY model version 1.
Native workflow authorization is still blocked; no scoring was executed. During
this diagnosis run only configuration and section 1a, not Run All. See
[native binding evidence and exact platform prerequisite](docs/aidp-native-mlflow-binding.md).


## Policy fidelity — real execution completed, acceptance pending

The final frozen revision produced **12/15 normal PASS and 2/2 isolated-fault
PASS**. Causal feature roles remain correct. Policy answers still introduce an
unsupported prioritization restriction, confuse PREMIUM with accepted priority,
and cite an incorrect source filename. SQL/RAG bindings, selected model, KB,
data and computes are preserved; the fault-test copy was restored afterwards.

No inconclusive parameter request was repeated; nine outcomes remain unknown.
**AGENT ACCEPTANCE PASS is false; PR #1 stays a draft.** See
[actual execution](docs/operations-agent-policy-fidelity.md) and
[all case evidence](docs/operations-agent-acceptance.json).

## Previous 0.6.3 campaign — response contract verified; acceptance pending

The [role-separated response](docs/order-risk-response-contract.md) is deployed
and verified in AIDP against the saved training artifact. The causality case now
passes: prediction outputs are not presented as predictors and no individual
attribution is invented. Model, KB and other tools are preserved.

All 17 fresh-session prompts used one frozen configuration: **13 PASS, 2 FAIL,
2 PENDING**. The failures add policy hierarchy/deadline rules unsupported by the
source. Unavailable-service preconditions were not exercised while SQL/RAG were
healthy. Of 21 direct raw-parameter attempts, 11 returned HTTP responses and ten
had unknown outcomes. **AGENT ACCEPTANCE PASS is false; the PR remains a draft.**
See [actual execution](docs/operations-agent-execution.md),
[case evidence](docs/operations-agent-acceptance.json) and
[direct parameter observations](docs/operations-agent-native-parameters.json).

## Historical operations adviser 0.6.2 — native SQL and RAG

The dedicated DEV agent now runs six bounded native Spark SQL tools and policy
RAG with the existing model. The corrected KB ingested both TXT files successfully;
real native Tests verified four chunks against the source policies. The final
Workbench canvas shows all seven tools and a combined SQL/RAG conversation passed.

All **17 acceptance cases executed: 16 PASS, 1 FAIL**. Causality still confuses
model outputs/metadata with predictors after a targeted repair. Native unexpected
argument/type-coercion boundaries also remain uncertified. **AGENT ACCEPTANCE PASS
is false** and the PR stays a draft. The original Custom Code loader failure is
historical; Custom Code is not exposed by the current native agent.

See [setup](docs/operations-agent.md), [actual execution](docs/operations-agent-execution.md)
and [case responses](docs/operations-agent-acceptance.json). The 0.6.2 wheel is in
ignored `dist/` on the local Mac. No business data, model, training, scoring,
Autonomous publication, authentication or compute policy was changed.
