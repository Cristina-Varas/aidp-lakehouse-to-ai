# Operations adviser — native SQL and policy RAG

## Current follow-up: role-separated order-risk response

Candidate **0.6.3** reads the saved training feature artifact and separates model
outputs, fitted feature values, date derivation sources and metadata. It is
locally verified, **not yet deployed or verified live**; project Spark is STOPPED.
Acceptance is pending. See [contract, direct parameter tests and single-configuration campaign](order-risk-response-contract.md).
The working native 0.6.2 agent, KB and model remain preserved.

## Verified 0.6.2 baseline and scope

Version **0.6.2** provides six bounded native Spark SQL tools and native policy
RAG in the existing DEV agent `lakehouse_to_ai_operations_adviser`. The corrected
KB ingested the two TXT successfully, and independent native Tests verified their
actual retrieved chunks. All 17 acceptance cases were executed: **16 PASS, 1 FAIL**.
The remaining causal/feature-role response is incorrect after one targeted repair.
**AGENT ACCEPTANCE PASS is false; the PR remains a draft.**

See [execution evidence](operations-agent-execution.md),
[case responses and verdicts](operations-agent-acceptance.json) and
[local checks](operations-agent-local-verification.md). Raw traces, authentication
and private deployment exports stay outside Git. This is an advisory synthetic
snapshot as of **2026-10-28**, sources **v2**, not live ERP monitoring.

The user previously reported SCORING PASS: 600 predictions, 450 alerts and
150 non-alerts, model version 1, threshold 0.25 and identical repeat results.
Notebook 06 and subsequent native Tests independently checked the existing
snapshot. No training, scoring, Autonomous publication or business-data writes
were performed during agent integration.

## Reuse the existing readiness receipt

`notebooks/06_prepare_operations_agent.ipynb` verifies existing Delta data and the
six SQL families without loading a model or writing business tables. It already
passed earlier in AIDP. **Do not repeat 01–05 or notebook 06 merely to redeploy
tools.** Reuse the protected `AGENT DATA READY` receipt after verifying the target
and table definitions. Predictions and scoring-input versions are both **1**;
immutable table identities come from that receipt, not guessed names or versions.

Notebook readiness is not native-tool or agent acceptance. The wheel is an
optional project handoff artifact; native SQL/RAG deployment does not require
installing it into shared Spark or AI libraries.

## Reproducible native SQL definitions

Use the successful native SQL export for this workspace and existing compute.
Copy the placeholder-only `config/operations-agent-native-sql.example.json` to a
private local directory and fill it with independently verified routing and the
new KB identity. Keep authentication native; never add credentials to this file.

```sh
PYTHONPATH=src .venv/bin/python -m aidp_lakehouse.agent_native_sql \
  --receipt /path/outside-git/agent-data-readiness.json \
  --environment /path/outside-git/native-environment.json \
  --profile complete \
  --feature-contract /path/outside-git/feature_contract.json \
  --output-dir /path/outside-git/native-operations-062
```

The command does not deploy anything. It writes private tool definitions and a
manifest marked PREPARED_ONLY, runtime_verified=false and agent_acceptance_pass=false.
Identical reruns change zero files; conflicting existing directories are preserved
and rejected. `--profile static` prepares only the three parameter-free queries.

| Native SQL tool | Runtime input | Maximum rows |
| --- | --- | ---: |
| `risk_summary` | None | 1 aggregate |
| `rank_open_orders` | Integer `limit`, 1–20; default 10 | 20 |
| `get_order_risk` | Exact `O-########` | 1 |
| `get_customer_orders` | Exact `C-######` | 20, with total_matching |
| `risk_by_carrier` | None | 3 |
| `risk_by_region` | None | 4 |

The three static queries retain the original pinned SQL. Parameterized native
queries use documented quoted string placeholders, exact key guards, validated
integral rank bounds and a fixed LIMIT 20. Invalid known values select an error,
not a silently missing record. Ranking retains score descending / order_id
ascending and explicit evidence columns; the model cannot replace the SQL.

**Native boundary limitation:** the backend accepted empty-name extra arguments
from the model for no-input tools. Exact unexpected-argument rejection and numeric
coercion equivalence with the original Python adapter remain uncertified. One
negative native Test timed out and was not replayed. Real Spark verified 14 guard
predicates separately; it did not establish all binder/error behaviors. The
original stricter Python checks have been preserved. Do not describe these
interfaces as having identical argument-validation guarantees.

## Dedicated KB and native RAG

The corrected `lakehouse_demo.gold.operations_policies` has key
`b5310533e4fe43e3a8c0f3234c569f3a`, workspace **lakehouse_to_ai**, compute
**lakehouse_demo_cpu**, embeddings **MULTILINGUAL_E5_SMALL**, chunk size **1024**,
overlap **128**. Its only source is:

```text
/Volumes/lakehouse_demo/gold/lakehouse_to_ai_policy_sources/documents
```

TXT is the only enabled file type. The folder contains exactly
`order_prioritization.txt` and `incident_escalation.txt`, preserving the original
English numbered policies. Do not register the whole volume or add manifests,
control files, prompts, notebooks, packages, source data or model artifacts.

Ingestion already completed: **2 added / 0 modified / 0 deleted**, SUCCEEDED.
Do not reingest merely to query or redeploy. The old renamed empty KB is retained
and is not a fallback. If the new name resolves to a different identity later,
stop and inspect it before using it.

`agent_policies.validate_policy_kb` checks saved identity, binding, embedding
model, chunking and the exact source. `policy_rag_definition` builds the verified
native SDK representation: RAG `inputSchema.query` is a **string**, unlike SQL's
flat parameter map. `verify_policy_chunks` checks actual retrieved document IDs
and normalized chunks against the approved policy text. An LLM answer alone is
not retrieval evidence.

The current Ask-AIDP tool set does not expose the legacy KB lifecycle. Its REST
wrapper permits only the current API prefix. Where required, use Workbench's
[documented KB interface](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/knowledge-bases.html)
or the exact native interface verified from that runtime, reusing existing auth.
Never derive endpoints from the UI address or recreate authentication.

## Preserve native canvas and selected model

Build definitions with the reusable modules after reading the current owned agent
and saved KB metadata into private local files:

```python
from aidp_lakehouse.agent_native_sql import native_tools, native_canvas_diagram
from aidp_lakehouse.agent_policies import policy_rag_definition
from aidp_lakehouse.agent_order_risk import TrainingFeatureEvidence, apply_order_risk_contract
from aidp_lakehouse.agent_bundle import validated_snapshot

evidence = TrainingFeatureEvidence.from_native_records(
    feature_contract_bytes, model_version_record, run_info)
sql_tools = apply_order_risk_contract(
    native_tools(receipt, environment), validated_snapshot(receipt), evidence)
rag_tool = policy_rag_definition(
    kb_metadata, environment, existing_diagram["nodes"]["planner"]["llm"])
candidate = native_canvas_diagram(existing_diagram, sql_tools, rag_tool)
```

These variables are private readbacks, not notebook-global dependencies. This
prepares the 0.6.3 candidate; do not deploy it before the pending runtime gate.
`native_canvas_diagram` preserves model, instructions and unrelated metadata,
uses native `toolsMap` and `toolReferences`, and the observed **bottom → top**
ports. It refuses a different graph instead of resetting it. Inline-only tools
can execute through the API while disappearing from the visual canvas; use the
verified native representation. The current UI shows **Tools (7)**.

Retain `src/aidp_lakehouse/agent_resources/instructions.txt` as the reviewed source
of project instructions. No instruction prompt guarantees compliance: causal
claims still failed acceptance in the actual selected model. Do not change the
owner's selected model or claim that the prompt repaired that failure.

## Validate and preserve failure evidence

Use independent native SQL Tests to compare actual rows, including ranking order,
customer membership, missing IDs and aggregate reconciliation. For RAG, compare
real chunks and source identities; only then check chat citations. If chat traces
redact payloads, record that limitation and the independent comparison.

Run acceptance prompts unchanged in new conversations. Record tools, arguments,
responses, source citations, configuration revisions and verdict reasons. Keep
failed attempts when a targeted repair is tested. Test SQL/RAG unavailability by
reversible DEV-only configuration changes, never by stopping shared compute,
changing authentication or deleting a KB. Always restore and reread the complete
canonical configuration afterward.

The final DEV deployment is operational, but **causality remains FAIL** and the
native argument boundaries above need further certification. No merge is allowed
on the basis of deployment status or 16 passing cases.

## Historical Custom Code path

`agent_bundle` and its original six guarded classes remain available and preserve
their prior behavior. The loader failure for the six-tool Custom Code deployment
was never explained by internal logs. The active native alternative does not use
that package and does not require retrying that failure. Historical evidence is
retained in the execution record; it is not current native-agent status.

Official references: [native SQL, RAG and Test](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/agent-flow-tools.html),
[Knowledge Bases](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/knowledge-bases.html),
[deployed sessions](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/invoke-deployed-agent.html).
