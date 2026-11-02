# Operations agent execution — 2 November 2026

## Current result

**Isolated native `risk_summary`: PASS. Full agent acceptance: BLOCKED.**
Version **0.6.1** fixes native configuration compatibility and encoded-error
propagation. The original SQL-only deployment has been restored; the full 0.6.1
candidate draft is separately saved and passes native validation. Neither an
ACTIVE deployment nor draft validation proves that its conversations work.

| Gate | Actual evidence |
| --- | --- |
| Notebook 06 | Earlier AIDP workflow SUCCESS; six Spark query families passed |
| Native SQL-only probe | Earlier PASS; one invocation, one row, two columns |
| Native Custom Code `risk_summary` | **PASS**, directly visible structured Test output |
| Six-tool UI suite | **Outcome unverified**: `Failed to fetch results`; not replayed |
| Corrected six-tool deployment | HTTP **201 / ACTIVE**, followed by runtime failure |
| First six-tool conversation | HTTP **400 / AIDP_USER_CODE_EXECUTION_ERROR** |
| Restored SQL-only baseline | **Restored and rechecked PASS**, one SQL call / one row / two columns |
| Full candidate draft | Preserved and **isValidAgent=true**, no errors |
| Policy KB | Created by user; ACTIVE, but project binding mismatch; no sources |
| Ingestion / retrieval / citations | **Not executed / not verified** |
| Agent acceptance | **0 executed, 17 blocked**, no AGENT ACCEPTANCE PASS |

The machine-readable [execution record](operations-agent-execution.json) and
[17 case dispositions](operations-agent-acceptance.json) distinguish these gates.
An isolated tool Test is not a completed agent conversation acceptance case.

## Verified environment and preservation

The existing Ask-AIDP **0.10.0**, CLI **5.1.1**, TypeScript SDK **5.1.1** and
original launcher/authentication were reused. Instance
**CristinaVaras-AIDPWorkbench**, workspace **lakehouse_to_ai**, region
**eu-frankfurt-1**, Standard catalog **lakehouse_demo** were reread. The existing
AI compute **lakehouse_to_ai_agent_dev** remains ACTIVE, **1 OCPU / 16 GB / one
replica**, as previously approved. The model remains the owner's selection:
**openai.gpt-oss-120b**, provider openai, Frankfurt.

Spark **lakehouse_demo_cpu** stopped under its unchanged 30-minute idle policy;
the user restarted it and Ask-AIDP subsequently confirmed ACTIVE. It was not
created, resized or given a different idle policy. No shared dependencies were
installed, no native authentication or URI configuration changed, and notebooks
01–05 were not executed in AIDP. Training, scoring, Autonomous publication and
business/source data writes were not performed.

The web session expired during work and the user restored it. No offered account
from another tenancy was used. macOS denied OS keystroke automation with
`osascript is not allowed to send keystrokes (1002)`; permissions were not changed.
Native UI operations used the already authenticated Workbench session. Browser
screens and raw responses remain private, outside Git.

## Independent data evidence

The earlier isolated notebook-06 workflow used the exact 0.6.0 wheel on that
kernel's temporary sys.path, with no pip/shared-library changes. Its single task
had `maxRetries=0` and finished SUCCESS. It verified:

- **600** unique predictions and **600** scoring-input orders.
- **450** alerts and **150** non-alerts.
- Predictions Delta version **1**, independently determined scoring version **1**.
- Top order **O-00030255**, customer **C-000550**, probability
  **0.5999136971617619**.
- Source/model/threshold consistency and the six read-only query families.

Those expected values were used to check results, never injected into agent
probe prompts. Fixed source reference date remains **2026-10-28**, sources **v2**.
Both Delta table identities and the complete receipt are stored privately.

The earlier native SQL-only probe also passed through a real agent conversation.
Its trace records Spark SQL success, one row and two columns. The raw tool trace
was service-redacted; the agent-rendered values matched notebook 06. No claim of
seeing that hidden payload is made.

## Native Custom Code Test and implemented fixes

The documented [Custom Code Test tab](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/agent-flow-tools.html)
was used on the dedicated AI compute. The isolated package was accepted and
`RiskSummary` discovered. Initial Test returned INVALID_CONFIG because Workbench
had converted nested `sql_connection` into JSON text. The 0.6.1 module now decodes
only valid JSON objects and rejects malformed/non-object/duplicate-key input.
Existing catalog/schema, parameter and credential checks remain in place.

Read-only native Test probes inspected the actual toolkit signatures/source,
without reading environment values or credentials. They established:

- `create_langgraph_tool(tool, private_context_provider=None)` is available.
- `SQLTool` routes Spark only with **catalogType=STANDARD**; its default is EXTERNAL.
- `_SparkSQLExecutor` reads **clusterKey**.
- The canvas export instead supplies **queryType=SPARK** and **sparkComputeKey**.

The module translates those observed representations and rejects conflicting
catalog or cluster keys. Business SQL, joins, limits, prediction/input versions
and model contract remain unchanged. It also decodes JSON-encoded native error
envelopes before checking them: a real native error previously appeared as
business data; after correction, Test returned TOOL_EXECUTION_ERROR.

With both translations applied, native isolated `risk_summary` returned visible,
unredacted structured content matching **600 / 450 / 150**, fraction **0.75**,
threshold **0.25**, registered model version **1**, the recorded training run and
cutoff, and observation date **2026-10-28**. This is a genuine native Test PASS.

A separate DEV-only suite was prepared using the same six reusable classes:
seven fixed reads (including a missing order), ten invalid-input checks, at most
two concurrent reads, and no writes. One confirmed Submit ended in
**Failed to fetch results**. Its server result is unavailable; individual passes
and native parameter rejections cannot be inferred. The suite was not replayed.
The verification class is not exposed in the prepared/deployed adviser tool list.

## Deployment and runtime are separate gates

The original full inline configuration had failed with HTTP 500. During this
continuation, a single isolated inline RiskSummary redeploy also returned
**500 / InternalError / Unexpected error occurred: Internal Server Error**.
No automatic SDK retry loop was used.

The native UI materialized the package under
`Agent_flows/.../CustomTools/...`, with provider AIDP. Those exact exported
metadata were saved privately and used for the corrected deployment, rather than
assuming the earlier `/Workspace/...` asset path was a runtime package binding.
The diagram now uses tool references/maps and the actual canvas ports
**bottom → top**, preserving the selected model.

The corrected six-tool diagnostic redeploy returned **201 / ACTIVE**. Its first
fresh-session `risk_summary` conversation then failed with:

```text
HTTP 400
AIDP_USER_CODE_EXECUTION_ERROR
The agent's user code could not be executed.
```

The native `aidpError.data` is empty; no internal exception/loader traceback was
provided. The AI-compute UI offers Details, Deployed Agents, Metrics and
Permissions, without a runtime-log tab. No exact package/import root cause is
claimed from this generic error. The remaining prepared individual chats were
not submitted after that failure.

The prior SQL-only deployment was restored once, returning **201 / ACTIVE**.
A fresh signed conversation then returned **200**: one probe invocation, one row
and two columns; the rendered values matched the independent snapshot. The
raw tool trace remained service-redacted, and is not claimed visible.
The full 0.6.1 candidate draft remains saved separately and validates successfully;
it was not redeployed after restoration. Original drafts and subsequent native
configurations are preserved in the protected runtime inventory.

## Policy KB and source status

Volume **lakehouse_demo.gold.lakehouse_to_ai_policy_sources**, folder
**/documents**, still contains only the two previously uploaded policy TXT files:

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| order_prioritization.txt | 1289 | 8d0214d1e39b000daae016c7fcb091b8e26657c1d738facecb8007538ef6f454 |
| incident_escalation.txt | 1342 | 7ad708d4453f5d3300663ea17ade94013b93a7567acac555d851c43817254276 |

Their earlier remote download matched original policy bytes. Neither policy source file
was changed in this continuation. Model artifacts, prompts and receipts remain
outside the policy ingestion folder.

The user created **lakehouse_demo.gold.operations_policies**, which was
independently listed as ACTIVE. Details show workspace/cluster keys that differ
from the verified project resources. The current UI row menu offers View, Rename,
Edit description and Delete; it does not expose binding edits. The empty KB was
preserved: no delete/recreate, no actual rename, no source registration and no
ingestion. Its embedding model and effective chunk settings are **not verified**;
1024/128 were requested form settings, not a verified saved configuration.

The [Oracle KB procedure](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/knowledge-bases.html)
requires a correctly selected ingestion workspace/compute and explicit source
folder. Do not ingest using the mismatched binding or the default compute of
another workspace. A dedicated corrected binding must be verified before adding
only `/documents` with TXT enabled. Both documents' ingestion and genuine section
retrieval/citations remain required before attaching retrieve_operations_policy.

## Local verification and wheel

On this Mac: **116 tests, 92 passed / 24 optional skipped**. The **13 operations
agent tests** also passed importing directly from the built wheel. Adapter tests
use a simulation, SQL fixture semantics use SQLite, and notebook workflow tests
use simulated sessions. They do not prove native deployment, RAG or acceptance.
Native evidence above comes from actual AIDP operations and is separate.

Notebook JSON/code structure and empty outputs were checked locally. Changes to
01–05 are package-version compatibility gates/text only, preserving their business
logic; they were not run in AIDP. The original 0.6.0 wheel is retained.

```sh
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m pip wheel . --no-deps --wheel-dir dist/
PYTHONPATH=dist/aidp_lakehouse_to_ai-0.6.1-py3-none-any.whl:tests \
  .venv/bin/python -m unittest test_operations_agent -v
```

The build used temporary isolated build dependencies, without changing AIDP.
The wheel's packaged implementation bytes match current source:

```text
/Users/cristinavarasmenadas/Library/CloudStorage/OneDrive-OracleCorporation/4. FY27/1. Customers/Workshops/14. Quistor/aidp-lakehouse-to-ai/dist/aidp_lakehouse_to_ai-0.6.1-py3-none-any.whl
```

SHA-256: `7de56bdb378c998096f7137878fca9e8ffe8ccb007a7448b9c850c3df7a456c0`.
This is on the **local Mac**. `dist/` remains ignored. Native Test used matching
standalone ZIP source; this wheel was not installed into shared Spark/AI libraries.

## Required next actions

1. Obtain the internal loader exception/traceback for the recorded failed
   six-tool conversation from the platform owner. Request identity, timestamp and
   raw response are retained in protected local runtime outside Git. Do not
   guess imports, change authentication, restart compute or redeploy blindly.
2. Correct/preserve the empty KB's workspace/compute binding through the supported
   owner UI, then independently verify it. Record an actually selected embedding
   model and effective chunk settings; only then ingest the two TXT sources.
3. After fixing the proven runtime cause, validate all six tools individually,
   including errors/limits/missing IDs, remove the diagnostic probe, attach tested
   RAG and run all 17 cases in fresh sessions. Actual calls, results, citations
   and verdicts are mandatory; hidden trace content needs independent checks.

The PR remains a **draft**, with no merge. No cleanup was performed. The dedicated
AI compute continues consuming AIDP Units while active; no unverified hourly
monetary price is asserted. Reused compute, models, tables, sources and prior
execution artifacts are preserved.
