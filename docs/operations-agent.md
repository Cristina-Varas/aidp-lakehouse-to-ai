# Operations adviser — prepared SQL + RAG milestone

## Status and scope

The user confirmed SCORING PASS in AIDP on 1 November 2026: 600 predictions,
450 alerts, 150 non-alerts, exact-version repeat and no additional rows. The
shown ranking uses model 1, threshold 0.25 and scoring-input Delta version 1.
This is user-provided evidence. We did not independently query that kernel.

This change prepares notebook 06, reusable read-only queries, a Custom Code
package builder, unchanged policy bytes in TXT form, agent instructions and an
acceptance plan. It does **not** deploy an agent, create/ingest a KB, publish to
Autonomous or establish native SQL authorization. Native adapter tests use a
simulation, and local query semantics use SQLite; neither is live AIDP evidence.

The adviser estimates acceptance-time risk for a fixed synthetic snapshot as of
2026-10-28. It does not monitor later events or determine a business work queue.
It cannot send messages, reschedule deliveries or retrain/change the model.

## 1. Verify existing data using notebook 06

Build the candidate 0.6.0 wheel from this branch with the existing project build
procedure. Install only the project wheel without replacing platform packages.
Preserve the working 05 kernel until ready to attach/restart compute. Run
`notebooks/06_prepare_operations_agent.ipynb`; 01–05 need not be rerun.

The first gate checks managed Delta schemas and identities, exactly 600 unique
prediction/input keys, customer and acceptance timestamps, model 1 and its run,
finite probabilities, consistency with each saved decision threshold and fixed
source provenance. It pins the prediction Delta version and reads the scoring
Delta version recorded in those predictions. The Spark session and Python driver
must both use UTC. A timezone error is a stop, not permission to reinterpret data.

The second gate runs the six query families against those versions in Spark.
Copy the JSON readiness receipt to a local file **outside Git**. An optional new
driver output directory builds files locally; it does not upload them. Neither
`AGENT DATA READY` nor `NOTEBOOK SQL PASS` is an agent acceptance result.

From the same candidate source, the local bundle command is:

```sh
PYTHONPATH=src python -m aidp_lakehouse.agent_bundle \
  --receipt /path/outside-git/agent-data-readiness.json \
  --output-dir /path/outside-git/quistor-operations-agent
```

Identical repeated builds change zero files. A different existing output directory
is rejected without overwriting it. The bundle contains:

| File | Use |
|---|---|
| `operations_sql_tools.zip` | Six read-only Custom Code tool classes |
| `agent_instructions.txt` | Instructions for the executor agent |
| `documents/order_prioritization.txt` | Byte-preserved fictional policy |
| `documents/incident_escalation.txt` | Byte-preserved fictional procedure |
| `data_readiness.json` | Pinned data identities and counts |
| `manifest.json` | File checksums and explicit pending states |

Do not upload the whole bundle as a knowledge source. Only `documents/` belongs
in the KB source folder. Metadata, prompts, code and receipts stay outside it.

## 2. Check native SQL capability before configuring the agent

Use the existing **CristinaVaras-AIDPWorkbench / lakehouse_to_ai** workspace.
Create a separate DEV agent named `lakehouse_to_ai_operations_adviser`; do not
reuse another demonstration's agent or compute without checking its identity.
Attach a suitable existing ACTIVE AI compute. Do not silently provision compute.

In a native SQL tool, check whether the dialect selector offers **Spark SQL**
against the standard `lakehouse_demo` catalog and a running Spark cluster.
Oracle's detailed SQL-tool instructions and tutorial now describe this path,
while introductory notes still say external catalogs only. This inconsistency
means the actual instance capability must be verified before relying on it.

If Spark SQL is available, configure it for `lakehouse_demo.gold`. Run a static
`SELECT` from `lakehouse_demo.gold.late_delivery_predictions` with a one-row
limit. Record success, target workspace and cluster. Copy the tool's actual JSON
Configuration export; preserve its native dialect and cluster keys exactly.
Do not infer keys/identifiers from UI URLs, copy credentials, replace native auth
or use the MLflow endpoints for SQL. A successful notebook query is not this test.

If Spark SQL is unavailable, stop and record **BLOCKED: standard-catalog SQL tool
unavailable**. The external-Oracle route would require separately publishing
approved Gold data to Autonomous and granting its read-only account access. That
fallback is not implemented or executed here; do not point the agent at empty
Autonomous tables or an unrelated existing demo.

The SQL tool needs its selected Spark cluster to remain running. Do not change
its idle/Forever setting without considering the resulting cost and existing
workspace policy. Report a stopped cluster as unavailable, not an empty result.

## 3. Configure and test the guarded SQL package

Upload `operations_sql_tools.zip` as a Custom Code tool package. It uses only
standard-library Python plus native `aidputils`; it installs no replacement
platform dependencies. It registers:

| Tool | Runtime input | Limit |
|---|---|---:|
| `risk_summary` | None | 1 aggregate row |
| `rank_open_orders` | Integer `limit`, 1–20 | 20 |
| `get_order_risk` | Exact `order_id`, `O-########` | 1 |
| `get_customer_orders` | Exact `customer_id`, `C-######` | 20, plus total_matching |
| `risk_by_carrier` | None | 3 |
| `risk_by_region` | None | 4 |

For **each enabled class**, populate the initially empty `sql_connection` with
that tested native Spark SQL configuration. Keep the generated prediction/input
versions and table IDs from the receipt. Configuration is private deployment
state, not a runtime argument chosen by the LLM.

The adapter validates runtime parameters, creates a fixed SELECT for that call,
and delegates to the documented `AIDPToolConf` / `create_langgraph_tool` SQL
integration with an empty native parameter list. Thus user text never reaches a
native SQL template. The installed toolkit's support for composing the native
SQL tool from Custom Code is **not verified by this change**. Test it first on
this exact runtime. If class registration, configuration or invocation differs,
inspect the installed SDK/example via Ask-AIDP and adapt it; do not guess methods
or mark the mocked tests as deployment evidence.

Test each tool independently, compare its raw result with notebook 06 and save
receipts. An invalid ID, fractional/oversized limit or unexpected argument must
raise before the SQL helper is called. Test both existing and missing valid IDs.
Test a simulated native failure: it must not become a fabricated business answer.

Queries pin versions; source refreshes require a new readiness receipt and
bundle. Rerun the identity gate before deployment if either table may have been
recreated. The recorded IDs do not cryptographically bind a native SQL execution
to the old table: do not deploy after recreation without rechecking.

## 4. Ingest policies and attach RAG

The bundle changes `.md` to `.txt` without changing bytes; checksums establish
source equivalence. Oracle's KB documentation lists TXT as an ingestion format.
Upload the two TXT files into a dedicated managed-volume folder or a separate
Object Storage prefix exposed by an external volume. Preserve original source
files and avoid shared folders containing other demos or control metadata.

Create a dedicated Knowledge Base in `lakehouse_demo.gold`, for example
`operations_policies`. Select the verified workspace/ingestion cluster and an
embedding model available there. A multilingual model is a reasonable candidate
for Spanish questions over English policies, but retrieval quality must be tested.
Use explicit chunking settings, then inspect retrieval; do not assume defaults
preserve every numbered section.

Add only the isolated documents folder as the source, enable TXT ingestion, and
verify the ingestion job completes for **both** documents. An empty KB creation
success is insufficient. Add a RAG tool named `retrieve_operations_policy`, select
this KB and initially retrieve up to four chunks. Verify raw retrieval for:

- Risk is advisory and needs coordinator review: prioritization sections 4–5.
- PREMIUM alone does not override earlier promises: prioritization section 3.
- Severity 1/2/3 targets: escalation section 3.
- Human approval for communication or a revised promise: escalation section 4.

If ingestion/retrieval fails, retain the failure evidence and stop the combined
acceptance gate. Test in fresh conversations to avoid confusing cached responses
with retrieval from the new KB.

## 5. Agent setup and acceptance

Use one executor agent with the six SQL tools and the policy RAG tool. Paste the
bundle instructions into its instructions field. Select an approved model
available on the verified AI compute; do not assume a model from another demo.
Keep this in DEV until native SQL, ingestion, retrieval and answer checks pass.

Run the cases in `operations-agent-acceptance.json` in separate fresh sessions.
For each, record session/case ID, tool names and inputs, raw tool results, final
answer, actual document citations and pass/fail reason. When traces are
incomplete, verify the native tool output independently; answer text alone is
not proof of execution. All counts and ordering must reconcile with SQL.

Keep SQL data, retrieved policy, and proposed human action distinct in answers.
For combined questions, failure of either tool must be visible. No individual
feature-attribution mechanism is supplied, and risk cannot determine severity.

Only report **AGENT ACCEPTANCE PASS** after successful native query invocation,
verified ingestion/retrieval of both documents and the completed case evidence.
Until then, report the specific unfinished gate. No live result is asserted here.

## References checked 1 November 2026

- Tool authoring, SQL and RAG: https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/agent-flow-tools.html
- SQL tutorial capability description: https://docs.oracle.com/en/cloud/paas/ai-data-platform/videos.html
- KB ingestion: https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/knowledge-bases.html
- Agent creation: https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/agent-creation.html
