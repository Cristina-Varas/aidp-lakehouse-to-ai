# Operations agent continuation — 1 November 2026

## Actual result

**Notebook 06 passed in AIDP. Agent acceptance remains pending.** The imported
0.6.0 handoff was reviewed and applied on `feat/operations-agent-0.6.0`, preserving
main and the unrelated local `.DS_Store`. The ZIP patch base exactly matched
`a27072275b437c01a4797f8ac125224593f12c5b`; `git apply --check` passed before applying.
The supplied wheel's Python, policy and template bytes match the patched source.

The existing ask-aidp 0.10.0 launcher, CLI **5.1.1**, TypeScript SDK **5.1.1** and
original authentication were reused for all platform operations. No replacement
MCP configuration, dependency upgrade, profile change or native URI change was
made. The dependency checker located the SDK/CLI through the existing launcher's
`AIDP_VENDOR_NODE_MODULES` mapping; no vendored package was installed. Its FDE
component is absent and outside this existing agent-only milestone. App/recording
scope was not added from the 3.5.0 skill to this resumed project.

## Independently verified in AIDP

Workspace **lakehouse_to_ai**, instance **CristinaVaras-AIDPWorkbench**, region
**eu-frankfurt-1**, Standard catalog **lakehouse_demo**, existing Spark compute
**lakehouse_demo_cpu** was ACTIVE for notebook 06; it subsequently stopped under the unchanged
30-minute idle policy. On **2026-11-02**, the user started it in the console and
a fresh readback independently confirmed **ACTIVE**.

A new isolated notebook-06 workflow executed the canonical notebook's code using
the exact supplied project wheel on that kernel's temporary sys.path. This used
no pip command, shared library change or restart; existing 04/05 sessions were
preserved. The task had `maxRetries: 0` and completed **SUCCESS**.

| Check | Actual result |
| --- | --- |
| Readiness/provenance | AGENT DATA READY |
| Unique prediction/input orders | 600 / 600 |
| Alerts / non-alerts | 450 / 150 |
| Predictions Delta version | **1** |
| Scoring-input Delta version | **1**, independently read from predictions |
| Six query families in notebook Spark | NOTEBOOK SQL PASS |
| Top order / customer | O-00030255 / C-000550 |
| Top estimated risk | 0.5999136971617619 |
| Training / scoring executed | **No / No** |

The matching version numbers were discovered independently; predictions version
was not assumed from the handoff's input version. Both managed Delta table IDs
are recorded in the private readiness receipt. Source/model/probability/threshold
consistency and exact cohort coverage were checked before the query phase.
The earlier SCORING PASS/repeat was reported in the handoff; this execution
corroborates existing persisted results, not a new inference or scoring rerun.

The clean canonical 06 is also uploaded, with overwrite disabled, at:

```text
/Workspace/aidp-lakehouse-to-ai/notebooks/06_prepare_operations_agent.ipynb
```

The isolated execution notebook and one-task workflow are separately retained
under the project's `_diagnostics/operations_agent_060_20261101` path. No 01–05
notebook was executed or overwritten.

## Actual prepared native resources

- **AI compute `lakehouse_to_ai_agent_dev`: ACTIVE**, created after the user's
  explicit approval: AMD, **1 OCPU / 16 GB / one replica**, no horizontal scaling.
  Capacity was read back. It consumes AIDP Units while active; no hourly monetary
  price is asserted. Existing Spark sizing, idle policy and libraries were not changed.
- **DEV agent `lakehouse_to_ai_operations_adviser`: full DRAFT preserved**, attached
  to that dedicated AI compute. A separate SQL-only diagnostic deployment is
  ACTIVE; the complete six-tool adviser has not deployed successfully. Long-term memory and summarization disabled.
  The actual instructions, a bounded native Spark SQL probe and the prepared
  package's six guarded tool classes are saved in its canvas. Its service-managed
  `.aflow` is in the project folder. No other demo's agent/compute was reused.
- **Managed volume `lakehouse_demo.gold.lakehouse_to_ai_policy_sources`: ACTIVE**,
  folder **`/documents`**. Exactly two TXT files were uploaded with overwrite
  disabled, listed remotely, downloaded and compared byte for byte:

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| order_prioritization.txt | 1289 | 8d0214d1e39b000daae016c7fcb091b8e26657c1d738facecb8007538ef6f454 |
| incident_escalation.txt | 1342 | 7ad708d4453f5d3300663ea17ade94013b93a7567acac555d851c43817254276 |

Only those files belong to the ingestion source. Model artifacts, code, prompts,
receipts and manifests are excluded from this volume/folder.

The six-file deployment bundle is also uploaded to the project's separate
workspace assets folder `operations-agent-0.6.0`. It is **not** a KB source. A
second local build changed **zero files**. Exact resource keys, raw outputs,
operation identities and cleanup ownership remain in protected runtime outside
Git; the sanitized [execution summary](operations-agent-execution.json) contains
logical names and results only.

## User-selected model and native deployment observations

The user reported selecting a model and saving the DEV draft. A fresh native
read independently confirmed **google.gemini-2.5-flash**, provider **google**,
region **eu-frankfurt-1**. That UI save had retained the model but lost the
canvas edge and inline tools. Only the missing prepared edge/tools were restored,
with fresh reads and conditional updates preserving the selected model.
Native validation then returned **isValidAgent=true**, no validation errors.
The earlier AFV_AGENT_LLM_REQUIRED result is historical and resolved.

One CLI deployment submission of the complete draft timed out. Readback found
**three implicit SDK attempts**, all terminal **FAILED / HTTP 500 /
InternalServerError**. No additional complete-agent deployment was submitted.
A bounded changed diagnostic retained the same model, instructions and compute,
removing only the Custom Code package from the deployed variant. A single signed
REST deployment request returned **HTTP 201 / ACTIVE** with the fixed native SQL
probe alone. This narrows the failing configuration but does not establish a
Custom Code root cause or prove native SQL execution. The subsequent driver
ERROR log search returned zero records.

The **complete two-tool draft has now been restored and revalidated successfully**
without redeploying. Its six guarded classes remain prepared, with an intentionally
empty sql_connection. The ACTIVE deployment is the diagnostic SQL-only snapshot;
it is not the completed operations adviser. A single subsequent diagnostic chat failed before SQL; see the dated update below.

## Gates that remain pending

Historical start blocker, now resolved by the owner: the selected Spark compute was STOPPED. The first start lookup accidentally
selected restart and was rejected before changes. The corrected /actions/start
request, explicitly carrying an empty JSON body, was also rejected with HTTP 400:
**The request body must not be null**. Neither call started or restarted compute;
no settings, libraries or idle policy changed. Further repair attempts stopped.
The owner was asked to start this existing compute in the AIDP console before
continuing the one-row SQL read probe. No native SQL result is claimed.

The installed 5.1.1 SDK defines SqlToolQueryType.SPARK, SqlToolConfiguration,
TestSqlToolDetails, Custom Code configuration and KnowledgeBase data types.
The exposed CLI/client/reference set lacks direct tool-test and KB lifecycle
operations. These are integration limitations, not proof that AIDP lacks those
features. The documented [SQL tool UI](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/agent-flow-tools.html)
and [KB UI](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/knowledge-bases.html)
provide supported operator paths. No private endpoint was invented.

A bounded signed chat transport was prepared using the actual exported diagnostic
deployment endpoint, the same existing launcher, SDK and authentication provider.
It was invoked once with Gemini (failure) and once after the owner-selected
OpenAI update (success), as documented below. The MCP REST wrapper does not expose that chat route;
preparation does not imply successful runtime authentication or a tool response.
No credentials, endpoint credential identifiers or raw headers are published.

**No KB was created or ingested, no RAG retrieval/citations were tested and none
of the 17 live acceptance cases ran. AGENT ACCEPTANCE remains PENDING.**

## Runtime update — 2026-11-02

The user reported starting Spark. Fresh Ask-AIDP reads confirmed
**lakehouse_demo_cpu ACTIVE**, the same diagnostic deployment **ACTIVE**, and
the existing agent/model identity. One signed read-only probe was then submitted
using the exact exported endpoint and unchanged launcher/authentication.

The native backend returned **HTTP 400 / AIDP_GENAI_CLIENT_ERROR**. The provider
returned **404 / NOT_FOUND** for **google.gemini-2.5-flash**, stating that the
model was not found or the project lacks access in the configured region.
This establishes a provider/model access failure, not a SQL query failure.
No SQL result, RAG result or acceptance-case success is claimed. No retry,
model substitution, training, scoring or data changes were performed.

The owner was asked to select/save an accessible model. Native draft validation
alone does not prove that a selected model can actually be invoked. The full
prepared draft and existing diagnostic deployment were left unchanged.

## OpenAI follow-up — 2026-11-02

The owner selected another model. Fresh reads confirmed **openai.gpt-oss-120b**,
provider **openai**, region **eu-frankfurt-1**, with Spark still ACTIVE. As before,
the UI save had removed edges/tools; the prepared topology was restored while
preserving that actual model. The one-row SQL-only diagnostic passed validation.

A create request returned **409**, because the existing diagnostic deployment was
already ACTIVE; it created nothing. The documented **redeploy** operation then
returned **201 / ACTIVE**. The complete draft was restored afterwards and again
passed native validation. No complete Custom Code redeployment was attempted.

One native chat returned **HTTP 200**, with empty envelope error code/message.
Its trace records exactly **one native_spark_sql_probe.tool** span and
**sql_tool.spark_sql.success: SUCCEEDED**, **one row / two columns**, with the
version-pinned query applied. The agent rendered **O-00030255** and
**0.5999136971617619**, matching the independently verified notebook-06 snapshot.
The native trace redacts the raw tool payload as **[REDACTED]**; an unredacted
payload comparison is not claimed. The prompt contained neither expected value.
The successful native configuration and verification receipt are retained locally
outside Git. This is **NATIVE SQL PROBE PASS**, not six-tool or RAG acceptance.

The Gemini error above is historical. OpenAI access is now verified. The remaining
Custom Code deployment blocker has not been repaired or retested, and the KB and
17 acceptance cases remain pending. No training, scoring or data changes occurred.

## Exact next operator steps

1. Preserve the selected OpenAI model and the successful native SQL configuration.
   The complete draft is saved; the ACTIVE deployment remains diagnostic-only.

2. Export the successful native SQL configuration and configure/test each guarded
   class, including invalid IDs/limits, valid missing orders and propagated
   native failures. Diagnose the complete draft's deployment separately; do not
   resubmit the timed-out operation or treat its retries as pending.
3. Create dedicated **lakehouse_demo.gold.operations_policies** through the KB UI.
   Select lakehouse_to_ai / lakehouse_demo_cpu and an embedding model actually
   offered there; record chunk/overlap settings. Ingest only the policy volume's
   **/documents** folder with TXT enabled. Verify both ingestion jobs and actual
   retrieved numbered sections/citations before attaching the retrieval tool.
4. Validate/deploy the complete owned DEV graph, removing the diagnostic probe
   from agent-facing tools only after the six classes pass. Run all 17 cases in
   fresh sessions, retaining actual tool calls, results, citations and decisions.

No mainline layer, training, scoring or Autonomous publication was performed.

## Local verification and files

This Mac: **115 tests, 91 passed / 24 optional skipped**. All **12 agent tests**
also passed importing directly from the supplied wheel. Their adapter uses a
simulation and SQL semantics use SQLite; the separate notebook-06 checks above
used real AIDP Spark. The Linux handoff's 100 passed / 15 skipped remains its own
record, not a test count executed on this Mac.

```sh
.venv/bin/python -m unittest discover -s tests -v
PYTHONPATH=dist/aidp_lakehouse_to_ai-0.6.0-py3-none-any.whl:tests \
  .venv/bin/python -m unittest test_operations_agent -v
PYTHONPATH=src .venv/bin/python -m aidp_lakehouse.agent_bundle \
  --receipt /private/tmp/aidp-existing-mcp/operations-agent-0.6.0/agent-data-readiness.json \
  --output-dir /private/tmp/aidp-existing-mcp/operations-agent-0.6.0/operations-bundle
```

Private readiness receipt and bundle are on this Mac outside Git. Raw MCP files
must not be committed. The wheel is verified against the imported patch and
copied to the repository's ignored **dist/**:

```text
/Users/cristinavarasmenadas/Library/CloudStorage/OneDrive-OracleCorporation/4. FY27/1. Customers/Workshops/14. Quistor/aidp-lakehouse-to-ai/dist/aidp_lakehouse_to_ai-0.6.0-py3-none-any.whl
```

SHA-256: `ca6e82efc793a2dbb49ad382c33116534943f8a9bd013cad4a84a2ac2c7828d7`.
The existing local GitHub identity is used for branch/PR publication; the remote
integration's earlier HTTP 403 does not prove that local Git access is blocked.

## Ownership and cleanup

The protected run-state inventory distinguishes the new AI compute, DEV agent,
policy source volume/files, workspace assets, canonical 06 and isolated workflow/
notebook from every reused dependency. Deleting/stopping anything requires an
explicit cleanup decision and a fresh dependency check. Retain all reused
workspace/catalog/Spark/layers/model/source resources and ambiguous ownership.
No cleanup was executed. The approved project AI compute remains active for DEV
review/testing and continues consuming units until stopped.
