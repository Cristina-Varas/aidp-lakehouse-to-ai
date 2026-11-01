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
**lakehouse_demo_cpu** ACTIVE.

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
- **DEV agent `lakehouse_to_ai_operations_adviser`: DRAFT**, attached to that
  dedicated AI compute, not deployed. Long-term memory and summarization disabled.
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

## Gates that did not pass

The real `agent validate-agent` operation returns **isValidAgent=false** with
**AFV_AGENT_LLM_REQUIRED**: planner `llm.provider`, `llm.modelId` and `llm.regionId`
are unset. A model from another demonstration was not copied or guessed. Select
an approved model offered by the target instance in the DEV agent UI and save;
Ask-AIDP can then reread that actual configuration.

The installed 5.1.1 SDK defines `SqlToolQueryType.SPARK`, `SqlToolConfiguration`,
`TestSqlToolDetails`, Custom Code configuration and KnowledgeBase data types.
The actual client/request set and CLI do **not** expose tool-test, KB creation/
ingestion or LLM-catalog selection operations. The MCP reference also lacks
those operations. This is a limitation of the exposed integration, **not proof
that AIDP lacks Spark SQL or KB support**. No private endpoint or method was
invented. The documented [SQL tool UI](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/agent-flow-tools.html)
and [KB UI](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/knowledge-bases.html)
remain the current supported operator path for those gaps.

The native SQL probe uses exact SDK keys and the verified Spark compute key, with
one fixed version-pinned SELECT and a one-row limit. It is a **saved proposal**,
not a successful native SQL-tool execution. The guarded classes deliberately
retain an empty sql_connection until that probe succeeds and its real native
configuration is exported. The Custom Code composition remains simulated-only.
The Spark inventory found no aidputils package; that is not an inspection of the
AI runtime and does not establish missing AI-compute libraries.

**No KB was created or ingested, no RAG retrieval/citations were tested, no agent
was deployed and none of the 17 live acceptance cases ran.** Neither AGENT DATA
READY nor an ACTIVE compute is AGENT ACCEPTANCE PASS.

## Exact next operator steps

1. In the saved DEV canvas select an available, approved LLM and save. Re-read
   it and rerun native validation; preserve unrelated owner edits.
2. Select/test `native_spark_sql_probe` directly using the attached AI compute.
   Expect O-00030255 and 0.5999136971617619. Export the real configuration, then
   configure each of the six guarded classes. Test them individually, including
   rejected IDs/limits, missing valid orders and native failure propagation.
   Remove the diagnostic-only probe from agent-facing tools after this gate.
3. Create dedicated **`lakehouse_demo.gold.operations_policies`** through the KB
   UI. Select **lakehouse_to_ai / lakehouse_demo_cpu**, an embedding model offered
   there (MULTILINGUAL_E5_SMALL is the documented multilingual candidate), and
   explicit chunk/overlap settings. Register **only** the source volume's
   `/documents` folder with TXT enabled. Start ingestion and verify both files'
   completed ingestion jobs. These are instructions, not recorded execution.
4. Attach `retrieve_operations_policy` using that actual KB's exported config.
   Test retrieval of both policies and required numbered sections. Genuine
   retrieved chunks/citations are required; file upload alone is insufficient.
5. Validate/deploy the owned DEV graph and read back its actual endpoint and
   version. Execute the 17 cases in fresh sessions and retain tool calls, raw
   results, actual citations and pass/fail reasons. Do not copy operating rows
   into an agent question to simulate native retrieval.

Until the missing native operations are exposed or these UI gates are completed,
retain **AGENT ACCEPTANCE PENDING**. Autonomous publication, source changes,
training, new scoring and outbound messages remain outside this milestone.

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
