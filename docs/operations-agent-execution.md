# Operations agent execution — 2–3 November 2026

## Current follow-up: 0.6.3 response contract — PENDING

The focused correction is implemented and locally verified against the actual
saved training feature artifact. **It has not been deployed or tested in AIDP.**
The working 0.6.2 native agent/KB/model remain unchanged. Project Spark is STOPPED;
one explicit-body start request returned HTTP400, `The request body must not be null`,
and was not repeated. Manual start of the existing compute is required.

The **21 direct native parameter calls** and **17 new acceptance sessions on one
final configuration** are prepared but **NOT RUN**. No earlier result is reused
as current acceptance. See [focused response contract and execution steps](order-risk-response-contract.md).
Current local checks: **135 tests, 111 passed / 24 optional skipped**, plus 32 tests
from the built 0.6.3 wheel. No shared AIDP package, business data or model changed.


## Verified 0.6.2 baseline (previous campaign)

**DEV operational with six native SQL tools and policy RAG. Acceptance: FAIL
(17 executed, 16 passed, 1 failed). No AGENT ACCEPTANCE PASS.**

The dedicated agent `lakehouse_to_ai_operations_adviser` is ACTIVE using the
owner-selected `openai.gpt-oss-120b`. Custom Code and the diagnostic SQL tool are
absent from its exposed tools. The final native canvas shows six SQL tools and
one RAG tool; a fresh combined conversation called SQL and RAG successfully.

The remaining failure is **causality**. After a targeted instruction repair, the
model still described `late_probability`, `predicted_late` and `decision_threshold`
as model inputs. These are outputs/metadata. It did disclaim individual feature
attribution, but that does not make the unsupported predictor claim acceptable.
The failed responses remain recorded; further blind prompt retries stopped.
The PR stays a draft and has not been merged.

[Machine-readable execution](operations-agent-execution.json) and
[all 17 case records](operations-agent-acceptance.json) contain actual arguments,
responses, citations, SQL success/failure events, verdict reasons and protected
raw-response hashes. They retain configuration revisions: this bounded campaign
is not a claim that all cases passed on one final immutable configuration.

## Corrected KB and ingestion

The user recreated `lakehouse_demo.gold.operations_policies` and renamed the old,
empty object to `operations_policies_unconfigured_20261102`. The old object was
preserved and was not ingested or referenced by the final RAG configuration.

| Saved property | Verified value |
| --- | --- |
| New KB key | `b5310533e4fe43e3a8c0f3234c569f3a` |
| Workspace | `lakehouse_to_ai` |
| Spark compute | `lakehouse_demo_cpu` |
| Embedding model | `MULTILINGUAL_E5_SMALL` |
| Chunk size / overlap | **1024 / 128 characters** |
| Source | `/Volumes/lakehouse_demo/gold/lakehouse_to_ai_policy_sources/documents` |
| Source filter | `*.txt` |
| Registered sources | **1** |

Ask-AIDP reread the project compute as ACTIVE. Its CLI/reference exposes no KB
lifecycle, and its REST tool rejects `/20240831` with:

```text
path must start with /20260430/aiDataPlatforms/{aiDataPlatformId}/.
```

The fallback read the actual KB route from authenticated Workbench and its
public current JavaScript client, and checked request models in the installed
Oracle SDK. Native signed requests reused the **same existing Ask-AIDP launcher,
profile and authentication file**. No endpoint was inferred from the UI URL,
no credential was copied, and no new MCP configuration was created.

The saved new key, binding, embedding model and chunk settings were verified
before the first source update. Ask-AIDP listed and downloaded `/documents` again:
only these two approved files were present, matching original policy bytes.

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `order_prioritization.txt` | 1289 | `8d0214d1e39b000daae016c7fcb091b8e26657c1d738facecb8007538ef6f454` |
| `incident_escalation.txt` | 1342 | `7ad708d4453f5d3300663ea17ade94013b93a7567acac555d851c43817254276` |

A single guarded source update requested inline ingestion. Source key:
`3a87dd5cae9b4ae29ba23b9a59a98a11`. Ingestion run
`83b2985c187b4e008a685c945b05ed6c` finished **SUCCEEDED**, with **2 added,
0 modified, 0 deleted**. No second ingestion was submitted. The metadata counters
`currentProcessedItems` and `timeLastEmbedding` remained null; they were not used
to infer success. The job result and actual retrieval provide the evidence.

The source-level workspace, compute, filter and chunk settings were reread and
match the KB. The folder was never broadened to include other volume contents,
receipts, control files, prompts, code or model artifacts.

## Real retrieval and citations

Two fresh conversations invoked `retrieve_operations_policy`, one for each
policy. They returned queue/risk guidance and severity/communication guidance
with the corresponding TXT identities. Chat traces redact raw tool payloads;
we do not claim to have seen the hidden chat retrieval content.

Independent **native RAG Test** calls returned visible `retrievedChunks`, including
both policies' `#chunk_0` and `#chunk_1` document IDs beneath the exact `/documents`
path. All four chunks match the approved source text after whitespace
normalization. This independently verifies document identity, content and section
availability. `agent_policies.verify_policy_chunks` performed that comparison.

The native SDK uses `inputSchema: {"query": ""}` for RAG, unlike SQL's flat parameter
map. An initial SQL-style RAG schema was rejected with HTTP 400
`Unable to process JSON input`; the installed `RagToolInputSchema` established
the correct type. The corrected definition validated and deployed successfully.
RAG uses catalog/schema/name and topK=4, as supported by this runtime. Its name
was resolved to the verified new KB; no old UUID appears in the final diagram.

## Native SQL alternative and its boundaries

The Custom Code loader failure remains historical and unresolved. No platform-owner
logs were obtained and no new Custom Code deployment was attempted in this
continuation. The working alternative uses native SQL definitions generated by
`agent_native_sql`, retaining the fixed **prediction/scoring Delta versions 1/1**
and the receipt's table identities. Gold metadata was reread with Ask-AIDP:
both inputs are managed Delta tables. No Gold, Silver or Bronze data was written.

| Tool | Direct native Test rows | Conversation evidence |
| --- | ---: | --- |
| `risk_summary` | 1 | 600 opens / 450 alerts / 150 non-alerts |
| `rank_open_orders` | 10, and separately 20 | Deterministic score-descending / order-ID-ascending ranking |
| `get_order_risk` | 1 | Correct ID, customer, score, model/run and cutoff |
| `get_customer_orders` | 2 | C-000550 only, total_matching=2 |
| `risk_by_carrier` | 3 | Groups reconcile to 600 / 450 |
| `risk_by_region` | 4 | Groups reconcile to 600 / 450 |

All six native Tests returned visible structured rows. The exact top score was
`0.5999136971617619` for `O-00030255`, matching the earlier notebook-06 receipt.
These values checked results; they were not supplied as expected answers in the
agent prompts. All displayed ranking IDs, including the 20-row limit case, were
compared with independent native Test rows. Actual native conversation events
show SQL successes with the expected row and column counts. Missing order
`O-99999999` returned zero rows rather than an invented score.

Static queries are unchanged. Parameterized templates use Oracle's documented
quoted string placeholders, fixed business-key guards, and an integer 1–20 rank
bound. Invalid values select `raise_error`, not an empty business result. The rank
query retains a fixed LIMIT 20 and explicit output columns. A separate real Spark
constant-only query verified **14 guard predicates**, including trailing newline,
short IDs, fractional/excess/zero/negative limits, boolean text and NaN text.
This establishes predicate semantics, not every native binding/type behavior.
Earlier constant-ID conversations independently showed malformed/quoted input
causing native SQL failure and the valid input returning unchanged.

One direct native Test with an invalid business ID timed out after 150 seconds.
Its outcome is **UNKNOWN**, not a confirmed rejection, and it was not replayed.
The remaining prepared negative Test requests were not submitted. Native
no-input tools also accepted spurious empty-name arguments generated by the
model. Unexpected-argument rejection and exact numeric-coercion equivalence with
the stricter Python adapter are **not certified**. The original Python adapter's
checks remain intact. These differences are not silently presented as identical
validation; native fixed SQL and guarded known parameters bound the reads.

## Acceptance and restoration

Every case ran in a new session. SQL/RAG content hidden by chat traces was checked
against the independent native Tests and approved policy bytes. Public records
contain sanitized answer text and evidence summaries; headers, principal IDs,
full traces and private deployment configurations remain outside Git.

The initial combined case failed: it called SQL alone and fabricated
`operations_policy.pdf`. A single instruction repair made current-turn RAG
mandatory before policy claims and prohibited unsupported predictor claims.
Combined passed afterward and again after the canvas conversion, with actual
SQL+RAG calls and the approved TXT source. Causality failed again and remains FAIL.
No failed evidence was discarded and no criteria were relaxed to obtain PASS.

The two deliberate failure cases did not stop or resize compute:

- **RAG unavailable:** a temporary, independently inaccessible KB reference in
  the DEV tool. SQL succeeded; RAG failed; the answer reported missing policy and
  supplied no fabricated citation. The real KB/vectors were untouched.
- **SQL unavailable:** a temporary read-only constant `raise_error` query. A real
  native SQL failure event was observed; the answer reported failure and did not
  invent a ranking.

The complete canonical configuration was restored in a finally block, redeployed
ACTIVE, and reread. It was then represented using the previously observed native
`toolsMap` / `toolReferences` and **bottom → top** ports. Workbench reload showed
**Tools (7)**: six SQL and one RAG. A fresh combined conversation passed after
that conversion. No diagnostic or Custom Code tool remains exposed.

## Local checks and wheel

**127 tests: 103 passed, 24 optional skipped.** Local SQL fixtures use SQLite with
explicit adaptations of Spark-only expressions. Adapter/session tests use
simulations. They are not local real-Spark tests. Actual Spark SQL, native RAG,
ingestion and conversation evidence above came from AIDP. Notebook JSON, Python
syntax, cleared outputs and diff whitespace were checked locally.

```sh
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m pip wheel . --no-deps --wheel-dir dist/
PYTHONPATH=dist/aidp_lakehouse_to_ai-0.6.2-py3-none-any.whl:tests \
  .venv/bin/python -m unittest test_operations_agent test_agent_native_sql test_agent_policies -v
```

The wheel is on the **local Mac**, in ignored `dist/`:

```text
/Users/cristinavarasmenadas/Library/CloudStorage/OneDrive-OracleCorporation/4. FY27/1. Customers/Workshops/14. Quistor/aidp-lakehouse-to-ai/dist/aidp_lakehouse_to_ai-0.6.2-py3-none-any.whl
```

SHA-256: `c104ce2d5ab052eb4f9597bacd8308f428893fa463ca2275f68824064048338d`.
No wheel or dependency was installed into shared AIDP libraries in this work.
Notebook 01–05 edits are package compatibility/version text only; none was run.
The original generator, bootstrap, Bronze/Silver/Gold transformations, model,
predictions, Object Storage sources and inactivity policy were preserved.

The remaining gate is a truthful causal/feature-role response and certification
of the noted native parameter boundaries. There is no outstanding manual KB
creation or ingestion step. The previous Custom Code history is preserved in the
JSON record and the [0.6.1 execution document](https://github.com/Cristina-Varas/aidp-lakehouse-to-ai/blob/d56eb6113b9d06125c1aba417f43e8c51173db11/docs/operations-agent-execution.md).

Official references used: [Knowledge Bases](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/knowledge-bases.html),
[native tool configuration and Test](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/agent-flow-tools.html),
and [deployed agent sessions](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/invoke-deployed-agent.html).
