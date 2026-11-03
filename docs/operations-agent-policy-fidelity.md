# Policy fidelity follow-up — 3 November 2026

## Current status

The policy-only instructions are **deployed ACTIVE and validated** in the existing
DEV agent. Native readback confirms all seven SQL/RAG tool configurations and the
selected `openai.gpt-oss-120b` are identical to the prior working configuration.
The 0.6.3 saved-feature/causality correction remains intact. No policy source,
KB content, embeddings, model, data, compute configuration or inactivity setting
was changed.

**Response verification is PENDING**, not PASS. `lakehouse_demo_cpu` was reread
STOPPED. The user has been asked to start that existing compute in
**CristinaVaras-AIDPWorkbench → lakehouse_to_ai → Compute**, preserving its
configuration and inactivity policy. No new policy response, normal case or
controlled-fault conversation has been submitted in this follow-up.

Operational configuration SHA-256:
`c077d5f5eb6044e2cb77701e95b913b510f05a0e5a7058925a9db93feb48d05c`.
This is a policy-instruction revision of source package **0.6.3**, not a new model
or data release. The previously built 0.6.3 wheel predates this instruction-only
revision; it is not represented as containing the new text. Native deployment
reads the versioned instructions and does not require a shared-library update.

## Exact policy correction

The versioned `src/aidp_lakehouse/agent_resources/instructions.txt` now states:

- Safety-critical incidents first. Then consider accepted priority, remaining
  time to the original promise and customer impact together. The policy defines
  no hierarchy or tie-break among those three considerations. PREMIUM alone
  does not override an earlier contractual promise.
- Severity 2 means notifying the team lead and an illustrative one-hour
  acknowledgement target. It is not a mandatory maximum deadline or commitment.
  No detection/notification/opening time origin is supplied by the policy.
- Score or `predicted_late` alone establishes neither an operational incident
  nor its severity. Actual exception and impact evidence are needed first.

The causality, prediction/input/metadata and date-derivation instructions are
unchanged. The wording repair does not add a new business policy or edit the
retrieved source documents.

## Required execution sequence

1. Verify existing Spark ACTIVE through Ask-AIDP. Preserve the current canonical
   diagram export and model/tool bindings outside Git.
2. Execute **premium, escalation, combined**, in that order, in fresh sessions.
   Manually compare every policy assertion/citation with the two approved source
   documents. Do not start the full campaign unless all three pass; retain any
   failed response without blind retries.
3. Freeze/read back the final diagram digest. Run **15 normal cases** in unique
   new sessions, checking that digest before each case and after the campaign.
   Preserve the causal-regression case; do not reintroduce score-as-predictor or
   per-order attribution claims. Record SQL/RAG names, arguments, observable SQL
   events, assistant responses and actual citation references.
4. Test the two unavailable-service cases using the isolated DEV copy below.
   Do not change the final canonical agent or shared dependencies to cause faults.
5. Record normal/fault verdicts separately. Healthy-tool responses cannot verify
   an outage, and isolated-copy results are not same-canonical-config results.

Prepared execution helpers and full private exports are in the existing
`/private/tmp/aidp-existing-mcp/` runtime. The guarded runner refuses duplicate
submissions or unexpected configuration changes and requires a reviewed three-case
PASS before the 15-case phase. No helper silently restarts compute or changes auth.

## Isolated controlled-fault copy

A new owned agent **lakehouse_to_ai_operations_policy_fault_dev** was created in
`lakehouse_to_ai`. The native copy operation returned `toolsMap=null` and empty
planner tools. Its newly owned diagram was hydrated from the preserved final
canonical diagram and read back; model, planner instructions, seven tools and
tool references match. The original agent was not altered by this operation.
The copy is **not deployed** yet and has no tested fault behavior.

The copy uses the **existing dedicated** `lakehouse_to_ai_agent_dev` AI compute;
no compute was created, resized, restarted or reconfigured. Metadata-only agent
creation and later deployment do not imply creation of another compute.

Prepared variants differ from the final canonical graph by clone identity and:

| Scenario | Only behavioral change in the isolated copy | Required failure evidence |
| --- | --- | --- |
| RAG unavailable | `retrieve_operations_policy` points to a unavailable test-only KB reference | Independent GET returned HTTP404 `NotAuthorizedOrNotFound`; additionally require actual native RAG Test failure and attempted RAG call in the conversation |
| SQL unavailable | `rank_open_orders` uses a read-only `raise_error` query with a controlled marker | Actual `sql_tool.spark_sql.failure` event and no fabricated ranking |

The real `lakehouse_demo.gold.operations_policies`, its two TXT files and vectors
are untouched. The SQL variant writes no tables and stops no compute. The two
clone variant digests and their exact one-tool differences are preserved privately.
Check the canonical graph before and after each isolated scenario. Retain the copy
as an isolated diagnostic artifact; no cleanup/deletion is authorized here.

## Inconclusive raw parameters

The ten earlier inconclusive parameter attempts were **not resubmitted**. The
[recovery investigation](operations-agent-parameter-recovery.json) distinguishes
request rejections, numeric-string acceptance, extra arguments with no result
effect, and unknown outcomes.

Inspection of the installed SDK result contract and Workbench caller found a
synchronous native Test response containing SQL rows, without a status/statement
handle. Actual timeout/transport receipts contain no returned server request or
operation ID; body hashes are not server lookup handles. The inspected MCP/SDK
reference exposed no Test-status lookup. This does not rule out platform-internal
capabilities.

Authenticated `cluster search-logs` queries for the exact timeout window and
`INVALID_ORDER_ID` / `INVALID_CUSTOMER_ID` each returned HTTP200 with zero results.
The broader response was truncated, but one complete log record was recovered.
Its entire bound SQL matches `order_id="O-123"`, its event time is inside that
attempt window, and Spark reports `java.lang.RuntimeException: INVALID_ORDER_ID`.
This recovers one **SQL guard failure**, not an HTTP rejection or request-schema
validation result; the original HTTP timeout is retained.

Further narrow-window searches returned some records outside their requested
intervals. Combined with the zero message-filter results despite the recovered
error, this leaves filtering/clock behavior unresolved. Do not interpret zero
results as absence or claim exhaustive log coverage. The other **four timeouts
and five transport errors remain UNKNOWN**. No parameter request was repeated.
Compiled native-tool coercion/defaults and strict validation are not certified.

## Local checks

**32 operations-agent tests passed** in the existing project environment after
this instruction revision. Query fixtures use SQLite adaptations/simulations,
not real local Spark. They do not prove policy-answer fidelity in AIDP.

Machine-readable current state: [policy follow-up](operations-agent-policy-fidelity.json).
Historical 0.6.3 campaign: 13 PASS / 2 FAIL / 2 unexercised outage preconditions;
those observations are not results for this newly deployed instruction revision.
**AGENT ACCEPTANCE PASS remains false; PR #1 remains a draft.**
