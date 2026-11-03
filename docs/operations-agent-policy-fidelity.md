# Policy fidelity verification — 3 November 2026

The existing DEV agent remains functional for native SQL and policy retrieval.
The response contract still separates prediction, ten fitted inputs, original
UTC date derivation sources and metadata. **Policy fidelity has not passed.**

The final frozen revision **r3** produced:

| Scope | Executed | PASS | FAIL |
| --- | ---: | ---: | ---: |
| Normal cases, one canonical configuration | 15 | 12 | 3 |
| Controlled failures, isolated DEV copy | 2 | 2 | 0 |
| Additional focused smoke on final revision | 4 | 1 | 3 |

**AGENT ACCEPTANCE PASS remains false. PR #1 stays a draft.** The 17 acceptance
cases are not a mixture of successful answers selected from different revisions.
Intermediate r2 and its failed responses are [archived separately](operations-agent-policy-history.json).
There were **44 fresh conversations overall**, including both revisions and their
focused smoke tests; **21** belong to final r3 (4 focused + 15 normal + 2 isolated).

Final canonical operational configuration SHA-256:
`277c4e8c7ca8c26a2a32691c9d238f042c16be5cc07fdb8cda0714d2dd06fb3c`.
It was reread before each normal case, after the campaign and before/after each
isolated fault. Spark was verified ACTIVE through the existing Ask-AIDP launcher.

A later authorized [isolated planner comparison](operations-agent-planner-comparison.md)
produced 1/4 PASS with Cohere after a controlled output-budget adjustment. No
promotion was made; the copy was restored and SQL checked. The canonical results
above are unchanged. An independent RAG Test also exposed an unsupported summary
restriction; source chunks were correct.

The subsequent [Meta trial](operations-agent-planner-comparison-meta.md) yielded
1 PASS / 2 FAIL / 1 PARTIAL; no promotion. Both planner trials were confined to
the existing copy, restored afterwards, with actual SQL controls. The canonical
results remain unchanged.

## Preserved configuration and scope

All seven native SQL/RAG definitions and the selected `openai.gpt-oss-120b` remain
identical to the working baseline. Only planner instruction text changed. The
actual ML model remains `lakehouse_demo.gold.late_delivery_acceptance/1`, training
run `fc64485e353c41168cb90ee0f0bc5f71`. No training, scoring, source upload,
business-data write, Autonomous operation, KB reingestion, dependency/auth change,
compute creation/resizing/restart or inactivity change was performed.

The two policy source documents were not edited. The instructions correctly
state safety first followed by three unordered considerations; illustrative
one-hour severity-2 acknowledgement without a clock-start; and no inference of
an incident or severity from score alone. These instructions are not a
deterministic response validator, and observed runtime answers violate them.

## Changes and observed failures

Initial focused r1 tests failed. The r2 repair put source-faithful final-answer
constraints at the front and used the documented `updatedNodes` planner update.
It improved some answers but introduced an overly broad retrieval instruction.
The r3 correction limits that gate to policy/procedure questions and requires
causal-attribution statements to come from `get_order_risk`/the saved model
contract. The final causality test uses SQL alone, lists only the ten fitted
inputs, and invents no individual attribution or policy source.

The normal r3 failures are:

- **combined:** prohibits using later information for operational prioritization.
  The policy excludes later outcomes from acceptance-time prediction inputs; it
  does not prohibit later evidence in human operational review.
- **premium:** treats PREMIUM as an accepted priority. It is a customer segment,
  distinct from accepted priority NORMAL/HIGH/URGENT in the generator/contracts.
  The core no-automatic-premium-override answer is correct, but the added mapping
  is unsupported.
- **severity:** correctly denies that score establishes severity 1, but cites
  `incidente_escalation.txt`, which is not an approved source. Its generic
  `retrieved_policy` marker is unverified, not a demonstrated retrieved citation.

Final focused smoke also failed: PREMIUM was mapped exclusively to customer
impact; escalation added “from notification” while contradictorily saying no
clock-start is defined; combined cited nonexistent sections 6–7 and restricted
operational review to acceptance-time information. Those failures remain visible
alongside the later normal answers. An occasional correct answer does not certify
stable policy fidelity.

## Native retrieval evidence and visibility limits

Two independent native RAG Tests used the actual failed premium/escalation queries.
Both returned HTTP200 and four chunks covering the two approved TXT documents.
`verify_policy_chunks` matched normalized chunk content and exact source identities
against the original policy text. The visible synthesized answers in those Tests
were faithful. RAG configuration is byte-identical in final r3.

This is **independent Test evidence**, not inspection of the corresponding hidden
chat result. Chat exposes tool names/arguments, SQL success/failure events and
assistant output; tool-result bodies and system prompts are redacted. We do not
claim to have seen them or proven a cache, deployment/compiler or LLM root cause.
Some replies also say SQL was “shown”; actual SQL-text visibility is unverified.

## Isolated, verified failure scenarios

The owned copy **lakehouse_to_ai_operations_policy_fault_dev** uses the same
workspace and existing dedicated AI compute. Its baseline differs from the final
canonical diagram only by key, display name and description. Each test changes
one tool configuration relative to that clone baseline:

| Case | Controlled difference | Actual failure evidence | Result |
| --- | --- | --- | --- |
| RAG unavailable | Only RAG points to an unavailable test-only KB reference | Native Test HTTP400: KB not found/not accessible; actual attempted RAG retrieval in fresh chat | Available order data shown; missing verified procedure stated; no invented policy citation |
| SQL unavailable | Only rank query becomes read-only `raise_error` | Actual `sql_tool.spark_sql.failure` events | `AIDP_SQL_TOOL_QUERY_EXECUTION_ERROR`; no cached/invented ranking |

The real KB, its two TXT documents/vectors, Object Storage, business tables and
shared compute configurations remain untouched. These are **isolated-copy error
handling tests**, not outages of the shared services or tests on the canonical
graph. Healthy-tool responses were not used as evidence of outage handling.

Native copy initially returned `toolsMap=null` and omitted `computeKey`. Inline
`planner.tools` was already empty in the reference-backed source diagram, so it
is not evidence of lost inline definitions. The map and references were hydrated
from the preserved canonical diagram. In r2 the first native Test
returned the explicit missing-compute configuration error; assigning the same
existing dedicated AI compute to clone metadata fixed that prerequisite. The
subsequent Test returned the intended inaccessible-KB error. This concrete repair
was not a retry of any inconclusive SQL parameter request.

In the final SQL-failure conversation there were **three internal rank calls**
with arguments `{"limit":20}`, `{}`, and `{"limit":10}`, and three failure events.
The controller submitted the conversation once. We do not claim the native agent
made no retries. The controlled query ignores the limit argument to raise an
error, so these calls do not certify original-tool argument/default behavior.

After both tests, controlled faults were removed from the copy. Its base was
restored, validated, redeployed ACTIVE and reread; the canonical hash was unchanged.
No post-restore clone chat test, deletion or shared-compute change is claimed.

## Raw parameters and local verification

No inconclusive raw SQL parameter request was resubmitted. The original matrix
and [recovery investigation](operations-agent-parameter-recovery.json) keep these
categories separate:

- Two HTTP400 request rejections; validation stage not established.
- Numeric string `"10"` accepted with the configured SQL cast and baseline rows;
  hidden intermediate coercion not observed.
- Three extra arguments with no observed result effect, not strict rejection.
- One original HTTP timeout correlated to a real Spark `INVALID_ORDER_ID` guard
  error using the entire bound SQL and log event time; original timeout retained.
- **Nine UNKNOWN outcomes** (four other timeouts and five transport errors).

Log filtering/time behavior was inconsistent; zero results are not proof of
absence. Native strict validation, compiled schema coercion/defaults and safe
agent retry behavior remain uncertified.

**32 local operations-agent tests passed** after the final instruction revision.
Query fixtures use SQLite adaptations/simulations, not real local Spark. Actual
native Spark SQL/RAG execution and the isolated failures above are separate cloud
evidence. Source package version remains 0.6.3; the existing ignored wheel
predates these instruction revisions and is not claimed to contain them. Native
canvas deployment uses the versioned instruction text and private diagram exports;
no shared-library installation was required.

Current [machine-readable execution](operations-agent-policy-fidelity.json) and
[all final case records](operations-agent-acceptance.json) contain responses,
arguments, verdict reasons, configuration/session/evidence hashes and limitations.
Raw traces, complete configurations and credentials remain outside Git.

No additional prompt repair or model change is included. A diagnostic comparison
with a different native planner LLM **only in the isolated copy** would need
separate authorization; the selected canonical LLM, RAG tool/model, ML model,
data and computes would remain unchanged.
