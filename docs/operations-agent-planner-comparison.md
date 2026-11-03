# Isolated planner LLM comparison — 3 November 2026

**Comparison completed; candidate not promoted.** Cohere inference was reachable,
but the four required cases did not pass the response contract. The isolated copy
was restored to its original model/parameters and an actual SQL control passed.
The canonical agent was never changed. A subsequent bounded
[Meta comparison](operations-agent-planner-comparison-meta.md) also did not pass
the four-case contract; its original copy configuration was restored afterwards.

| Candidate configuration | Cases | PASS | FAIL |
| --- | ---: | ---: | ---: |
| `cohere.command-a-03-2025`, inherited `modelSettings=null` | 4 | 0 | 4 |
| Same model, planner-only `max_tokens=2048` | 4 | 1 | 3 |

The second configuration produced actual SQL and RAG calls. Its failures were:

- **premium:** correct policy meaning, but no retrieval or source citation. The
  instructions already contain constraints; repeating them does not prove RAG.
- **escalation:** actual RAG call and illustrative one-hour target, but no source
  filename/title/section citation. The after-notification wording is ambiguous;
  it is not certified as a clock-start rule.
- **combined:** actual SQL/RAG calls and correct top-five IDs, but invents an
  acceptance-time at-risk marking requirement, extends the prediction-input
  restriction to operational review, and provides no source citation.
- **causality — PASS:** actual `get_order_risk`, correct stored score/threshold,
  no individual causal attribution and no invented policy source.

These are four acceptance probes, not a general model-quality benchmark. A model
being offered or producing fluent text is not equivalent to passing tool-use,
source-fidelity or citation checks.

## Scope and model discovery

The user authorized changing only the planner in the existing
`lakehouse_to_ai_operations_policy_fault_dev`. Its seven native tool definitions,
including the RAG model `openai.gpt-oss-120b`, stayed byte-identical. The canonical
planner, predictive ML model/version, KB, documents, data, computes, inactivity
settings, native authentication and installed dependencies were preserved.

Ask-AIDP has no dedicated LLM-inventory tool. We inspected the installed SDK and
current Workbench client, then used that client's signed read-only model route
with the **same existing launcher/profile**. No MCP configuration or credential
store was created or changed.

Workbench's enum constant `_AllResourceTypeValues.AI_MODEL` serializes to
**`GEN_AI`**, not the literal `AI_MODEL`. Its picker uses `modelName` and `vendor`
for the LLM `modelId` and `provider`. The instance identity and actual compartment
were read before querying regional models; no tenancy/compartment was invented.
The correct Frankfurt CHAT query returned **eight offers**. Only the selected
candidate's inference was tested; this is not proof that every catalog offer is
usable by this runtime.

We selected `cohere.command-a-03-2025`, provider `cohere`. Oracle documents
[tool use, RAG and multilingual capabilities](https://docs.oracle.com/en-us/iaas/Content/generative-ai/cohere-command-a-03-2025.htm).
That motivated the trial; it does not certify AIDP integration or these answers.

## What was actually verified

The availability conversation asked only “¿Cuántos pedidos abiertos hay?”—no
counts or operating rows were injected. It returned HTTP200 and the model trace
requested Cohere, but only announced intent to call `risk_summary`; no SQL call
occurred. Model inference access passed; the SQL/tool-use probe failed.

Trace `gen_ai.request.model` identifies the configured candidate. The response
model is `unknown`; no independent server-weight attestation is claimed. System
prompts and chat tool-result bodies remain redacted.

With inherited settings, all four case replies ended abruptly and invoked no
tools. The API reported zero token counters and exposed no useful finish reason;
we do not interpret those zeros as actual usage or assert an exact default limit.

One controlled settings trial added `max_tokens=2048` **only to the copy planner's
`modelSettings`**. The installed AIDP `AgentDiagramAgentNode` documents this map
for model-specific inference parameters; the installed OCI SDK and
[Oracle CohereChatRequest reference](https://docs.oracle.com/en-us/iaas/tools/python/latest/api/generative_ai_inference/models/oci.generative_ai_inference.models.CohereChatRequest.html)
define `max_tokens` (wire name `maxTokens`). Readback confirmed the requested map.
Replies were then complete and three cases invoked tools; the original effective
limit remains unobserved. No temperatures, RAG settings, prompts or tool schemas
were changed during this candidate comparison.

That redeploy request returned MCP error **“This operation was aborted”**. It was
**not resubmitted**. Read-only agent/deployment lookups recovered an ACTIVE
deployment with the exact request description and intended draft settings.
Only then were the four cases submitted. The missing RPC response is not recorded
as HTTP201 or as a demonstrated deployment failure.

All case conversations were new, question-only sessions. Default and explicit
budget variants have separate configuration hashes/results; successful answers
were not stitched across them. There were **ten fresh conversations**: one
availability probe, four cases per variant, and one restoration control.

## Independent RAG finding

A separate native RAG Test used the **exact query** from the candidate's combined
case, retaining the existing RAG model/configuration. HTTP200 returned four chunks
covering both approved TXT files. `verify_policy_chunks` matched their source
identities and normalized text to the original policy documents.

Its visible synthesized answer nevertheless added that later outcomes must not
be used to “alimentar o validar predicciones”. The approved policy excludes later
outcomes from acceptance-time prediction **inputs**; it does not state a ban on
post-hoc validation. That extra restriction is unsupported.

This is a demonstrated issue in **that independent RAG synthesis**, not proof that
the earlier hidden conversation result was identical or that every error came
from RAG rather than planner generation. Correct source chunks alone do not
certify a faithful synthesized answer. Changing only the planner did not satisfy
the full contract.

## Restoration and remaining status

The copy was restored to `openai.gpt-oss-120b` with original `modelSettings=null`,
validated, redeployed HTTP201 ACTIVE and reread against its baseline hash. Its
post-restoration control made a real `risk_summary` call, returned one SQL row
and correctly answered **600 open orders**. Model request trace matched the
original model. The canonical hash remained:

`277c4e8c7ca8c26a2a32691c9d238f042c16be5cc07fdb8cda0714d2dd06fb3c`.

No inconclusive raw SQL parameter request was replayed; the nine original unknown
outcomes remain separately documented. Strict native argument validation is not
certified. No training/scoring, data write, source upload, Autonomous operation,
compute resizing or dependency/authentication change was performed.

The canonical acceptance results remain **12/15 normal PASS and 2/2 isolated
fault PASS**, with three policy-fidelity failures. **AGENT ACCEPTANCE PASS is
false; PR #1 remains a draft.** This experiment does not authorize or justify
promoting the candidate to the canonical agent.

See [machine-readable comparison](operations-agent-planner-comparison.json),
[canonical results](operations-agent-acceptance.json), and
[parameter recovery](operations-agent-parameter-recovery.json). Raw responses,
full diagrams, headers and credential references stay outside Git in the existing
protected local runtime. Project code did not change during the comparison;
the earlier 32 local operations tests are separate SQLite/simulation checks,
not proof of native model behavior.
