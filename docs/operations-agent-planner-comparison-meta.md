# Second bounded planner comparison — Meta Llama 3.3

**Not promoted.** A second native alternative was tested only in the same isolated
DEV copy, then the original model/parameters were restored and SQL checked.
No canonical, RAG, predictive-model, data or compute configuration changed.

The regional catalog offered `meta.llama-3.3-70b-instruct`, vendor `meta`.
[Oracle's model reference](https://docs.oracle.com/en-us/iaas/Content/generative-ai/meta-llama-3-3-70b.htm)
identifies this exact model. We reused Workbench's model-name/vendor binding and
the documented Generic OCI `max_tokens` setting, at 2048, only in the planner.
Catalog presence was not treated as proof of inference access.

The availability probe returned HTTP200, requested Meta in the trace, invoked
`risk_summary`, and correctly answered **600 open orders**. The response-model
field is `unknown`; no independent server-weight attestation is claimed.

| Fresh-session case | Result | Actual observation |
| --- | --- | --- |
| premium | PARTIAL, not accepted as PASS | RAG called; no automatic premium override, safety first, three considerations. Only a generic translated policy title, no explicit filename/numbered section. Meaning/retrieval pass; full citation criterion is not certified. |
| escalation | FAIL | RAG called, but answer says one hour **after notification**, an unsupported clock-start. |
| combined | FAIL | SQL/RAG called and top-five IDs correct, but restricts human review to acceptance-time data and excludes later operational evidence. The policy only excludes later outcomes as acceptance-time prediction inputs. |
| causality | PASS | Actual order lookup; quotes the no-individual-attribution contract, invents no cause/policy source. Reply is in English; locale compliance is not claimed. |

Thus **1 PASS, 2 FAIL, 1 PARTIAL**. Partial is deliberately not counted as a pass.
The first [Cohere comparison](operations-agent-planner-comparison.md) also failed
the four-case contract. This was a bounded two-candidate diagnostic, not a sweep
of all catalog models, a general benchmark or proof that no model could work.

All seven native tool definitions and the RAG LLM `openai.gpt-oss-120b` stayed
byte-identical. The comparison changed only planner model ID, provider and its
output budget. Each case checked candidate/canonical configuration hashes;
sessions were new and operating rows were not injected into questions. Actual
chat tool bodies/system prompts remain redacted, with independent native RAG
Test evidence documented in the first report.

After testing, the copy was restored to original `openai.gpt-oss-120b` and
`modelSettings=null`, validated, redeployed HTTP201 ACTIVE and reread against its
baseline. A fresh control made a real `risk_summary` call, returned one SQL row,
and correctly answered 600. The canonical hash remained
`277c4e8c7ca8c26a2a32691c9d238f042c16be5cc07fdb8cda0714d2dd06fb3c`.

This candidate used **six fresh conversations**: availability, four cases and
restoration control. Both planner comparisons total **16**. No inconclusive raw
parameter case was replayed; the nine original unknown outcomes and lack of
strict native argument certification remain unchanged.

The [machine-readable record](operations-agent-planner-comparison-meta.json)
retains actual answers, tools, arguments, SQL events, model/config/session hashes
and reasons. Full configurations, native response headers and traces remain in
the protected existing runtime outside Git. No libraries, training/scoring,
source documents or shared resources were changed.

**AGENT ACCEPTANCE PASS remains false; PR #1 stays a draft.** Both candidates
were restored without promotion. Further policy-fidelity remediation must address
verified source/citation handling; changing only the planner did not satisfy the
contract in these probes. This is not a demonstrated diagnosis of the earlier
hidden chat payloads or a claim that every failure originated in RAG.
