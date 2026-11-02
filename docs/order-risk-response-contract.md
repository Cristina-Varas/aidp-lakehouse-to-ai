# Order-risk response contract — 0.6.3 candidate

## Verification status

The correction is implemented and locally verified. **It has not been deployed
or verified in AIDP.** The last read of `lakehouse_demo_cpu` was STOPPED. One start
request using the documented native route and explicit `{}` body returned HTTP
400, `InvalidParameter: The request body must not be null`; it was not retried.
The existing working agent, its six SQL tools, RAG binding, KB contents, selected
LLM, model version 1, data and compute configuration have been preserved.

Direct native parameter tests and the new 17-case acceptance campaign remain
**NOT RUN / PENDING**. Earlier 0.6.2 outcomes are historical evidence, not results
for this candidate. Start the existing `lakehouse_demo_cpu` in
**CristinaVaras-AIDPWorkbench → lakehouse_to_ai → Compute**, without changing its
size or 30-minute inactivity policy, and verify ACTIVE before live SQL tests.

## Saved training contract is the source of truth

Ask-AIDP reread the FINISHED run
`fc64485e353c41168cb90ee0f0bc5f71`, listed its artifacts, and checked registry model
`lakehouse_demo.gold.late_delivery_acceptance`, version **1**, belongs to that run.
`feature_contract.json` was downloaded through the documented native workspace
artifact mechanism. Its temporary access URL stayed outside chat, Git and logs.
No model was loaded, trained, scored, updated or re-registered.

Downloaded artifact SHA-256:

```text
d56d52239c6801fe1a695b90e2ff50e2db3a4383d7bcd983cf62494b57a487eb
```

It matches the supported training feature contract. The query producer consumes
those saved bytes and records their digest, rather than deriving predictor roles
from whatever columns happen to be returned by an operational lookup.

The saved artifact defines 11 raw pipeline input columns. Fitted preprocessing
uses **4 categorical features and 6 numeric features**, including the single
derived numeric feature `promised_lead_days`:

```text
(promised_delivery_at_acceptance - accepted_at).total_seconds() / 86400
```

The original dates are UTC inputs used only for this derivation. They are not two
additional fitted feature columns. The response displays the same day-based value
using the difference in timestamp microseconds divided by microseconds per day,
with DOUBLE arithmetic to avoid decimal-scale rounding. No new derived feature,
model, target rule or business threshold is introduced.

## Response shape

`get_order_risk` keeps its name, key, routing, exact-ID guard and one-row maximum.
Its row contains `order_risk_response`, a JSON object with these explicit roles:

| Section | Meaning |
| --- | --- |
| `prediction` | `late_probability`, `predicted_late`, `decision_threshold`: output and decision rule, **never predictors** |
| `model_inputs.feature_values` | Only the 10 fitted categorical/numeric feature values named by the saved artifact |
| `derivation_source_dates` | Original accepted/promise dates, UTC usage and promised-lead-time formula |
| `metadata` | Business identifiers, model/run, cutoff, Delta versions, reference date and artifact digest; **not predictors** |
| `attribution` | Both individual feature attribution and causal explanation explicitly unavailable |

The lookup now includes `transit_distance_km`, `line_count` and `units` from the
already-existing pinned scoring table, because the prior operational projection
omitted them. No table/data/schema is changed. The other five SQL tools and RAG
configuration are unchanged. A valid but absent order still returns zero rows.

`agent_order_risk.TrainingFeatureEvidence` verifies the saved artifact and exact
run/version. `order_risk_response_query` builds the focused query;
`apply_order_risk_contract` changes only that tool's query and description.
`validate_order_risk_response` checks roles, types, provenance, actual prediction
consistency and the date derivation against a visible native result. A correct
payload does not by itself prove that the LLM describes it accurately.

## Reproducible preparation

Keep the authenticated run/version records, downloaded artifact and routing
configuration in the existing protected local directory, outside Git.

```sh
PYTHONPATH=src .venv/bin/python -m aidp_lakehouse.agent_native_sql \
  --receipt /path/outside-git/agent-data-readiness.json \
  --environment /path/outside-git/native-environment.json \
  --profile complete \
  --feature-contract /path/outside-git/feature_contract.json \
  --output-dir /path/outside-git/native-operations-063
```

A complete export now requires the artifact and contains its digest. It cannot
silently export the old flat response when the artifact is missing. Repeating an
identical build changes zero files; conflicting existing content is preserved.
The manifest still says PREPARED_ONLY, runtime_verified=false and
agent_acceptance_pass=false.

## Direct native parameter tests

`agent_native_verification.parameter_cases` defines **21 raw JSON cases**, using
independently verified business keys. They cover valid/absent orders, malformed,
numeric, null, omitted and quoted IDs, customer format, numeric/string/fractional/
excess/zero/boolean/null/omitted limits and extra arguments on both parameterized
and parameter-free tools. The prepared requests have not been executed in AIDP.

Use the existing native Test endpoint (the same supported path used by Workbench),
passing the exact final SQL config and raw `paramValues.values`. No LLM call is
involved. Record the wire-body digest, original JSON types, HTTP status, elapsed
time and response. At most two independent calls may run concurrently; do not
automatically retry a timeout or rejected request.

Classify only observed outcomes:

- **Rejected request:** actual HTTP 4xx, with the native error code. This alone
  does not establish which validation stage rejected it. Authentication failures
  are a separate category, never parameter-validation evidence.
- **Numeric string accepted:** compare visible rows with the numeric baseline.
  Record the configured SQL TRY_CAST and that native intermediate types were not
  observed; do not guess where earlier coercion occurred.
- **Extra argument has no result effect:** accepted request and rows identical to
  its baseline. It was not rejected. Do not infer hidden logging or processing.
- **Empty result:** zero rows, never proof of parameter rejection.
- **Timeout or transport/service failure:** UNKNOWN outcome, not a rejection.

The Test API accepts config/values, not the saved LLM-facing `inputSchema`.
Its results therefore do not certify compiled-tool schema coercion or defaults.
`classify_native_result` enforces these distinctions. Local classifier fixtures
are not observations of native behavior.

## One final configuration and fresh sessions

After Spark is ACTIVE:

1. Execute the focused native lookup and verify the visible structured response
   against the downloaded artifact before replacing the working lookup.
2. Apply only the lookup query/description and focused response instructions.
   Preserve the model, other six tool definitions, KB, graph and resource settings.
3. Validate/deploy, read back and freeze the operational configuration digest.
   `semantic_graph_sha256` includes model, instructions, queries, schemas and
   bindings; it omits only service audit fields. Retain the full raw graph too.
4. Execute the direct parameter matrix with those exact tool configs. Classify
   each actual result without normalizing inputs or treating timeouts as rejects.
5. Run all 17 unchanged acceptance prompts in unique new sessions, checking the
   same frozen graph before/after the campaign. Verify the causal response never
   presents the three prediction fields as predictors and never invents attribution.
6. Keep acceptance pending until both response behavior and parameter evidence
   have been reviewed. Preserve failed responses and unknown outcomes.

The two unavailable-service cases have explicit failure preconditions. The
0.6.2 campaign changed DEV tool configs between those cases; that method does
**not** meet this campaign's single-configuration requirement. Do not repeat it.
Only use a verified session-scoped failure mechanism or an actual unavailable
service without changing the final graph. Otherwise execute the prompt but mark
the failure condition **NOT EXERCISED / PENDING**, not PASS. Do not inject fake
tool output or silently relax the cases.

## Local validation

135 tests: **111 passed, 24 optional skipped**. SQL role/derivation fixtures use a
SQLite adaptation; these are not local real-Spark or live-agent tests. The new
native request classifier and immutable-configuration hash also have regression
fixtures. All **32 operations tests** passed importing directly from wheel 0.6.3;
35 packaged source/resource files match the current checkout byte-for-byte.

The ignored wheel is on this **local Mac**:

```text
/Users/cristinavarasmenadas/Library/CloudStorage/OneDrive-OracleCorporation/4. FY27/1. Customers/Workshops/14. Quistor/aidp-lakehouse-to-ai/dist/aidp_lakehouse_to_ai-0.6.3-py3-none-any.whl
```

SHA-256: `2fdd87ec8962d977381a1a6fda3af647561f8ab820b5b8a715edb58d2cc2699d`.
Nothing was installed into shared AIDP libraries. Notebook 01–05 changes are
version compatibility/text only; those notebooks were not executed.
