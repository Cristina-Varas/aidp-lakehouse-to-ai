# Local verification — 0.6.3 response-contract candidate

2–3 November 2026, existing project environment on the local Mac. **135 tests:
111 passed / 24 optional skipped**; **32 operations tests** passed importing
directly from the 0.6.3 wheel. All 35 packaged source/resource files match source.
The role-query fixtures use SQLite adaptations, not real Spark. New fixtures
verify exact saved-contract roles, day units, output/provenance separation,
missing-artifact refusal, raw JSON parameter types and conservative observation
classification. No live SQL or new agent acceptance was executed for 0.6.3.

The actual training artifact was downloaded and its run/model identity checked
through native AIDP APIs. This read-only evidence is separate from the pending
response execution. The existing agent/KB/model are unchanged.

See [the focused correction and pending runtime steps](order-risk-response-contract.md).

## Historical 0.6.2 local report

# Local verification — operations adviser 0.6.2

Date: 2 November 2026. Environment: this Mac and the existing project `.venv`.

- **127 tests: 103 passed, 24 optional skipped**. No local real Spark execution.
- New native SQL tests compare guarded query results with the original generator
  on small SQLite fixtures, including ties, full columns, invalid IDs, invalid
  numeric limits, deterministic definitions and canvas preservation.
- Policy tests reject stale KB identities, wrong bindings, broader sources,
  changed chunking, fabricated chunk identities and unsupported policy text.
- Notebook JSON, AST syntax, cleared outputs and whitespace checks passed.
- Wheel modules/resources are compared byte-for-byte with source; operations,
  native SQL and policy tests also run directly from the built wheel.

Actual AIDP native SQL, 14 real Spark guard-predicate fixtures, two-document
SUCCEEDED ingestion, visible policy chunks and 17 real acceptance cases are
separate [cloud evidence](operations-agent-execution.md). Local simulations
cannot establish those results. Acceptance remains FAIL (16/17 pass).

Build output `dist/aidp_lakehouse_to_ai-0.6.2-py3-none-any.whl` is on this local Mac
and ignored by Git; no shared AIDP libraries were updated.

## Historical 0.6.0 report (different environment)

The following is preserved as historical evidence and does not describe current
Mac capabilities or the later AIDP execution.

# Local verification — operations adviser candidate 0.6.0

Date: 1 November 2026. Environment: isolated Codex Linux workspace, Python 3.12.
This is not the user's Mac, an AIDP kernel, a Spark service or an AI compute.

## Executed

- Default unittest discovery: **115 total, 100 passed, 15 skipped** (optional
  Spark/Delta/MLflow dependencies unavailable). Installed sklearn allowed more
  training-unit cases than in the earlier Mac report; counts are environment-specific.
- New operations-agent tests: **12 passed**. This includes 600-row synthetic
  fixture reconciliation, corrupt/mixed provenance rejection, SQL semantics in
  SQLite with only Delta table bindings substituted, deterministic ties,
  customer filtering, missing orders, unsafe inputs, bounded limits, deterministic
  bundle generation, overwrite protection, policy-byte hashes and notebook gates.
- Native Custom Code/toolkit adapter: **simulation only**, verifying six
  registered classes, parameter rejection before tool creation, fixed SQL with
  no native runtime placeholders and propagation of a failed native response.
- All canonical notebooks validate with nbformat; code cells parse and new
  notebook 06 has no saved outputs or execution counts.
- Candidate wheel built with setuptools. Packaged Python, policy and template
  bytes match source. All 12 agent tests pass when importing directly from it.
- git diff whitespace check passed.

Candidate wheel SHA-256:

```text
ca6e82efc793a2dbb49ad382c33116534943f8a9bd013cad4a84a2ac2c7828d7
```

The wheel is a local build output in ignored `dist/`, not a release uploaded to
GitHub, the user's Mac or AIDP. Build it in the connected local checkout before
installation. No generated source data, predictions or credentials were committed.

## Not executed

- Notebook 06 against the user's real Delta tables.
- Native Spark SQL tool availability, routing or authorization.
- Composition of the native SQL toolkit inside Custom Code on the target runtime.
- KB creation, two-document ingestion, retrieval or citation verification.
- Agent creation, deployment, chat or any of the 17 live acceptance cases.
- Autonomous business publication, retraining or new scoring.

The user-confirmed 600-row SCORING PASS remains attributed to the user. The
candidate's preparatory flags deliberately leave native SQL/RAG/deployment false.
