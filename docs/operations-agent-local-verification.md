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
