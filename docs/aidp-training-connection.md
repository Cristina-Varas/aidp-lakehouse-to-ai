# Existing AIDP connection and runtime inspection — 31 October 2026

## Existing connection reused

The active conversation did not expose ask-aidp in its tool list; querying that
server through the resource interface returned `unknown MCP server`. Read-only
inspection of the effective Codex configuration confirmed the enabled entry:

- Configuration: `~/.codex/config.toml`, server **ask-aidp**.
- Existing launcher: `~/.config/ask-aidp/ask-aidp-mcp`.
- The launcher sources the existing `ask-aidp.env` and execs the configured Node
  server. Its authentication/environment was reused intact, not reconstructed.
- Server handshake: **ask-aidp 0.10.0**; actual CLI help/version: **5.1.1**.
- Instance: **CristinaVaras-AIDPWorkbench**; workspace: **lakehouse_to_ai** ACTIVE.
- Region: **eu-frankfurt-1**; endpoint:
  `https://datalake.eu-frankfurt-1.oci.oraclecloud.com`.
- Compute: **lakehouse_demo_cpu** ACTIVE, `amd.generic`, x86_64.

An explicit stdio MCP client reused that exact launcher/entry. No second MCP
configuration, profile substitution, credential reconstruction, client restart,
compute restart or dependency/configuration change was required for connection.
The unrelated ask-aidp-datascience entry was never used. No token, password,
authentication variable value or wallet content was printed or copied into Git.
Raw operational responses are outside Git in a protected temporary directory;
only the non-secret inspection report is checked in.

Codex configuration behavior follows the
[official OpenAI MCP documentation](https://developers.openai.com/codex/mcp/).
The current conversation's tool exposure remains distinct from a successfully
connected server launched using its existing configuration; no client restart
was performed or claimed to be needed for these checks.

## Independently executed AIDP checks

Read-only workspace/cluster queries and four table metadata requests returned
success. All four Gold objects are **MANAGED / DELTA / ACTIVE** with the expected
40 / 8 / 17 / 15 column counts. The user then explicitly authorized one isolated
runtime diagnostic notebook and its one-task workflow on the existing compute:

```text
/Workspace/aidp-lakehouse-to-ai/_diagnostics/runtime_readonly_20261031_83f6ed50/runtime_probe.ipynb
```

The same notebook/workflow was refined and run three times for read-only runtime
inspection. No canonical notebook/table was modified. No compute, MLflow run,
registered model or deployment was created. The approved diagnostic notebook,
workflow and their run/output metadata are the only created AIDP resources.
No automatic cleanup deletes them; retain them for review.

The notebook counted Gold directly: **30,600 operations, 296 KPI groups,
30,000 training rows, 600 scoring rows**. Fingerprint equality, zero subsequent
inserts, reconciliation PASS and 9,816 late deliveries remain attributed to the
user's Gold execution report; the diagnostic did not rebuild Gold or recompute
those fingerprints/late counts.

The helper's automatic output export initially returned 404. The same completed
run's output was retrieved through the documented
[fetchOutput API](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aiwap/op-aidataplatforms-aidataplatformid-workspaces-workspacekey-taskruns-taskrunkey-actions-fetchoutput-post.html).
No diagnostic run was repeated merely to work around export. Later refinements
were additional reads of runtime capabilities in the same approved resources.
The probe uses base64 for its non-secret JSON report because NOTEBOOK export
rendering did not preserve escaped quotes in Python API signatures.

## Actual runtime and MLflow limitation

The live report is [aidp-training-runtime.json](aidp-training-runtime.json):

| Component | Observed in the diagnostic runtime |
| --- | --- |
| Python | 3.11.13 |
| Spark engine | 3.5.0-dataflow-3-5-2-12-df.1.0.103 |
| PySpark distribution | 3.5.9 |
| Delta Python distribution | 3.1.0 |
| scikit-learn | 1.8.0 |
| pandas | 2.2.3 |
| NumPy | 2.4.6 |
| MLflow module / mlflow-skinny distribution | 3.16.0 |
| Full `mlflow` distribution | Absent; this is not a missing module |
| skops | 0.14.0 |
| Request-auth provider | mlops_auth_provider |

Native sklearn logging exposes `name`, `code_paths`, `signature`,
`pip_requirements` and `skops_trusted_types`; its serialization default is
**skops**. set_experiment/start_run/log_dict/register_model/load_model and the
required sklearn estimators/encoders are present. The standalone
[probe](../notebooks/probes/mlflow_runtime_probe.py) makes no training/scoring or
MLflow write calls and does not print full URIs or environment values.

The native registry REST search returned HTTP 200. The proposed experiment lookup
returned 404 (not created/accessible), not proof that experiment creation is
permitted. In the **diagnostic workflow context**, however, constructing
`MlflowClient()` raised `MlflowException / INVALID_PARAMETER_VALUE`; safe flags
identified the filesystem backend maintenance-mode / MLFLOW_ALLOW_FILE_STORE
condition. The configured tracking/registry strings have no URI scheme.
Authentication-provider presence alone does not prove that the Python client's
native tracking/registry binding is ready.

This is a Python/native-MLflow context prerequisite, **not a failure of the
reused Codex–AIDP connection**. Do not replace the native URIs, switch OCI
profiles, install another MLflow package or enable local file-store fallback to
hide it. Review the native Experiments binding in the actual interactive notebook
session before running notebook 04. Its preflight must succeed before fitting.
The diagnostic result does not establish that every interactive session has the
same binding problem.

Oracle documents [native Experiments](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/experiments.html),
[Models](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/models.html)
and the requirement to restart associated compute/create a new compute when
first enabling these features. No restart was performed; this is not asserted
as a guaranteed fix for the observed context. The implementation preserves
native configuration and fails with a clear message if it remains unready.
Actual AIDP model training, logging, registration and version reload are pending.
