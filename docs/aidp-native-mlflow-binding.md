# Native MLflow connection diagnosis — 1 November 2026

## Current result and boundaries

**Connection restoration is not yet verified in notebook 05.** The existing
ask-aidp server independently reads the intended experiment, successful run and
registered version, but the native Python client in an isolated workflow fails
metadata authorization. No model was loaded, trained or scored; no registered
model, source table, predictions table or native authentication was modified.

Release 0.5.1 adds a fail-closed native initialization stage to notebook 05. During
this diagnosis, execute **only configuration and section 1a**, then stop. Do not
Run All or execute model loading/scoring cells. `NATIVE CONNECTION PASS` means
metadata routing/identity passed, not model loading or SCORING PASS.

The initializer first preserves a working platform-injected HTTP(S) binding if
its metadata matches the exact project. Otherwise it tries an explicit client
using two non-secret documented API bases copied from successful ask-aidp GET
responses. Only after experiment/run/version reads match does it set both global
URIs and verify the default client. Failed candidate reads leave global routing
untouched. Failed default-client verification restores prior URI settings. It
never changes environment variables, auth providers, signers or dependencies.

The explicit bases are **API-verified candidates**, not a claim that native
resource-principal access has succeeded. Native authorization is checked before
activation. The successful default-binding path also preserves internal native
service hosts rather than replacing them with a public API host.

## Comparison actually performed through ask-aidp

The existing effective Codex launcher `~/.config/ask-aidp/ask-aidp-mcp` was reused,
with its existing environment and authentication. No additional MCP server,
profile substitution, credential reconstruction or client restart was needed.
Networked requests needed the execution sandbox's network permission; the initial
sandboxed requests timed out. Auth/configuration was not rebuilt to address that.

Read-only GETs independently verified HTTP 200 for:

- Workspace experiment **lakehouse_to_ai_late_delivery**, experiment ID **1**.
- Run **fc64485e353c41168cb90ee0f0bc5f71**, experiment ID 1, **FINISHED**.
- Model **lakehouse_demo.gold.late_delivery_acceptance**, version **1**, **READY**,
  linked to that exact run.

Saved notebook 04 contains successful native tracking/registry preflight,
exact-version reload and TRAINING PASS output. Its code has no explicit URI
setter. Notebook 05 contains its reported URI-shape diagnostic and preflight
failure. Both active sessions use **lakehouse_demo_cpu** on
**CristinaVaras-AIDPWorkbench / lakehouse_to_ai**, with distinct Python kernels.
The compute is ACTIVE; user-defined Spark environment settings are empty.

MCP reads saved code/output/session metadata; it does **not** provide a documented
operation for executing Python in those existing kernels. The live Python URI
values in the successful 04 kernel were therefore not independently retrieved.
Do not infer them from the compute identity or from saved outputs. The standalone
[context probe](../notebooks/probes/native_mlflow_context.py) can be run as one
read-only cell in each kernel; it prints only scheme/hostname and presence flags,
never full URI paths, environment values, principals or tokens.

## Native implementation inspected in the real compute

The existing isolated diagnostic notebook/workflow was reused, not either
canonical notebook:

```text
/Workspace/aidp-lakehouse-to-ai/_diagnostics/runtime_readonly_20261031_83f6ed50/runtime_probe.ipynb
```

The staged diagnostic inspected source and metadata, then tested read-only
clients. It did not install the project wheel or replace any native package.
MLflow **3.16.0 / mlflow-skinny** loads from `/aidp/default-library/python/amd/...`.
The native entry point is:

```text
mlflow.request_auth_provider:
  mlops_auth_provider -> mlops_mlflow_plugin.plugins.mlops_auth_provider:MLOpsAuthProvider
```

`get_auth()` obtains the native Spark context and Java
`ComputeLocalRequestSigner` through `auth_utils.get_resource_princpial_signer()`
(the misspelling is the runtime's actual function name). The provider adds the
existing Spark `dh-user-principal` and signs requests. It does not choose or
initialize the MLflow tracking/registry URL. No signer/header/token was printed
or copied, and no auth bypass was enabled.

The inspected `aidp_utils` helper exposes instance metadata/domain and a region
client config, not a workspace MLflow bootstrap function. No guessed helper API
or UI-derived endpoint is introduced. Oracle's
[Experiments sample](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/experiments.html)
uses set_experiment/autolog/start_run with an already provisioned native context;
it does not supply a URL initialization step. None of those write operations
was executed during diagnosis.

## Relative path versus native alias

In the diagnostic, the effective tracking/registry strings and the injected
Spark driver/executor tracking strings have **no scheme or hostname** and match
one another. The actual MLflow empty-scheme factories are `_get_file_store` for
both tracking and registry. Construction fails with FileStore's maintenance-mode
error. No local directory was found at that relative value.

This proves the observed token is treated as a relative filesystem location,
**not a supported native backend alias**. It does not establish that its literal
contents are a known placeholder spelling; those contents are deliberately not
printed. Installed native store entry points include `mdi` and `oracledb`, but
neither changes the observed empty-scheme routing. Provider presence alone is
not evidence of backend readiness. The existing FileStore rejection remains;
MLFLOW_ALLOW_FILE_STORE is never enabled.

## Native authorization result and exact platform prerequisite

Native explicit-client probes used the exact bases of the documented, HTTP-200
MCP reads: tracking is workspace-scoped, registry instance-scoped. These were not
inferred from the Workbench browser URL. The Oracle
[run GET API](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aiwap/op-aidataplatforms-aidataplatformid-workspaces-workspacekey-mlops-api-2.0-mlflow-runs-get-get.html)
and [model-version GET API](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aiwap/op-aidataplatforms-aidataplatformid-mlops-api-2.0-mlflow-model-versions-get-get.html)
show the two scopes. Notebook CONFIG contains the actual non-secret bases in one
section; do not copy an interface URL or append guessed service paths.

In the **separate workflow diagnostic context**, auth provider selection and
Spark principal presence are true, but AIDP_ID and AIDP_WORKSPACE_KEY do not equal
the verified MCP context identifiers. They are not full instance OCIDs / the
verified workspace display name either. This comparison is evidence about the
workflow only; it does not prove notebook 05 has the same identity mismatch.
All three native reads fail with platform **NotAuthorizedOrNotFound** (wrapped
by MLflow as INTERNAL_ERROR), while the same documented resources return 200
through ask-aidp. That error cannot by itself distinguish missing authorization
from an incorrectly scoped native session.

The required platform state is therefore:

1. Notebook 05's own kernel must receive the native instance/workspace MLflow
   routing for **CristinaVaras-AIDPWorkbench / lakehouse_to_ai**, not the observed
   unqualified default value. Preserve the native auth provider and native libraries.
2. Its existing Spark/Java principal must be authorized to read experiment 1,
   the pinned run and the pinned registered version in that context. An external
   SDK profile with access is not a replacement for this native principal.
3. Run section 1a only and require all three metadata identities to match through
   both the explicit/native client and the effective default client. If it fails,
   have the workspace/compute administrator repair the native session binding
   and/or its permissions; do not synthesize a principal or enable FileStore.
4. If comparing kernels, use the safe context probe in the still-successful 04
   session. The exact missing initialization event remains unproven until that
   live comparison or platform repair; opening a panel is not asserted as a fix.

Oracle documents restarting associated compute on first use of
[Experiments/Models](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/models.html).
Here 04 has already succeeded, so restart is **not proven sufficient**, and none
was performed. Coordinate any platform repair/restart with the owner to preserve
sessions; do not rerun training. After connection diagnosis is resolved, model
loading/artifact verification and eventual scoring remain separate later cells.

## Local verification and delivery

The default suite passes **103 tests: 79 passed, 24 optional skipped**. Seven new
binding tests are **simulations**, not native authorization success. They verify
read-before-set ordering, preservation of a working native route, no mutation on
candidate authorization/identity failure, URI rollback on default-client failure,
unsafe/local/credential-bearing route rejection, and missing-provider/active-run
stops. Existing notebook checks remain green. New backend code never calls fit,
predict, model loading, run creation or model registration.

The new initializer was actually executed as source in the isolated native
workflow, without installing a wheel. It returned **BLOCKED /
NotAuthorizedOrNotFound**, with **global routing unchanged**, **authentication
unchanged**, **model_loaded=false** and **scoring_performed=false**. This validates
its fail-closed behavior, not successful restoration. See the
[sanitized execution receipt](aidp-native-mlflow-diagnosis.json).
A successful native initializer or model reload in notebook 05 is **not claimed**.
Only the isolated diagnostic notebook/workflow was changed in AIDP; canonical
04/05, tables, registry resources and native configurations were preserved.

Build the project wheel using the existing local environment, keeping dist ignored:

```sh
.venv/bin/python -m unittest discover -s tests -v
PYTHONPATH=/opt/homebrew/Cellar/oci-cli/3.94.0/libexec/lib/python3.14/site-packages \
.venv/bin/python -c "from setuptools.build_meta import build_wheel; print(build_wheel('dist'))"
```

Install only project wheel **0.5.1** with no dependency replacement, restart Python
as required for a new project wheel, and import the updated notebook. During this
diagnosis run only its configuration and metadata-binding stage. Raw MCP outputs
and provider source remain outside Git in protected local temporary files.

Verified wheel on the user's local Mac:

```text
/Users/cristinavarasmenadas/Library/CloudStorage/OneDrive-OracleCorporation/4. FY27/1. Customers/Workshops/14. Quistor/aidp-lakehouse-to-ai/dist/aidp_lakehouse_to_ai-0.5.1-py3-none-any.whl
```

SHA-256: `afec9872a5c824a2bc62647ceae392d6b45bb2e4984af293fd34a40867aaa190`.
All packaged Python modules match source. Seven simulated binding tests also pass
when importing the final wheel directly; these overlap the default suite.
Notebooks validate structurally, parse, and contain no saved output. Existing
transformation, training, model-loading and publication modules are unchanged;
only shared package-version guards are extended. No source files or objects in
Object Storage were changed.
