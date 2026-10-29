# Autonomous AI Lakehouse to AI

The database bootstrap is complete. No business tables or data have been created.

Use the existing SQLcl MCP saved connection **`CristinaVaras-ADBLakehouse-FY27`**.
Do not substitute `ADB_FY27_ADMIN`; `aidp_demo_admin` is not the saved alias.

See [bootstrap instructions](sql/bootstrap/README.md) and the
[execution record](sql/bootstrap/EXECUTION.md) for the verified database identity,
account policy, secure local credential mechanism, and rerun procedure.

The source-data milestone is implemented in `src/aidp_lakehouse/`:
deterministic ERP extracts, separate initial/incremental batches, defect
manifests, acceptance-time feature boundaries, fictional policies, validation,
and a separate OCI upload command. Reusable Python modules own the logic; future
notebooks should stay thin.

Read the [architecture](docs/architecture.md), [source-data commands and contract](docs/source-data.md),
and [verification record](docs/source-data-verification.md).

```sh
export PYTHONPATH="$PWD/src"
.venv/bin/python -m aidp_lakehouse generate --seed 42 --reference-date 2026-10-28 \
  --historical-orders 30000 --open-orders 600 --output-dir /private/tmp/aidp-lakehouse-source-20261028
.venv/bin/python -m aidp_lakehouse validate --data-dir /private/tmp/aidp-lakehouse-source-20261028
.venv/bin/python -m aidp_lakehouse upload --data-dir /private/tmp/aidp-lakehouse-source-20261028 \
  --storage-config /private/tmp/aidp-lakehouse-storage/storage.local.json \
  --batch initial --dry-run
```

The displayed output directory has already been generated locally; generation
refuses to overwrite it. Datasets and credentials stay outside Git. The initial
batch and both policy documents are uploaded in the shared
`CristinaVaras-bucket-AIDP` bucket under
`demos/lakehouse-to-ai/source-data/v1/seed-42/reference-2026-10-28/`.
The target is tenancy `oraemeaaidata2`, namespace `frgpslcqn7sm`, region
`eu-frankfurt-1`, compartment `CristinaVaras`. See the
[storage execution record](docs/object-storage-execution.md) for local
configuration, remote object verification and exact commands. Incremental files
remain local. No Autonomous business tables, data loading, model training, or agents
are implemented by this milestone.

The AIDP catalog/Bronze notebooks are prepared and locally validated:
[00_setup_catalog.ipynb](notebooks/00_setup_catalog.ipynb) and
[01_register_bronze.ipynb](notebooks/01_register_bronze.ipynb).
See [AIDP Bronze setup](docs/aidp-bronze-setup.md) for workspace/compute,
permissions, package installation, execution order and expected source counts.
AIDP runtime verification is pending; the notebooks have not been executed in
an authenticated AIDP session. No catalog or Bronze table was created locally.
