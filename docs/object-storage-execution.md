# Shared AIDP Object Storage — 29 October 2026

## 1. Verified destination and account

| Setting | Verified value |
| --- | --- |
| Tenancy | `oraemeaaidata2` |
| Local profile | `DEFAULT` in the selected external OCI config |
| Region | `eu-frankfurt-1` |
| Object Storage namespace | `frgpslcqn7sm` |
| Shared bucket | `CristinaVaras-bucket-AIDP` |
| Compartment | `CristinaVaras` (`ACTIVE`) |
| Bucket lookup | HTTP 200 |
| Source-data prefix | `demos/lakehouse-to-ai/source-data/v1/seed-42/reference-2026-10-28/` |

The initial `~/.oci/config` was valid but pointed to `oraemeadatamgmt`, a
different tenancy. Exact-bucket queries there returned 404 and no uploads were
made. After the user identified `oraemeaaidata2`, an existing local config for
that tenancy was found. Its private-key path was incomplete. The matching
existing private key was identified by its public fingerprint, including files
with a text extension, and an effective config was written outside Git. Original
credential files and the default OCI config were preserved. No key, token,
password, credential contents or generated private key was printed or committed.

Authentication, namespace discovery, bucket lookup and compartment identity were
then verified using the correct existing credentials. The bucket is shared by
multiple demos and was not created or renamed. Only this demo/run prefix was
listed for post-upload object verification.

## 2. Shared-bucket structure

```text
CristinaVaras-bucket-AIDP/
  demos/
    lakehouse-to-ai/
      source-data/v1/seed-42/reference-2026-10-28/
        customers/initial/customers.csv
        orders/initial/orders.csv
        deliveries/initial/deliveries.csv
        events/initial/events.jsonl
        documents/order_prioritization.md
        documents/incident_escalation.md
        _control/initial/manifest.json
        _control/initial/ingestion_metadata.json
    <another-demo-id>/
      ...
```

Future incremental objects use the same entity names with `incremental/`
instead of `initial/`. No incremental objects were uploaded. Future business
ingestion must use the four entity prefixes, and future RAG ingestion must use
only `documents/`. `_control/` is excluded from both consumer locations and
holds the manifests describing injected defects and source ingestion metadata.

## 3. Actual upload and remote verification

The dry-run explicitly selected **8 objects / 31,557,733 bytes**, comprising the
four initial source files, two control files, and two policy documents. No
incremental file or local credential was in the allowlist. The live upload used
the identical reviewed destination and mapping, without `--overwrite`.

Each upload used `If-None-Match: *`, and a preflight rejected existing names
before any writes. Each resulting object passed a HEAD check of size and stored
SHA-256 metadata. A subsequent paginated listing matched all eight planned
names/sizes and returned exactly eight objects under the run prefix, with no
additional names. Remote content was not downloaded and rehashed; SHA-256 HEAD
verification compares stored metadata to the local file checksum.

All rows below are relative to the verified source-data prefix:

| Object | Local and remote bytes | Result |
| --- | ---: | --- |
| `customers/initial/customers.csv` | 55,363 | Uploaded and verified |
| `orders/initial/orders.csv` | 5,397,982 | Uploaded and verified |
| `deliveries/initial/deliveries.csv` | 3,630,112 | Uploaded and verified |
| `events/initial/events.jsonl` | 22,469,866 | Uploaded and verified |
| `_control/initial/manifest.json` | 1,016 | Uploaded and verified |
| `_control/initial/ingestion_metadata.json` | 763 | Uploaded and verified |
| `documents/order_prioritization.md` | 1,289 | Uploaded and verified |
| `documents/incident_escalation.md` | 1,342 | Uploaded and verified |

The non-secret machine-readable receipt is
[object-storage-upload-result.json](object-storage-upload-result.json).
The common source location is:

`oci://CristinaVaras-bucket-AIDP@frgpslcqn7sm/demos/lakehouse-to-ai/source-data/v1/seed-42/reference-2026-10-28/`

## 4. Local configuration and exact commands

Local settings: `/private/tmp/aidp-lakehouse-storage/storage.local.json`.
Effective OCI config: `/private/tmp/aidp-lakehouse-storage/oci.local.config`.
Both files have permissions `0600` inside a `0700` directory outside Git.
The existing matching private key remains in its original local location.
The repository contains only the placeholder configuration example at
[config/storage.example.json](../config/storage.example.json), never the local
authentication configuration or secret material. OS temporary-file cleanup may
remove these local config files; preserve them in a private location outside
Git or recreate the external configuration before future operations.

There was no pre-existing project environment. The initially selected system
Python was 3.9.6, so the newly created environment was replaced with the installed
Python 3.14.7 interpreter. The project and optional OCI SDK 2.189.0 are installed
in `.venv`; `pip check` reported no broken dependencies. No shared/system
environment was modified.

Commands executed from the repository root:

```sh
/opt/homebrew/bin/python3 -m venv --clear .venv
.venv/bin/python -m pip install -e '.[oci]'
.venv/bin/python -m pip check
.venv/bin/python -m unittest discover -s tests -v

.venv/bin/python -m aidp_lakehouse upload \
  --data-dir /private/tmp/aidp-lakehouse-source-20261028 \
  --storage-config /private/tmp/aidp-lakehouse-storage/storage.local.json \
  --batch initial --dry-run

.venv/bin/python -m aidp_lakehouse upload \
  --data-dir /private/tmp/aidp-lakehouse-source-20261028 \
  --storage-config /private/tmp/aidp-lakehouse-storage/storage.local.json \
  --batch initial
```

The environment creation command replaced only the environment created in this
execution; do not use `--clear` to recreate a future populated environment.
The live upload is already complete. Rerunning it intentionally refuses the
existing objects. Use dry-run for review; do not bypass protection with
`--overwrite` for a routine rerun.

## 5. Validation and boundaries

All **15 tests passed**, including exact entity/control/document mapping,
initial/incremental isolation, external config validation and precedence,
default no-overwrite behavior, concurrent-creation protection and remote
listing verification. Existing generator reproducibility, relationships,
outcome consistency, injected defects and immutable acceptance features remain
covered. Generator code and SQL bootstrap scripts were preserved.

No configuration is missing for the completed initial upload. Incremental data
remains local. No bucket/resource creation, AIDP table creation, Autonomous
business-table creation/loading, model training or agent implementation was
performed.
