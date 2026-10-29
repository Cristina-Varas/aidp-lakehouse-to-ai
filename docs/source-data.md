# Source-data contract and operations

## 1. Generate and validate

From the repository root, no installation or cloud credentials are required:

```sh
export PYTHONPATH="$PWD/src"
python3 -m aidp_lakehouse generate --seed 42 --reference-date 2026-10-28 \
  --historical-orders 30000 --open-orders 600 \
  --output-dir /private/tmp/aidp-lakehouse-source-20261028
python3 -m aidp_lakehouse validate \
  --data-dir /private/tmp/aidp-lakehouse-source-20261028
python3 -m unittest discover -s tests -v
```

The first command has already been executed for this milestone. It refuses an
existing output directory. Choose another directory for a new run. Generation
inside the repository is rejected; generated CSV/JSONL, environment files and
credentials are also ignored by Git. Python 3.11+ is required. The reference
date is explicit and never taken from the host clock. Same version, parameters
and Python random implementation produce byte-identical files, including
manifests: paths and wall-clock times are not serialized. Reproducibility was
verified with two independent full-size runs under Python 3.14.7.

Optional installed CLI: create an ignored local virtual environment, install
with `python3 -m pip install -e .`, then use `aidp generate`, `aidp validate`, and
`aidp upload` with the same arguments. Keep all reusable logic in the modules.

## 2. Layout and timestamps

```text
<output>/
  initial/       customers.csv, orders.csv, deliveries.csv, events.jsonl
                 manifest.json, ingestion_metadata.json
  incremental/   customers.csv, orders.csv, deliveries.csv, events.jsonl
                 manifest.json, ingestion_metadata.json
  documents/     order_prioritization.md, incident_escalation.md
  dataset_manifest.json
```

CSV uses a header, UTF-8 and comma delimiters; JSONL contains one event per line.
Stable source business keys are `C-...`, `O-...`, `D-O-...`, and `E-O-...-TYPE`.
Each entity has `source_created_at`, `source_updated_at` and `row_version`.
Source timestamps use ISO 8601 UTC with `Z`. These fields describe the source,
not actual ingestion. Separate ingestion metadata records the extraction cutoff,
source-system label, file counts and SHA-256 hashes; real ingestion fields will
be assigned by the future Bronze stage.

The initial cutoff is reference-date midnight UTC; historical orders were
accepted 30–730 days earlier and have observed outcomes. Open initial orders
were accepted in the preceding six days and have promises 2–8 days after the
cutoff. These selected cohorts are deliberate; there is a gap between historical
and open acceptance windows. The incremental cutoff is 30 days after the
reference date. New incremental orders are accepted on day +2; their completed
outcomes all occur before that cutoff. Open incremental orders are deliberately
right-censored and can be overdue at extraction; do not invent their outcomes.

## 3. Fields

| File | Business key | Fields beyond common source timestamps/version |
| --- | --- | --- |
| customers.csv | customer_id | customer_name, region, segment |
| orders.csv | order_id | customer_id, accepted_at, promised_delivery_at_acceptance, customer_segment_at_acceptance, customer_region_at_acceptance, priority_at_acceptance, inventory_fill_rate_at_acceptance, warehouse_load_at_acceptance, planned_carrier_at_acceptance, transit_distance_km, line_count, units, current_priority, current_status |
| deliveries.csv | delivery_id | order_id, dispatched_at, delivered_at, actual_carrier |
| events.jsonl | event_id | order_id, event_type, event_at, payload |

Acceptance features are explicitly allowlisted: `accepted_at`,
`promised_delivery_at_acceptance`, `customer_segment_at_acceptance`,
`customer_region_at_acceptance`, `priority_at_acceptance`,
`inventory_fill_rate_at_acceptance`, `warehouse_load_at_acceptance`,
`planned_carrier_at_acceptance`, `transit_distance_km`, `line_count`, and `units`.
Time-derived features may be derived from the acceptance timestamp and original
promise. No lateness probability, target label, or latent disruption value is
exported into orders. See [architecture](architecture.md) for exclusion rules.

Events are `ACCEPTED`, `DISPATCHED`, `DELIVERED`, and optional `INCIDENT_OPENED`.
Incident payloads contain a fictional exception type. Only the acceptance event
exists at prediction time. Later operational events belong to outcome/history
analysis; incident presence is intentionally outcome-associated and would leak
the target if used as an acceptance-time input.

## 4. Default counts and incremental contract

Verified with seed 42 and reference date 2026-10-28:

| File | Initial raw rows | Incremental raw rows | Incremental composition |
| --- | ---: | ---: | --- |
| customers.csv | 600 | 35 | 20 inserts, 10 segment updates, 5 exact duplicates |
| orders.csv | 30,600 | 1,160 | 1,000 inserts, 120 updates, 25 exact duplicates, 8 invalid dates, 7 unknown customers |
| deliveries.csv | 30,000 | 830 | 700 new-order completions, 120 existing-order completions, 10 exact duplicates |
| events.jsonl | 94,572 | 2,747 | 2,727 unique new lifecycle/incident events, 20 exact duplicates |

Initial orders comprise 30,000 historical deliveries and 600 open orders. After
the valid incremental merge, there are 620 customers, 31,600 orders, 30,820
completed deliveries and 780 open orders. The aggregate late count is 10,107
(32.79% of observed completed deliveries). Incident/event counts vary with seed.

For another historical count `H`, customer count is `max(20,min(600,H//10))`;
new incremental orders are `max(40,H//30)`; 70% (floor) have completed outcomes.
Updates complete `min(120,open_orders)` existing open orders, advancing version
and update time and setting current priority to HIGH. Original priority and all
other acceptance fields remain unchanged. Ten customer segment changes show why
later master records cannot be blindly joined into historical features.
Minimum supported inputs are 100 historical and 10 open orders.

Each batch's separate `manifest.json` records expected counts, checksums and
changes. The incremental manifest lists all new/updated customer/order keys and
every injected defect by file, business key, kind, expected action and 1-based
data-record number (CSV header excluded). There are exactly 60 duplicate rows
across files and 15 quarantined order rows. Invalid dates use February 30;
unknown references use `C-UNKNOWN`. Defective orders have distinct keys and no
downstream deliveries or events. Duplicates are byte-equivalent logical records,
not distinct business changes. Bronze retains them all; Silver will deduplicate
or quarantine according to the manifest contract. This milestone validates but
does not silently clean the raw extracts.

## 5. OCI upload

The source-data generation milestone originally had no storage destination.
On 29 October 2026, the existing local configuration for tenancy
`oraemeaaidata2` authenticated in `eu-frankfurt-1`, and namespace discovery
returned `frgpslcqn7sm`. The shared bucket `CristinaVaras-bucket-AIDP` was verified
in compartment `CristinaVaras`. The initial batch and policy documents have been
uploaded and verified. The default `~/.oci/config` belongs to another tenancy;
use the external project storage configuration shown below. See the
[storage execution record](object-storage-execution.md) for the verified target,
source locations and upload results.

The optional SDK is installed in the project's `.venv` (Python 3.14.7). Use that
environment rather than the system `python3`, which may resolve to Python 3.9.
For another compatible project environment, installation is:

```sh
.venv/bin/python -m pip install -e '.[oci]'
```

Configure these non-secret values with the real existing destination (the
variables below must be set locally; no bucket name is assumed):

```sh
export AIDP_OCI_NAMESPACE='<actual namespace>'
export AIDP_OCI_BUCKET='<existing shared AIDP bucket>'
export AIDP_OCI_PREFIX='demos/<demo-id>/source-data/<version>/<dataset-run>'
```

Alternatively use `--storage-config` with a non-secret JSON file outside Git;
the placeholder-only template is [config/storage.example.json](../config/storage.example.json).
The configured local file is `/private/tmp/aidp-lakehouse-storage/storage.local.json`.
It holds namespace, shared bucket, demo/run prefix, profile, authentication type
and explicit region; it contains no API keys or tokens. The directory is mode
0700 and file is mode 0600. It selects the external effective OCI config
`/private/tmp/aidp-lakehouse-storage/oci.local.config`, also mode 0600. That file
references the existing matching private key; it does not contain the private
key. Temporary storage may be cleaned by the OS; preserve
the configuration in a private location outside Git if needed.

Precedence is explicit flags, then environment variables, then the local JSON
file, then defaults. `AIDP_STORAGE_CONFIG` can select the file. The flags
`--namespace`, `--bucket`, `--prefix`, `--region`, `--profile` and `--auth` allow
explicit overrides. `OCI_CLI_REGION` can override the local region.
OCI credentials stay in your external OCI config/key files; use `--config-file`
and `--profile` to select them. API-key authentication is default;
`--auth security_token` supports a configured session-token profile. No OCI
credentials are accepted as CLI arguments or stored in the project.

```sh
# Offline: lists only initial files plus both policy documents (8 objects).
# Without storage settings it explicitly shows unresolved placeholders.
.venv/bin/python -m aidp_lakehouse upload \
  --data-dir /private/tmp/aidp-lakehouse-source-20261028 \
  --storage-config /private/tmp/aidp-lakehouse-storage/storage.local.json \
  --batch initial --dry-run

# Live command already executed successfully. A rerun refuses existing objects.
# It verifies the existing bucket, then uploads only initial + documents.
.venv/bin/python -m aidp_lakehouse upload \
  --data-dir /private/tmp/aidp-lakehouse-source-20261028 \
  --storage-config /private/tmp/aidp-lakehouse-storage/storage.local.json \
  --batch initial

# Incremental is selected separately (6 objects); it is not uploaded now.
.venv/bin/python -m aidp_lakehouse upload \
  --data-dir /private/tmp/aidp-lakehouse-source-20261028 \
  --storage-config /private/tmp/aidp-lakehouse-storage/storage.local.json \
  --batch incremental --dry-run
```

## 6. Shared-bucket mapping

The shared bucket is not dedicated to this project. Isolate each demo under
`demos/<demo-id>/`; isolate source-data versions and reproducible dataset runs
below that. This demo's configured prefix is
`demos/lakehouse-to-ai/source-data/v1/seed-42/reference-2026-10-28`.
Other demos use their own demo IDs without changing existing objects.

Object names under that prefix are:

```text
customers/initial/customers.csv
orders/initial/orders.csv
deliveries/initial/deliveries.csv
events/initial/events.jsonl
_control/initial/manifest.json
_control/initial/ingestion_metadata.json
documents/order_prioritization.md
documents/incident_escalation.md
```

Incremental selection uses `customers/incremental/`, `orders/incremental/`,
`deliveries/incremental/`, `events/incremental/`, and `_control/incremental/`.
It has not been authorized for live upload at this step. Future business-table
ingestion must select one entity prefix at a time, never the whole demo root.
Future RAG ingestion must select only `documents/`. Defect manifests and source
ingestion metadata live exclusively under `_control/` so neither consumer
accidentally ingests them as business rows or policy documents.

Initial selection always includes both
policies; incremental does so only with `--include-policies`. Uploads use an
explicit file allowlist, never a directory recursion or credential upload.
The root `dataset_manifest.json` is a local summary; each upload has its own
batch manifest and ingestion metadata.

Dry-run does not initialize the SDK, read credentials, call OCI, or verify cloud
access. Live upload first checks bucket access and all selected object names,
refuses any existing object by default, and uses conditional PUTs with
`If-None-Match: *` against concurrent creation. A precondition failure stops the
run without retrying as an unconditional overwrite. `--overwrite` explicitly
permits replacement; it was not used. No resources are created. Live success
requires a HEAD check of each uploaded object's length and stored SHA-256
metadata, plus a paginated listing that matches every planned name and size and
reports the object count under this demo/run prefix. This is not an independent
download/re-hash of remote content. Additional pre-existing objects under the
run prefix are reported separately, without modifying them. This execution
found exactly the eight planned objects and no additional objects.

Uploads are not a cross-object transaction: permission/network errors can leave
a partial batch. The manifest and ingestion metadata are uploaded after the
four data files. Use a fresh prefix after resolving a failed run, or review
existing objects before explicitly permitting replacement. Never interpret a
partial upload as a committed Bronze ingestion run.

The implementation uses Oracle's [ObjectStorageClient API](https://docs.oracle.com/en-us/iaas/tools/python/latest/api/object_storage/client/oci.object_storage.ObjectStorageClient.html).
