# Source-data milestone execution — 28 October 2026

This is the historical source-generation record. For the subsequent configured
shared bucket and verified initial upload on 29 October 2026, see
[object-storage-execution.md](object-storage-execution.md).

## 1. Repository inspection and preservation

Inspected repository files, ancestor locations for `AGENTS.md`, and all existing
documentation before implementation. No applicable `AGENTS.md` was found.
Existing implementation consisted of the completed Autonomous bootstrap. Its SQL
scripts and execution records were preserved. No database calls were made in
this milestone. Code and fictional policy documents use English.

## 2. Actual generation

Executed from the repository with `PYTHONPATH=src` under Python 3.14.7:

```sh
python3 -m aidp_lakehouse generate --seed 42 --reference-date 2026-10-28 \
  --historical-orders 30000 --open-orders 600 \
  --output-dir /private/tmp/aidp-lakehouse-source-20261028
python3 -m aidp_lakehouse generate --seed 42 --reference-date 2026-10-28 \
  --historical-orders 30000 --open-orders 600 \
  --output-dir /private/tmp/aidp-lakehouse-source-20261028-repro
```

Generation automatically ran validation on each result. A recursive SHA-256
comparison found **all 15 output files byte-identical**, including manifests,
ingestion metadata and both policy documents. No paths or wall-clock times are
embedded in the reproducible files.

Initial: 600 customer rows, 30,600 orders (30,000 delivered plus 600 open),
30,000 delivery rows, and 94,572 event rows.

Incremental: 35 customer rows, 1,160 order rows, 830 delivery rows and 2,747
event rows. Valid changes add 20 customers and 1,000 orders, update 10 customers
and 120 orders, and add 820 delivery outcomes. There are exactly 60 duplicate
rows across the four files, 8 invalid-date orders and 7 unknown-customer orders.
Every defect is recorded in the incremental batch manifest separately from
source records. The first batch has no injected defects.

After merging valid versions: **31,600 orders; 30,820 completed; 780 open**.
There are 10,107 late outcomes (32.79% of completed deliveries). Priority cohorts
show overlapping outcomes:

| Original priority | On time | Late |
| --- | ---: | ---: |
| HIGH | 5,037 | 2,520 |
| NORMAL | 13,435 | 6,717 |
| URGENT | 2,241 | 870 |

Validation checks checksums/counts against both metadata records; independently
detects all documented defects; checks valid keys/FKs, source/extraction times,
one delivery per completed order, lifecycle events and chronology; checks exact
insert/update sets and versions; and confirms acceptance snapshots do not change.
Open orders have no completed outcome; they are never assigned a negative label.

## 3. Tests and upload plans

```sh
python3 -m unittest discover -s tests -v
python3 -m aidp_lakehouse upload \
  --data-dir /private/tmp/aidp-lakehouse-source-20261028 --batch initial --dry-run
python3 -m aidp_lakehouse upload \
  --data-dir /private/tmp/aidp-lakehouse-source-20261028 --batch incremental --dry-run
```

**13 tests passed.** Coverage includes deterministic bytes, different seed/date
sensitivity, refusal to overwrite local output or generate inside the repo,
relationship/outcome errors even with refreshed checksums, acceptance-feature
mutation, undocumented defects, batch isolation, SDK-free dry-run, missing
storage configuration, conditional PUT, remote metadata checks, refusal of
existing objects before writes and a simulated concurrent-creation HTTP 412.
Cloud contract tests use an in-memory fake client, not OCI.

Actual dry-runs listed 8 initial objects (four data files, batch manifest,
ingestion metadata, two policies) and 6 incremental objects. Both reported
`storage_configured: false`, `cloud_verified: false`, and `overwrite: false`.
Conditional-write usage was also checked against the locally installed Oracle
OCI CLI implementation, which uses `if_none_match='*'` for absent objects.

## 4. Remaining environment configuration

The OCI CLI and an external `~/.oci/config` with a DEFAULT profile exist. The
optional OCI SDK is not importable in the project Python environment. The
repository, targeted environment variables and OCI config/CLI defaults contained
no configured demo namespace, bucket or prefix. Credential values were not
printed. No destination was guessed. No live cloud request, bucket verification,
upload, resource creation, database table creation/loading, model training or
agent implementation was performed.

Before the initial upload, supply the real existing destination via
`AIDP_OCI_NAMESPACE`, `AIDP_OCI_BUCKET`, and `AIDP_OCI_PREFIX` (or equivalent CLI
flags), install the optional OCI SDK in a local environment, and select the
appropriate external authentication profile. Then run the documented initial
upload command; it verifies access first. Leave incremental upload for a later
explicit step. Files in `/private/tmp` may be cleaned by the operating system;
the deterministic generator can recreate this dataset in a new output directory.
