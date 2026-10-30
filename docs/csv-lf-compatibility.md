# CSV LF compatibility — v2 source probe

## 1. User-observed AIDP behavior

The user reported that original CSVs use CRLF, a direct reader with
`multiLine=true` returns `row_version="1"`, but registered external CSV tables
retain `row_version="1\r"`. Recreating customers with multiLine=true did not
resolve it. enforceSchema=true allowed reading, but full comparison still found
differences: **that workaround is not validated** and is not used here.

These are user-supplied runtime observations. Earlier matching counts, zero
corrupt rows and correct provenance did not prove field-level fidelity. Do not
trim business fields or weaken validation to make comparisons pass.

This milestone prepares LF sources and an isolated probe. **No main Bronze table
was migrated, replaced, dropped or altered.** Main notebook configuration remains
on v1; Silver transformation modules and all previous Bronze runtime fixes are
unchanged. Live AIDP LF-probe verification remains pending.

## 2. Local generation and exact comparisons

Package **0.2.1** makes the CSV writer explicitly use `lineterminator="\n"`.
It preserves all fields, column order, quoting, random calls, business rules,
outcomes, seed, reference date and injected defects. Generator business version
stays `1` because this is a transport-only change. Notebook 02's version guard
also accepts 0.2.1; its v1 configuration and Silver logic are not migrated.

Original dataset, preserved:
`/private/tmp/aidp-lakehouse-source-20261028/`

New dataset outside Git:
`/private/tmp/aidp-lakehouse-source-v2-lf-20261028/`

Independent repeat:
`/private/tmp/aidp-lakehouse-source-v2-lf-20261028-repro/`

Commands executed from the repository root:

```sh
.venv/bin/python -m aidp_lakehouse generate --seed 42 --reference-date 2026-10-28 \
  --historical-orders 30000 --open-orders 600 \
  --output-dir /private/tmp/aidp-lakehouse-source-v2-lf-20261028
.venv/bin/python -m aidp_lakehouse generate --seed 42 --reference-date 2026-10-28 \
  --historical-orders 30000 --open-orders 600 \
  --output-dir /private/tmp/aidp-lakehouse-source-v2-lf-20261028-repro
```

Both runs passed existing validation; all **15 files are byte-identical** between
the new runs. `aidp_lakehouse.csv_compat.compare_csv_terminators` checks every
original/new CSV using both ordered csv.reader equality and complete-byte
equality after CRLF-to-LF replacement. It preserves headers, duplicates, order
and field whitespace without trimming, sorting or deduplicating. A quoted
embedded CRLF cannot be silently normalized: regression tests reject that change.

| CSV | Data rows | LF terminators including header | New CR bytes |
| --- | ---: | ---: | ---: |
| initial/customers.csv | 600 | 601 | 0 |
| initial/orders.csv | 30,600 | 30,601 | 0 |
| initial/deliveries.csv | 30,000 | 30,001 | 0 |
| incremental/customers.csv | 35 | 36 | 0 |
| incremental/orders.csv | 1,160 | 1,161 | 0 |
| incremental/deliveries.csv | 830 | 831 | 0 |

All six CSVs preserve parsed records exactly and differ only in terminators.
Both JSONL event files, both policies and dataset_manifest.json are byte-identical
to v1. Batch manifests/ingestion metadata were regenerated with the new CSV
checksums; all other control contents, including the entire defect ledger, match.
Incremental was regenerated/compared locally for generator validation; it was
**not uploaded or processed through Silver**.

See [csv-lf-comparison.json](csv-lf-comparison.json) for full comparison evidence.

## 3. Actual OCI upload and preservation

Used the verified external OCI configuration for tenancy `oraemeaaidata2`,
Frankfurt (`eu-frankfurt-1`), namespace `frgpslcqn7sm`, compartment `CristinaVaras`,
shared bucket `CristinaVaras-bucket-AIDP`. The existing local configuration still
selects v1; the explicit command prefix override selects v2 only for this upload.

```sh
.venv/bin/python -m aidp_lakehouse upload \
  --data-dir /private/tmp/aidp-lakehouse-source-v2-lf-20261028 \
  --storage-config /private/tmp/aidp-lakehouse-storage/storage.local.json \
  --prefix demos/lakehouse-to-ai/source-data/v2/seed-42/reference-2026-10-28 \
  --batch initial --dry-run

.venv/bin/python -m aidp_lakehouse upload \
  --data-dir /private/tmp/aidp-lakehouse-source-v2-lf-20261028 \
  --storage-config /private/tmp/aidp-lakehouse-storage/storage.local.json \
  --prefix demos/lakehouse-to-ai/source-data/v2/seed-42/reference-2026-10-28 \
  --batch initial
```

The reviewed plan and actual upload contain **8 objects / 31,496,530 bytes**.
No-overwrite protection remained enabled, including conditional If-None-Match.
Relative locations beneath the v2 prefix are:

| Object | Bytes |
| --- | ---: |
| customers/initial/customers.csv | 54,762 |
| orders/initial/orders.csv | 5,367,381 |
| deliveries/initial/deliveries.csv | 3,600,111 |
| events/initial/events.jsonl | 22,469,866 |
| _control/initial/manifest.json | 1,016 |
| _control/initial/ingestion_metadata.json | 763 |
| documents/order_prioritization.md | 1,289 |
| documents/incident_escalation.md | 1,342 |

Remote names, count, sizes and stored checksum metadata matched. All eight objects
were then downloaded independently: actual SHA-256 hashes match local files and
downloaded CSVs have no CR bytes. Exactly eight objects exist under this v2 run
prefix, with no incremental objects.

All eight v1 objects were listed before and after: names, sizes, ETags and
modification times are unchanged. No v1 write, source deletion or bucket creation
occurred. Control manifests remain outside entity and policy ingestion paths.
See [csv-lf-oci-verification.json](csv-lf-oci-verification.json).

## 4. Standalone AIDP probe cell

Paste the complete [customers_lf_probe.py](../notebooks/probes/customers_lf_probe.py)
file into **one Python cell** in an authenticated AIDP notebook with attached
Spark compute. It is self-contained and needs no project wheel. Use existing
catalog/schema permissions and access to the v2 source directory.

The only registration is `lakehouse_demo.bronze.customers_lf_probe` over:

`oci://CristinaVaras-bucket-AIDP@frgpslcqn7sm/demos/lakehouse-to-ai/source-data/v2/seed-42/reference-2026-10-28/customers/initial/`

It explicitly uses STRING for all seven source fields and the extra corrupt
record field. Both direct and registered readers use header=true,
enforceSchema=false, multiLine=false, inferSchema=false and PERMISSIVE mode.
Existing probe type/provider/schema/location and option values must match;
otherwise execution stops without modifying that definition. There is no DROP,
ALTER, REPLACE, source write, or main-table registration in this cell.

All criteria must pass:

- Direct and registered counts are both **600**.
- Both have zero parser-corrupt rows and read only the expected customers.csv.
- Every row_version is exactly `"1"`, without trimming; samples use
  `repr(row_version)` to reveal hidden carriage returns.
- Full-row `exceptAll` comparisons in both directions return zero, over all
  source columns plus the corrupt-record field, preserving duplicate multiplicity.

The cell caches full frames before corrupt-field inspection and unpersists them
afterward. A readable table or count-only result is not success. Reader forms
follow [Oracle AIDP SQL grammar](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/sql-grammar.html)
and [CSV provider options](https://spark.apache.org/docs/3.5.6/sql-data-sources-csv.html),
but documentation/local testing does not establish success in the observed AIDP
runtime. **Live AIDP LF probe: pending.** Do not migrate main Bronze or run a v2
Silver build until this probe succeeds and a separate migration is authorized.

## 5. Local testing and artifact

Default tests cover the generator, validation, reproducibility, LF assertions,
ordered/whitespace-preserving comparisons and previous Bronze/Silver guards.
Opt-in **real local Spark** testing executes the complete standalone probe on a
separate 600-row LF fixture, adapting only local environment constants, then
reruns existing managed Silver/Delta checks. The fixture includes a quoted comma
and leading/trailing field spaces. Local native Spark normalizes file URIs
differently for DESCRIBE and provenance; explicit fixture URIs handle this without
weakening the real OCI probe. Local Spark success is **not** AIDP verification.

```sh
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m pip wheel . --no-deps --wheel-dir dist/
```

The new wheel is `dist/aidp_lakehouse_to_ai-0.2.1-py3-none-any.whl` and retains
the unchanged Bronze/Silver modules. Generated data, credentials and `dist/`
remain outside Git tracking.

Final results: default discovery ran 48 tests (**45 passed, 3 opt-in Spark tests
skipped**). The three real local Spark tests were executed separately and all
passed, including the complete probe fixture and existing Silver/Delta rerun
checks. Three CSV compatibility tests also passed while importing directly from
the built wheel in an isolated process. All packaged Python modules match source.

The isolated pip build stalled fetching its backend; only the stalled tasks in
this project directory were stopped. The final wheel was built offline using
the existing local setuptools 83.0.0 backend, without modifying the shared CLI
environment:

```sh
PYTHONPATH=/opt/homebrew/Cellar/oci-cli/3.94.0/libexec/lib/python3.14/site-packages \
  .venv/bin/python -c "from setuptools.build_meta import build_wheel; print(build_wheel('dist'))"
```

The verified artifact is on the user's local Mac inside this repository's
`dist/aidp_lakehouse_to_ai-0.2.1-py3-none-any.whl`. SHA-256:
`85e798ab410882ed3dd34afaf28e5a2a9fcee5e7fbf4c4793dd7d882b021c361`.
Its build and local tests do not change the pending AIDP probe status.
