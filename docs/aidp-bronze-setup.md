# AIDP catalog setup and queryable raw Bronze

## 1. Status and scope

Prepared and locally validated on **29 October 2026**:

- [00_setup_catalog.ipynb](../notebooks/00_setup_catalog.ipynb)
- [01_register_bronze.ipynb](../notebooks/01_register_bronze.ipynb)
- Reusable orchestration: [aidp_bronze.py](../src/aidp_lakehouse/aidp_bronze.py)

**Live Bronze verification was reported by the user:** counts 600, 30,600,
30,000 and 94,572 matched the initial manifest, with zero parser-corrupt rows
and passing source provenance checks. See [the attributed execution record](aidp-bronze-execution.md).
The agent has not executed an authenticated AIDP session; local checks and
user-reported live observations are recorded separately. Live Silver execution
is still pending. SQLcl connects to Autonomous and is not used for AIDP Spark DDL.

No cloud catalog/schema/table statements have been executed by this work. After
you import and run the notebooks, notebook 00 creates absent catalog/schema
metadata; notebook 01 creates absent external-table metadata over existing files.
Neither notebook writes business data, copies source objects, cleans records or
implements Silver, ML, agents or Autonomous publication. Silver and Gold receive
only their empty schema containers.

## 2. Verified source environment

These non-secret values come from the existing local storage configuration,
[upload execution record](object-storage-execution.md), and upload receipt.
The source-data reference date remains **2026-10-28**.

| Value | Setting |
| --- | --- |
| Tenancy | `oraemeaaidata2` |
| Object Storage namespace | `frgpslcqn7sm` — not the tenancy name |
| Region | Frankfurt / `eu-frankfurt-1` |
| Source compartment | `CristinaVaras` |
| Shared source bucket | `CristinaVaras-bucket-AIDP` |
| Source prefix | `demos/lakehouse-to-ai/source-data/v1/seed-42/reference-2026-10-28` |
| Standard catalog | `lakehouse_demo` |
| Schemas | `bronze`, `silver`, `gold` |
| Source batch | `initial` |

All table locations use this root URI:

`oci://CristinaVaras-bucket-AIDP@frgpslcqn7sm/demos/lakehouse-to-ai/source-data/v1/seed-42/reference-2026-10-28/`

| External table | Location relative to root | Only expected source object | Expected raw rows |
| --- | --- | --- | ---: |
| `lakehouse_demo.bronze.customers` | `customers/initial/` | `customers.csv` | 600 |
| `lakehouse_demo.bronze.orders` | `orders/initial/` | `orders.csv` | 30,600 |
| `lakehouse_demo.bronze.deliveries` | `deliveries/initial/` | `deliveries.csv` | 30,000 |
| `lakehouse_demo.bronze.events` | `events/initial/` | `events.jsonl` | 94,572 |

The notebook reads `_control/initial/manifest.json` and
`_control/initial/ingestion_metadata.json` as exact control objects for assertions.
It never registers them as tables or recursively reads the source root. Counts
come from those uploaded documents, not from a typed-in substitute. Documents,
incremental paths and control paths are outside all four external locations.

## 3. Required AIDP workspace, compute and access

Choose an existing AIDP instance in the intended tenancy, preferably in Frankfurt
with the source bucket. Select a workspace where you can import/edit/run files,
and attach a running AIDP Spark compute cluster. Use a runtime with Python 3.11+
to install this project's wheel. The notebook uses the AIDP-supplied `spark`
session; it never constructs a local Spark session or an Autonomous connection.
Workspace/compute names and OCIDs are not invented or embedded in the notebooks.
Notebook import and compute attachment are described in Oracle's
[notebook guide](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/notebooks.html).

Two independent permission layers must be ready:

1. **AIDP permissions:** workspace file import/edit/run rights, compute attachment
   rights, Master Catalog `CREATE_CATALOG`, catalog `CREATE_SCHEMA`, Bronze schema
   `CREATE_TABLE`, and query/inspection access (`SELECT`) on the resulting objects.
   Owners/admins may already have these actions. Existing objects require their
   corresponding permissions; `IF NOT EXISTS` does not bypass access controls.
   Have the AIDP administrator check the
   [permissions model](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/permissions-model.html).
2. **OCI access:** human catalog operations need bucket discovery access; the
   AIDP execution/service principal must also be allowed to list/read the four
   source directories and the two control objects in compartment `CristinaVaras`.
   Follow Oracle's [AIDP IAM policy guidance](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/iam-policies-oracle-ai-data-platform.html),
   including its external-compartment section if the instance and source bucket
   are in different compartments. Its service policies can require bucket
   inspection/read/update and governance permissions beyond a human's object
   read permission. Do not replace the documented principal conditions with an
   unrestricted blanket grant. This milestone does not change IAM policies.

The principal that uploaded the files is not necessarily the principal that
executes Spark. Keep API keys, session tokens, wallets, passwords and the local
OCI config outside the notebooks and repository. AIDP must supply its supported
Object Storage access; do not paste uploader credentials into Spark settings.
For a workspace in another tenancy, apply the documented cross-tenancy setup
before running; same-tenancy bucket access alone is not sufficient.

## 4. External-location limitations — check before DDL

Oracle's [table documentation](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/tables.html)
supports CSV and JSON external sources but restricts locations already registered
to external tables/volumes and locations beneath volumes. Check Master Catalog
for claims on the bucket root and all ancestors of the four initial directories,
including registrations belonging to other demos or AIDP instances. The supplied
locations are disjoint; the notebooks cannot prove global ownership visibility
for registrations they cannot inspect.

Do not create a volume over the shared bucket, the demo root, an entity root, or
an initial directory before registration. Do not register the entire shared
bucket as a table. An existing same-name table with a different provider, schema,
external type, location or critical parser settings causes an error and remains
unchanged. AIDP can also reject a new location due to a claim outside this schema.

If AIDP reports overlap, bucket-governance restrictions, permissions, unsupported
reader options or JSON-map limitations in your deployed runtime, stop and retain
the error. Do not drop an existing registration, convert to managed tables,
register a broad ancestor, create a replacement bucket, or copy/overwrite files
as a workaround. This work introduces **no storage-layout change**. Any required
alternative needs investigation of that actual runtime limitation first.

## 5. Import notebooks and provide reusable modules

Download the two `.ipynb` files from this repository and use **Upload File** in
the target workspace/folder. Alternatively, open them in a workspace checkout
where Git access has already been configured. No Git connection or workspace is
created automatically by these artifacts. Attach the intended running compute.

Importing `.ipynb` files alone does not install `aidp_lakehouse`. Build the base
wheel locally, without generated data or credentials:

```sh
.venv/bin/python -m pip wheel . --no-deps --wheel-dir dist/
```

Upload `dist/aidp_lakehouse_to_ai-0.2.0-py3-none-any.whl` to a workspace location
accessible from the notebook driver. Install it in the attached compute's Python
environment using the workspace's supported library mechanism or an initial
Python installation cell, with your actual workspace path:

```python
%pip install --no-deps --force-reinstall /Workspace/<folder>/aidp_lakehouse_to_ai-0.2.0-py3-none-any.whl
```

The base package has no third-party runtime dependencies. AIDP supplies Spark;
do not install a competing local PySpark package. Neither the optional local OCI
SDK nor `nbformat` is required by these AIDP notebook cells. Restart the Python
session after installation if required by the platform, then run notebook cells
from the start. The wheel is reusable across both notebooks; do not duplicate
the implementation into notebook cells. Oracle covers wheel use in the
[notebook guide](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/notebooks.html).

As an alternative, make a source checkout available to the driver, set
`REPO_ROOT` in each configuration section to its absolute path, and the bootstrap
cell adds `<REPO_ROOT>/src` to `sys.path`. This code runs on the driver and uses
built-in Spark expressions, not Python worker UDFs. Importing just a notebook
without the package or checkout will fail clearly rather than install code
implicitly. Do not assume the local Mac paths exist in AIDP.

## 6. Configuration and execution order

Both notebooks have one labeled configuration cell containing identical
non-secret `CONFIG` values and the optional `REPO_ROOT`. Keep these sections in
sync. The seed is 42, batch is `initial`, and reference date is fixed to
2026-10-28; the module rejects a different batch/date for this milestone.

1. Run **00_setup_catalog** top to bottom. It previews the intended DDL, creates
   the standard catalog if absent, verifies its type, and creates the three
   schemas if absent. Review the `DESCRIBE CATALOG`, `SHOW SCHEMAS`, and schema
   descriptions. If the catalog name already refers to an external catalog,
   execution stops before schema creation. No existing child is removed.
2. Run **01_register_bronze** top to bottom in the same intended workspace and
   compute environment. It reads and validates the two exact control objects,
   prints each entity's external DDL/location, inspects existing target tables,
   and registers absent tables. Review the returned action list: `REGISTERED`
   for new metadata, `PRESERVED` for matching existing metadata.
3. Review registered definitions and counts. `DESCRIBE TABLE EXTENDED` exposes
   schema, external type, provider, location and reader properties. The helper
   checks those details, compares raw counts to the manifest, and asserts that
   each table reads exactly its one expected object using `input_file_name()`.
4. Review five raw rows and five provenance rows per entity. The notebook shows
   the source timestamps and version plus explicitly derived batch/file metadata.

The standard-catalog command is AIDP's `CREATE CATALOG IF NOT EXISTS`, not Spark's
`CREATE DATABASE` or a replacement session catalog. The current Oracle grammar
page illustrates an OPTIONS clause but does not identify required connection
options for a standard catalog. Oracle's
[current AIDP sample](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/transformation/liquid-clustering/financial_services_delta_liquid_clustering_demo.ipynb)
uses the minimal no-OPTIONS standard-catalog form selected here. Do not invent
connection/compartment options; let the configured AIDP instance resolve standard
catalog placement. Its managed storage must not be placed over the source paths.
Table DDL combines the external-table form documented in
[Master Catalog guidance](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/manage-master-catalog.html)
with provider/options syntax from [AIDP SQL Grammar](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/sql-grammar.html).

Creation is not a cross-object transaction. On a partial failure, inspect the
objects already created and rerun; matching objects remain unchanged. If your
runtime's DESCRIBE metadata labels/options differ from the documented Spark-style
details, the guards stop for inspection. They do not silently accept an
unverified existing definition or recreate it.

## 7. Raw schemas, parser behavior and provenance

The [source-data contract](source-data.md) lists every source column. The module
uses the inspected CSV headers in their exact order. All CSV fields are
`STRING`, including numeric-looking fields, `accepted_at`, the original promise,
dispatch/delivery dates, source timestamps and row version. Inference is off.
There is no date cast, cleanup, uniqueness constraint, row filter or join.

CSV options explicitly declare a UTF-8 header, comma delimiter, double-quote
quoting and doubled-quote escaping, single-line records, no whitespace trimming,
and non-recursive file lookup. Source CSV records use CRLF; the reader's normal
line-ending detection handles them without a forced conflicting separator.
`enforceSchema=false` validates the header rather than silently imposing columns
on a differently ordered header. The reader retains parser failures in an
additional `_corrupt_record STRING` column using `PERMISSIVE` mode. No source
object is changed to add that column.

Events are UTF-8 JSON Lines: one complete object per line, not a JSON array.
`multiLine=false` is explicit. Columns are `event_at`, `event_id`, `event_type`,
`order_id`, `payload`, `row_version`, `source_created_at`, and `source_updated_at`,
plus `_corrupt_record`. Timestamps/keys/types are strings, `row_version` is
`BIGINT` as generated, and `payload` is `MAP<STRING,STRING>`: either `{}` or a map
with an `exception_type` string (`CAPACITY`, `STOCK`, or `TRANSPORT`). No payload
flattening or inferred empty struct can discard incident details.

All synthetic timestamps retain their original ISO-8601 UTC text. Deliberate
February-30 values exist in the **unregistered incremental** files, not the
initial batch; raw string typing ensures they are not coerced to null if a later
milestone explicitly registers them. No records are cleaned or quarantined here.
Parser-corruption checks stop verification if unexpected syntax problems occur,
while keeping the external raw rows available for inspection.

Source records contain `source_created_at`, `source_updated_at`, and
`row_version`, but no `batch_id`. The sample SELECT labels batch as `initial`
from the configured path/control metadata, reports the fixed reference date,
uses extraction time from the manifest, and adds `input_file_name()`. These are
query-result fields only; extraction time is not claimed to be an actual Bronze
ingestion timestamp. Source timestamp ranges are lexical ISO-text summaries.

Reader-option details follow the supported Spark 3.5-family providers:
[CSV options](https://spark.apache.org/docs/3.5.6/sql-data-sources-csv.html) and
[JSON options](https://spark.apache.org/docs/3.5.6/sql-data-sources-json.html).
AIDP's own catalog DDL and access controls are still required.

## 8. Local validation and runtime handoff

Executed locally:

```sh
.venv/bin/python -m pip install -e '.[notebooks]'
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m pip wheel . --no-deps --wheel-dir /private/tmp/aidp-lakehouse-wheel
```

**22 tests passed**, including both notebooks' `nbformat` validation, compilation
of every Python code cell, empty execution outputs, identical configuration,
literal initial-only locations, raw schema/header compatibility, JSON payload
shape, manifest checks, rerun preservation and conflicting-definition guards.
Existing source generation, defect, relationship, outcome and uploader tests
continue to pass. Fake AIDP objects exercise orchestration guards only; they do
not parse AIDP SQL or prove deployed provider behavior. No local Spark/AIDP query
execution is claimed.

The original 0.1.0 validation checked the complete existing 30,000-history source run against the raw
column contract: all three CSV headers/counts and all 94,572 JSON event shapes
match the definitions. The base wheel was built successfully at
`/private/tmp/aidp-lakehouse-wheel/aidp_lakehouse_to_ai-0.1.0-py3-none-any.whl`;
its helper bytes match the source module and it contains no source datasets or
credentials. Wheel SHA-256:
`57d4f8138d7c3373684b03edc4775fa193f692532052f24ebd84d8797a65dffe`.

The original handoff asked the user to run 00 then 01 and retain the actual
definitions, locations, counts and samples. That Bronze run has now been
reported successful by the user. Checked-in notebooks still have no fabricated
outputs or execution counts. Continue with [initial Silver](aidp-silver-setup.md),
whose live AIDP verification remains pending.

## 9. Version 0.1.1 — observed INTERNAL catalog metadata

The user supplied actual `DESCRIBE CATALOG lakehouse_demo` output with columns
`attribute` and `attribute_value`. The top-level `type` was `INTERNAL`; the
`catalog_details` JSON independently reported `catalogType: INTERNAL`. Version
0.1.1 recognizes this observed representation as Standard without removing
catalog validation. Attribute labels and type values are normalized for case,
whitespace, underscores, hyphens and camel-case names. Named dictionary/PySpark
rows and the previously supported positional metadata rows are accepted.

Every supplied type must be a recognized Standard representation (`INTERNAL`,
`Standard Catalog`, `Standard`, or `Catalog` after normalization). Top-level
type fields and JSON catalog details are checked together; repeated attributes
and duplicated JSON type keys cannot hide a conflict. External, unknown, empty,
missing or conflicting types cause a stop before schema creation. Invalid or
type-less catalog-details JSON is also rejected. Existing table validation still
uses the shared metadata reader and retains its original safety checks.

Local regression tests use the exact reported column/row format and simulate two
setup runs over an existing INTERNAL catalog and existing Bronze/Silver/Gold
schemas. Their metadata remains unchanged; all creation statements still use
`IF NOT EXISTS`. These are local simulations, not live AIDP verification.

Build and validate the new release with:

```sh
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m pip wheel . --no-deps --wheel-dir dist/
```

Use the **0.1.1** wheel rather than the older 0.1.0 file, force-reinstall it in
the attached AIDP environment, restart the Python session if required, and rerun
notebook 00 from the beginning. `dist/` remains Git-ignored. The observed output
was provided by the user; no live AIDP execution is claimed for this fix.

The full suite passed **26 tests**. The built 0.1.1 wheel was also imported
directly in an isolated Python process: its version and helper bytes matched
the release source, the observed INTERNAL rows passed, two simulated setup runs
used only guarded creates, and conflicting types stopped before schema creation.
Wheel SHA-256:
`88d64f2f39ed7aac8261dc1e1dd44cedf1dd8161cc1fc65bf3e790f7e20563a6`.

## 10. Version 0.1.2 — whole-text controls and table reader properties

This release consolidates two user-observed AIDP runtime compatibility fixes:

- Read each exact control JSON object with
  `spark.read.text(uri, wholetext=True)`, passing the Boolean keyword directly
  rather than using `.option("wholetext", "true")`. Control identity, document
  count, extraction time, manifest counts and checksum cross-checks are unchanged.
- Inspect reader options exposed under either `Table Properties` or
  `Storage Properties`, including both `mode` and `option.mode` representations.
  Option keys and the case-insensitive Boolean/enum values are normalized.
  Every occurrence is checked across both sections and duplicate metadata rows;
  a matching value cannot hide a disagreement under an alias or a later value.

The required settings remain `mode=PERMISSIVE`, `multiLine=false`, and, for CSV,
`header=true` and `inferSchema=false`. Missing, wrong or conflicting values still
stop validation. Conflicting values of other reported options are also rejected;
case-sensitive reader values such as delimiters/column names are not folded.
External table type, format/provider, exact location and raw schema validation
remain in place, as does the INTERNAL/Standard catalog guard from 0.1.1.

Regression tests cover multiline control JSON with the direct wholetext API,
the observed table-property representation, Storage Properties compatibility,
prefixed/plain aliases, duplicate/conflicting values, preservation on rerun,
missing required settings, and rejection of changed type/provider/location/schema.
These use local test doubles and the existing source-data tests; no live AIDP
execution is claimed. Install the latest wheel from section 5 and restart the
AIDP Python session before rerunning the notebooks.

The complete local suite passed **30 tests**. Six targeted regressions also
passed in an isolated process importing the helper directly from the built
0.1.2 wheel. Packaged helper bytes match the repository source, and wheel
metadata reports version 0.1.2. Artifact SHA-256:
`712e2a3b17e7125bedcd75b941c3aac493996a8ab10bb178e63cfda18521be45`.
