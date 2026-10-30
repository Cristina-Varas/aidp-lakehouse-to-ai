# Bronze v2 migration and canonical workflow

## 1. Evidence and scope

On **30 October 2026**, the user reported that the live AIDP
`lakehouse_demo.bronze.customers_lf_probe` passed: 600 rows in both registered
and direct reads, zero parser-corrupt rows, row_version exactly `'1'`, and
zero full-row `exceptAll` differences in both directions. This is **user-reported
runtime evidence**, not an agent-executed AIDP check. It validates the isolated
customers probe, not the other three registrations or a main-table migration.

No authenticated `ask-aidp` tool is available in this session. The agent has
**not inspected live definitions or executed the migration**. No alternative
AIDP CLI/SQLcl connection was used. Any agent inspection of AIDP must use
`ask-aidp` in **lakehouse_to_ai**. Preparation below performs the required live
inspection inside the user's authenticated notebook, before permitting changes.

Namespace **frgpslcqn7sm**, bucket **CristinaVaras-bucket-AIDP**, Frankfurt.
The tenancy name oraemeaaidata2 is not the namespace. Exact target root:

```text
oci://CristinaVaras-bucket-AIDP@frgpslcqn7sm/demos/lakehouse-to-ai/source-data/v2/seed-42/reference-2026-10-28/
```

The four independent locations are `customers/initial/`, `orders/initial/`,
`deliveries/initial/`, and `events/initial/`. Expected rows: **600 / 30,600 /
30,000 / 94,572**. No recursive root ingestion, incremental, documents or
_control registration is permitted. Source reference date stays 2026-10-28.
No Object Storage object is written, copied or deleted by this procedure;
both v1 and v2 remain intact. Canonical table names remain unchanged.

## 2. Install the wheel

Attach `dist/aidp_lakehouse_to_ai-0.2.2-py3-none-any.whl` as a Python library to
the existing AIDP Spark compute and restart its Python session. Repository
modules are packaged in this wheel; no driver-visible repository checkout or
monkeypatch is needed. Import `aidp_lakehouse` and confirm version `0.2.2`.
Select the existing **lakehouse_to_ai** workspace and intended compute in the UI;
the label in the cell is an operator assertion, not discovery of an OCI workspace
ID. Runtime identity checks require the same principal and Spark application
between preparation, application and restoration.

## 3. Prepare only: inspect and export restoration artifacts

Paste the complete [migration cell](../notebooks/migrations/migrate_bronze_v2.py)
into one Python cell. Leave `MODE = 'prepare'`. Execute this phase first.
It performs only read-only platform operations and creates a **new local
backup directory on the AIDP driver**, outside Git. It does not drop the probe
or change canonical tables.

Before producing the backup, preparation confirms Standard/INTERNAL catalog
identity, current principal/application, the exact qualified table names,
EXTERNAL type, CSV/JSON provider, complete raw schema and exact allowed initial
v1/v2 locations. It captures DESCRIBE metadata and actual `SHOW CREATE TABLE`
DDL for every canonical table and the probe if present, rechecks the live probe
against direct v2, and reads all four direct v2 sources for counts, corruption
and exact source-file provenance.

Inspect/download these files from the directory printed by preparation:

- `snapshot.json`: actual definitions, paths, schemas and runtime identity,
  with a consistency checksum.
- `restore_definitions.sql`: captured recreation DDL, retaining actual existing
  reader options (including any unsuccessful v1 workaround). It is an audit
  artifact, not a blind drop/recreate script.
- `restore.py`: guarded restoration entry point for that exact directory.

If `SHOW CREATE TABLE` is unavailable or returns an unexpected unqualified name,
preparation stops **before any platform change**. Obtain exact qualified DDL
through `ask-aidp` in lakehouse_to_ai and adapt/review export support; never
substitute assumed v1 DDL. SHOW CREATE may omit the EXTERNAL keyword; the export
adds it only after independently verifying the object is EXTERNAL.

DDL export does **not** promise to preserve ownership, grants, dependencies or
object IDs. Review/export those through the platform before applying. Recreated
objects get new identities; arrange reviewed regrants or dependent-object
handling when required. Do not proceed when that review is incomplete. Keep the
backup outside Git and retain a downloaded copy: driver storage may be ephemeral.
The generated directory is mode 0700 and initial files 0600. The snapshot can
contain platform properties; do not paste it into chat or commit it.

## 4. Apply later, explicitly

**Prepared only. Main-table application is not authorized in the current step.**
After reviewing/retaining the backup and permissions, a later operator execution
uses the same cell, sets `BACKUP_DIRECTORY` to the **literal exact existing path**
printed by preparation, `MODE = 'apply'`, and
`REVIEWED_BACKUP_AND_PERMISSIONS = True`. Do not leave the generated timestamp
expression when changing modes. Keep the same Spark application and principal.

All captured definitions are rechecked before the first DROP. The probe's
presence must also match the snapshot. Only the allowlisted EXTERNAL
`customers_lf_probe` metadata is removed first, freeing its v2 customers location.
Each canonical v1 EXTERNAL registration is then rechecked immediately before
metadata removal and recreated over its exact v2 entity/initial directory.
A canonical table already on compatible v2 is preserved and validated.
No managed-table DROP, PURGE, overwrite, data deletion or generic reset exists.
Unrelated Bronze objects are untouched.

Oracle documents that dropping an external table removes catalog metadata and
keeps underlying data; dropping a managed table deletes data. Consequently the
EXTERNAL checks are mandatory. See [Oracle tables](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/tables.html)
and [AIDP SQL grammar](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/sql-grammar.html).
There is no cross-table atomic transaction: stop on the first failure, inspect
the journal, and use reviewed restoration if needed. Do not blindly retry a
partially completed apply with the original plan.

## 5. Validation and restoration

After each registration, validation requires the exact schema, EXTERNAL
provider/location and reader properties. CSV uses explicit STRING columns,
header=true, inferSchema=false, enforceSchema=false, multiLine=false and
PERMISSIVE. Conflicting plain and option.* reader properties are rejected.
Both complete frames are cached before corruption checks. Counts, zero corrupt
rows, exact initial row_version (`'1'` for CSV, numeric 1 for events) and the
single exact source-file URI must agree.

All source columns plus `_corrupt_record` are compared with `exceptAll` in both
directions, retaining duplicate multiplicity. Spark disallows set operations on
MAP columns: for **comparison only**, events.payload becomes a sorted array of
all map entries. This preserves null versus empty maps, keys and values; no
source or registered field is trimmed, cast or rewritten. Original schemas are
checked before comparison. A failure stops execution; success produces
`verification.json`, and each completed metadata action is recorded in
`migration_journal.json`.

For manual restoration, run the generated `restore.py` in the original session,
or select `MODE = 'restore'` with the same exact backup directory and review
flag. It preflights every object, preserves unchanged captured definitions,
removes only compatible known v2 EXTERNAL metadata, recreates missing definitions
from the actual captured DDL, and restores the original probe after freeing its
location. Unexpected types, paths, schemas or changed identities stop restoration.
Restore/review permissions separately. Restoring v1 restores its known CRLF
behavior too; it is a metadata rollback, not a data repair.

## 6. Canonical notebook after migration, or for a fresh registration

[01_register_bronze.ipynb](../notebooks/01_register_bronze.ipynb) is the normal
entry point, run top to bottom in a fresh session with wheel 0.2.2. It has one
configuration cell selecting v2 and requires no variables or functions from
other notebooks. The reusable workflow performs runtime checks, catalog/schema
setup, exact control-document reads and validation, idempotent registration,
full direct-reader comparisons, small raw/provenance samples and a final
**BRONZE PASS**. Any stage failure prints **BRONZE FAIL** with its reason and
raises, preventing a successful summary. Restart from the top after fixing it.

The notebook never drops or replaces a table. It preflights existing canonical
registrations before creating any missing table. Any v1 location raises an
explicit instruction to use the separate migration and stops. Other incompatible
objects also stop; compatible v2 tables are preserved. It is safe to rerun.

[00_setup_catalog.ipynb](../notebooks/00_setup_catalog.ipynb) remains an optional
catalog-only administrator setup/check. Notebook 01 includes the same idempotent
Standard catalog and bronze/silver/gold schema setup; **00 is not a prerequisite**
and need not be run twice. Notebook 00 never registers source tables.

After BRONZE PASS, continue to [02_build_silver.ipynb](../notebooks/02_build_silver.ipynb)
with its matching v2 configuration and wheel 0.2.2. Its reusable Silver business
logic is unchanged. No Silver build was executed in AIDP by the agent.

## 7. Verification attribution

Local metadata orchestration tests use simulations; they do not validate AIDP
SQL execution. The opt-in comparison test uses real **local Spark 3.5.6** with
temporary CSV/JSON fixtures, including duplicated records, whitespace and MAP
payloads. It does not access cloud sources. The checked-in notebook has empty
outputs and null execution counts. Main-table migration and canonical notebook
runtime verification in AIDP remain **pending**.

Local commands executed from the repository root:

```sh
.venv/bin/python -m unittest discover -s tests -v
JAVA_HOME=/private/tmp/aidp-silver-jdk17/Contents/Home \
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_PYTHON=/private/tmp/aidp-silver-spark-env/bin/python \
AIDP_RUN_SPARK_TESTS=1 PYTHONPATH=src \
/private/tmp/aidp-silver-spark-env/bin/python -m unittest discover \
  -s tests -p test_bronze_validation_spark.py -v
```

The default suite ran **60 tests: 56 passed and 4 opt-in real Spark tests skipped**.
The new real local Spark comparison test was run separately and **passed** for
CSV and events JSON, duplicate multiplicity, MAP values including nulls,
whitespace fidelity, exact provenance, zero corrupt records and count mismatch
rejection. The three existing Silver/Delta opt-in tests were not rerun in this
change; their earlier execution is recorded in the LF milestone. Silver modules
and generator/Bronze runtime-fix modules are unchanged.

The complete canonical notebook was also executed top to bottom in a **fresh
simulated session** in a regression test, with no variables imported from other
notebooks. This verifies orchestration and PASS/FAIL gating, not AIDP operations.

Wheel build used the available offline setuptools backend:

```sh
PYTHONPATH=/opt/homebrew/Cellar/oci-cli/3.94.0/libexec/lib/python3.14/site-packages \
.venv/bin/python -c "from setuptools.build_meta import build_wheel; print(build_wheel('dist'))"
```

All Python files in wheel 0.2.2 were compared byte-for-byte with the repository
modules, and its METADATA version was verified. `dist/` remains Git-ignored.
The wheel is on the user's local Mac, at:

```text
/Users/cristinavarasmenadas/Library/CloudStorage/OneDrive-OracleCorporation/4. FY27/1. Customers/Workshops/14. Quistor/aidp-lakehouse-to-ai/dist/aidp_lakehouse_to_ai-0.2.2-py3-none-any.whl
```

Initial preparation found local `main` tracking `origin/main`, at baseline
`6d0ece4`, with the user's existing `.DS_Store` preserved. GitHub was initially
unreachable. Before publishing release 0.2.2, a fresh remote lookup confirmed
`refs/heads/main` still pointed to `6d0ece42802a8061cacddd17b8315cdafe91c7d6`.
Publication changes Git only; no AIDP operation is executed.
