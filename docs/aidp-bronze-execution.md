# Bronze execution observations

## User-reported live verification

Recorded on 29 October 2026 from the user's report in this conversation. The
exact AIDP execution timestamp, workspace/compute identifiers and raw job logs
were not supplied. The agent did not run these queries or independently inspect
an AIDP job. Preserve that attribution when using these results.

| Live AIDP table | User-reported rows | Parser-corrupt rows | Manifest comparison |
| --- | ---: | ---: | --- |
| lakehouse_demo.bronze.customers | 600 | 0 | Match |
| lakehouse_demo.bronze.orders | 30,600 | 0 | Match |
| lakehouse_demo.bronze.deliveries | 30,000 | 0 | Match |
| lakehouse_demo.bronze.events | 94,572 | 0 | Match |

The user also reported that exact source-file provenance checks passed. The
sources are the uploaded initial locations in
[object-storage-execution.md](object-storage-execution.md), under namespace
`frgpslcqn7sm`, shared bucket `CristinaVaras-bucket-AIDP`, and the fixed
`demos/lakehouse-to-ai/source-data/v1/seed-42/reference-2026-10-28/` prefix.
No incremental data is included in this observation.

The user subsequently reported a CRLF field-fidelity issue: registered CSV
row_version values retained `\r` despite those passing counts/provenance checks.
Those checks therefore do not establish full-row equivalence. Neither
multiLine=true nor enforceSchema=true is a validated workaround. The
[isolated v2 LF probe](csv-lf-compatibility.md) is prepared; main tables stay on v1.

The package incorporates the user-observed INTERNAL catalog representation,
direct wholetext JSON read and both property-section/option.* representations.
Regression tests check them without notebook monkeypatches. The checked-in
notebooks retain empty outputs because their files were not exported from the
user's live session; outputs were not manufactured from the supplied counts.

## Independently executed local checks

Earlier Object Storage upload results were verified through authenticated OCI
calls and recorded separately. At the Silver milestone the agent independently
transformed the existing **local initial extract**, compared two deterministic
core runs, and checked typed frames with **real local Spark 3.5.6**. Those counts
match the user-reported live Bronze counts and yielded zero local rejections and
an empty quarantine. See [silver-local-validation.json](silver-local-validation.json).

Actual local Spark/Delta integration tests also created managed fixture tables
and verified an insert-only rerun, schema/provider protections and typed
quarantine behavior. They used temporary local data and did not connect to AIDP.
See [aidp-silver-setup.md](aidp-silver-setup.md) for contracts, exact commands,
limitations and the **pending live Silver** execution checklist.
