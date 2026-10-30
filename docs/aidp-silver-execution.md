# Silver execution evidence

## User-reported AIDP result — 30 October 2026

The user reported successful initial Silver execution in AIDP:

| Managed Silver entity | Reported rows |
| --- | ---: |
| customers | 600 |
| orders | 30,600 |
| deliveries | 30,000 |
| events | 94,572 |

The user reported **zero rejected rows, an empty quarantine, zero inserted rows
on the second run, and unchanged fingerprints**. The exact job timestamps,
compute IDs, fingerprint values and raw logs were not supplied. These results
are user-provided runtime evidence. The agent did not execute or independently
inspect that live AIDP job. Gold must still validate its actual inputs on execution.

The current notebook configuration uses the LF v2 initial sources, seed 42,
reference date 2026-10-28. Gold reads only the managed Silver entities and exact
v2 control documents; it checks every Silver `_source_file` against the expected
v2 object URI. It does not alter Bronze, Silver or the uploaded sources.

Prior local Silver tests remain documented in [Silver setup](aidp-silver-setup.md)
and [silver-local-validation.json](silver-local-validation.json). They are separate
from this user-reported observation. Continue with
[03_build_gold.ipynb](../notebooks/03_build_gold.ipynb) using the
[Gold instructions](aidp-gold-setup.md). Live Gold verification is pending.
