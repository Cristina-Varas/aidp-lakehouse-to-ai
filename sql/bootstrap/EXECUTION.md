# Bootstrap execution — 28 October 2026 (Europe/Madrid)

## Verified target

The saved connection was rediscovered through the existing SQLcl MCP connection
list and used by its exact name. No replacement connection or MCP configuration
was created, and `ADB_FY27_ADMIN` was not used.

| Check | Observed value |
| --- | --- |
| Saved alias | `CristinaVaras-ADBLakehouse-FY27` |
| Saved username / connected session user | `admin` / `ADMIN` |
| Saved connect-string alias | `CRISTINAVARASADBLAKEHOUSEFY27_MEDIUM` |
| Saved endpoint | `adb.eu-frankfurt-1.oraclecloud.com:1522`, TCPS |
| Saved TNS service | `g47e32783f1f853_cristinavarasadblakehousefy27_medium.adb.oraclecloud.com` |
| Database name | `G47E32783F1F853_CRISTINAVARASADBLAKEHOUSEFY27` |
| Connected service | `G47E32783F1F853_CRISTINAVARASADBLAKEHOUSEFY27_medium` |
| Container | `G47E32783F1F853_CRISTINAVARASADBLAKEHOUSEFY27` |
| DB_UNIQUE_NAME | `te4f1pod` |
| Cloud service indicator | `DWCS` |
| Database version reported by MCP | `23.26.4.1.0` |
| Open mode | `READ WRITE` |
| NLS language / character set | `AMERICAN` / `AL32UTF8` |
| SQLcl autocommit | OFF; bootstrap DDL commits implicitly |

The saved service's database-specific stem matches the reported database,
container, and connected service. The saved host places the endpoint in OCI
Frankfurt; the host is shared and is not sufficient alone to identify the
instance. The database-specific service mapping establishes that this is the
Lakehouse target selected by the user. The OCI console display name and OCID were
not independently fetched and are not inferred from `DB_UNIQUE_NAME` or the
connection label. No credentials or wallet contents were displayed.

## Preflight and execution

After connecting, MCP `schema_information` was called before SQL inspection.
Read-only identity, account, grants, and quota checks were performed. The saved
`00_target_guard.sql` and `01_inspect.sql` scripts were then executed through MCP.

ADMIN had 287 effective session privileges and 85 enabled roles. Required
`CREATE USER`, `ALTER USER`, `CREATE SESSION`, and `CREATE TABLE` privileges were
also present as direct ADMIN grants with `ADMIN_OPTION=YES`. Default permanent
and temporary tablespaces were `DATA` and `TEMP`.

Neither target user existed. No pre-existing target grants, quotas, or objects
were found, so there were no existing-user differences to reconcile.

SQLcl's JavaScript scripting engine was unavailable. Read-only RSA wrapping
probes established the supported public-key encoding and SHA-256 OAEP settings.
Failed probes made no database changes. A local decrypt of a non-secret probe
confirmed the mechanism before user creation.

The saved `02_create_user.sql` block was executed separately for each username
through SQLcl MCP, substituting only public values. Each encrypted credential
was recovered locally before continuing. `03_configure.sql` then granted the
required privileges, and `04_verify.sql` completed with `BOOTSTRAP_VERIFIED`.

## Verified result

| Account | Status | Direct system grants | Admin option | DATA quota / used |
| --- | --- | --- | --- | --- |
| `LAKEHOUSE_DEMO` | `OPEN` | `CREATE SESSION`, `CREATE TABLE` | `NO` | 1,073,741,824 / 0 bytes |
| `LAKEHOUSE_DEMO_RO` | `OPEN` | `CREATE SESSION` | `NO` | 0 / 0 bytes |

Both accounts use `DATA`, `TEMP`, `DEFAULT` profile and `PASSWORD`
authentication. The reader has no `DBA_TS_QUOTAS` row, which means no storage
quota; the verification report normalizes that to zero. There are no other
quotas, role grants, object grants, or column grants for either account.
Neither schema contains objects. No business tables were created or data loaded.

Both creation blocks were executed again: each returned `PRESERVED` without
changing the password or account settings. Configuration was rerun without
adding grants, and the final verification again returned `BOOTSTRAP_VERIFIED`.

## Local credential handoff

The local credential file is:

`/private/tmp/aidp-lakehouse-secrets-4fuquc4i/credentials.json`

It contains one entry per account and is mode `0600`; its directory is mode
`0700`. The wrapping private key is also local with mode `0600`. Both decrypted
entries and their file permissions were validated without printing values.
A byte comparison confirmed neither password appears in repository files.
The directory is outside Git and outside OneDrive. Transfer the credentials
to your local password manager before operating-system temporary-file cleanup.

MCP requests and execution output contained generation code, public keys,
ciphertext and status messages only. No plaintext password was supplied in SQL,
chat, Git, process arguments, or execution logs. The database used normal
password-bearing CREATE USER DDL internally; no SQL tracing was enabled by this
bootstrap. No user was dropped, no existing password reset, and no object removed.
