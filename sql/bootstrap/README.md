# Database bootstrap

Use the existing SQLcl MCP integration and the exact saved alias
`CristinaVaras-ADBLakehouse-FY27`. No additional MCP configuration is needed.

## Policy

| Account | Direct system privileges | DATA quota | Purpose |
| --- | --- | --- | --- |
| `LAKEHOUSE_DEMO` | `CREATE SESSION`, `CREATE TABLE` | 1 GiB (1,073,741,824 bytes) | Own demo published tables |
| `LAKEHOUSE_DEMO_RO` | `CREATE SESSION` | 0 | Read explicitly published objects |

Both accounts use default tablespace `DATA`, temporary tablespace `TEMP`, and
the database's `DEFAULT` profile. No project roles, ANY privileges, unlimited
quota, object grants, or business objects are introduced. The bounded owner
quota is a starting limit for the demo, not a reservation of database storage.

The publication list is currently empty. When tables are introduced later,
grant `READ` on each approved object explicitly to `LAKEHOUSE_DEMO_RO`, without
grant option. Do not grant schema-wide access, `SELECT ANY TABLE`, `READ ANY TABLE`,
or broad roles. Oracle's built-in `PUBLIC` privileges remain database-managed;
these scripts do not redefine them.

## Files and order

1. `00_target_guard.sql`: require ADMIN on the exact verified database/container/service.
2. `01_inspect.sql`: report identity, effective session privileges and roles, direct
   delegation privileges, existing accounts, grants, quotas, and object counts.
3. `02_create_user.sql`: run once for each allowlisted account; existing accounts
   are reported as `PRESERVED` without password or account-setting changes.
4. `03_configure.sql`: add missing required direct privileges and raise an owner
   quota below 1 GiB to 1 GiB. Existing larger/unlimited quotas are preserved.
5. `04_verify.sql`: report resulting settings and assert the bootstrap baseline.

`run.sql` executes this order in a local SQLcl session. Inspect the preflight
report before applying changes if either account already exists. Extra roles,
privileges, quotas, locked/expired accounts, and object grants are reported by
inspection/verification; the scripts never revoke, unlock, reset passwords,
change existing profiles, lower quotas, or remove objects. Verification stops
on a baseline deviation so it can be reviewed. It never treats preserved excess
privileges as a successful least-privilege setup. Existing objects are reported
but are not removed. The verification of zero object grants describes bootstrap;
update the publication policy checks when objects are explicitly published later.

Oracle DDL commits implicitly. A partial run is not transactionally reversible;
rerun the scripts to add missing settings while preserving existing accounts.

## Password handling

MCP records submitted SQL in `ADMIN.DBTOOLS$MCP_LOG`. Password literals must
never be supplied in MCP requests, script substitutions, command arguments,
chat, or Git. Turning off SQLcl echo alone does not protect that audit log.

The creation block generates a separate 192-bit random password inside the
database with `DBMS_CRYPTO.RANDOMBYTES`. It encrypts the password with a local
3072-bit RSA public key using OAEP with SHA-256 before creating the account.
Only `CREDENTIAL|username|ciphertext` is returned. The private key stays local;
decryption writes directly to a mode-0600 JSON file inside a mode-0700 directory
outside the repository. Neither plaintext passwords nor private keys cross MCP.
Creation errors expose only an error code, never the generated DDL or password.

This uses Oracle's [DBMS_CRYPTO asymmetric encryption API](https://docs.oracle.com/en/database/oracle/oracle-database/26/arpls/DBMS_CRYPTO.html).
The public-key input format was verified against this instance: hex-encoded
ASCII base64 SPKI, with PEM headers and whitespace removed. A non-secret
encryption/decryption probe succeeded before any users were created.

`local_secrets.py` needs Python 3 and the `cryptography` package. It has three actions:

```sh
# Run from the repository. Choose a NEW directory outside Git and cloud sync.
umask 077
python3 sql/bootstrap/local_secrets.py init --directory /private/tmp/my-lakehouse-secrets
python3 sql/bootstrap/local_secrets.py public --directory /private/tmp/my-lakehouse-secrets
```

Only the `public` action prints key material, and that is public information.
Never print, paste, or commit `private-key.pem` or `credentials.json`.

For an MCP run, list saved connections, connect by exact alias, and call
`schema_information` immediately after connection. Execute `00` and `01`, review
the target and account differences, then execute the saved `02` block twice,
replacing `&&bootstrap_username` with each allowlisted username and
`&&bootstrap_public_key_hex` with the helper's PUBLIC output. Save each returned
ciphertext receipt to a local file in the private directory and recover it
immediately before moving to the next account. Execute `03` and `04` afterward.
Do not submit SQLcl substitution directives to `sql_run`; substitute public
values in the client, as was done for the recorded execution.

For a local SQLcl rerun, connect with the saved connection manager, then:

```sql
-- PUBLIC_HEX means the complete output of the helper's public action.
set echo off
set verify off
set serveroutput on
define bootstrap_public_key_hex = PUBLIC_HEX
spool /private/tmp/my-lakehouse-secrets/run-receipt.txt
@sql/bootstrap/run.sql
spool off
```

Recover newly created credentials locally:

```sh
python3 sql/bootstrap/local_secrets.py recover \
  --directory /private/tmp/my-lakehouse-secrets \
  --receipt /private/tmp/my-lakehouse-secrets/run-receipt.txt
```

Recovery also accepts a saved MCP response containing the ciphertext receipts.
It merges new usernames, refuses to overwrite a different existing credential,
and never prints passwords. A rerun where both users already exist emits no new
credentials; keep the original private file. Do not call `recover` on a receipt
that contains only `PRESERVED` messages.

For this execution the private directory location is recorded in
[EXECUTION.md](EXECUTION.md). `/private/tmp` can be cleaned by the operating
system: transfer the credentials to your local password manager before cleanup.
If a receipt is lost after account creation, rerunning does not recover or reset
that account's password; retain the original receipt and private key until the
credentials are safely stored. Recovery and initial account creation are separate
steps, so preserve each returned ciphertext receipt promptly.
