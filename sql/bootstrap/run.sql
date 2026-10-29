-- Start in sql/bootstrap with the verified ADMIN connection already active.
-- Define bootstrap_public_key_hex using local_secrets.py public (PUBLIC data only).
set echo off
set verify off
set serveroutput on
set linesize 32767
whenever sqlerror exit failure rollback
@@00_target_guard.sql
@@01_inspect.sql
define bootstrap_username = LAKEHOUSE_DEMO
@@02_create_user.sql
define bootstrap_username = LAKEHOUSE_DEMO_RO
@@02_create_user.sql
@@03_configure.sql
@@04_verify.sql
undefine bootstrap_username
undefine bootstrap_public_key_hex
