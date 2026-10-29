select user connected_user,
       sys_context('USERENV','DB_NAME') db_name,
       sys_context('USERENV','DB_UNIQUE_NAME') db_unique_name,
       sys_context('USERENV','SERVICE_NAME') service_name,
       sys_context('USERENV','CON_NAME') container_name,
       sys_context('USERENV','CLOUD_SERVICE') cloud_service
from dual;
select privilege from session_privs order by privilege;
select role from session_roles order by role;
select privilege, admin_option from dba_sys_privs
where grantee='ADMIN' and privilege in ('CREATE USER','ALTER USER','CREATE SESSION','CREATE TABLE');
select property_name, property_value from database_properties
where property_name in ('DEFAULT_PERMANENT_TABLESPACE','DEFAULT_TEMP_TABLESPACE');
select username, account_status, default_tablespace, temporary_tablespace, profile, authentication_type
from dba_users where username in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO');
select grantee, privilege, admin_option from dba_sys_privs
where grantee in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO') order by grantee, privilege;
select grantee, granted_role, admin_option, default_role from dba_role_privs
where grantee in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO');
select username, tablespace_name, bytes, max_bytes from dba_ts_quotas
where username in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO');
select grantee, owner, table_name, privilege, grantable from dba_tab_privs
where grantee in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO');
select grantee, owner, table_name, column_name, privilege from dba_col_privs
where grantee in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO');
select owner, object_type, count(*) object_count from dba_objects
where owner in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO') group by owner, object_type;
