-- Read-only reports, followed by executable assertions of the intended policy.
select username, account_status, default_tablespace, temporary_tablespace, profile, authentication_type
from dba_users where username in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO') order by username;
select grantee, privilege, admin_option from dba_sys_privs
where grantee in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO') order by grantee, privilege;
select grantee, granted_role, admin_option, default_role from dba_role_privs
where grantee in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO');
select u.username, 'DATA' tablespace_name, nvl(q.bytes,0) used_bytes, nvl(q.max_bytes,0) max_bytes
from dba_users u left join dba_ts_quotas q on q.username=u.username and q.tablespace_name='DATA'
where u.username in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO') order by u.username;
select username, tablespace_name, bytes, max_bytes from dba_ts_quotas
where username in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO') order by username, tablespace_name;
select grantee, owner, table_name, privilege, grantable from dba_tab_privs
where grantee in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO');
select grantee, owner, table_name, column_name, privilege from dba_col_privs
where grantee in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO');
select owner, object_type, count(*) object_count from dba_objects
where owner in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO') group by owner, object_type;
declare
  n number;
  procedure require_zero(p_count number, p_message varchar2) is
  begin if p_count<>0 then raise_application_error(-20010,p_message); end if; end;
begin
  select count(*) into n from dba_users
  where username in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO') and account_status='OPEN'
    and default_tablespace='DATA' and temporary_tablespace='TEMP' and authentication_type='PASSWORD';
  require_zero(2-n,'Accounts missing or account settings differ');
  select count(*) into n from dba_sys_privs
  where (grantee='LAKEHOUSE_DEMO' and privilege in ('CREATE SESSION','CREATE TABLE'))
     or (grantee='LAKEHOUSE_DEMO_RO' and privilege='CREATE SESSION');
  require_zero(3-n,'Required direct grants missing');
  select count(*) into n from dba_sys_privs
  where grantee in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO') and
    (admin_option='YES' or not (
      (grantee='LAKEHOUSE_DEMO' and privilege in ('CREATE SESSION','CREATE TABLE')) or
      (grantee='LAKEHOUSE_DEMO_RO' and privilege='CREATE SESSION')));
  require_zero(n,'Extra system privileges or grant options: inspect report; no revocations performed');
  select count(*) into n from dba_role_privs where grantee in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO');
  require_zero(n,'Existing role grants differ from baseline; inspect report');
  select count(*) into n from dba_ts_quotas
  where username='LAKEHOUSE_DEMO' and tablespace_name='DATA' and max_bytes=1073741824;
  require_zero(1-n,'Owner DATA quota differs from 1 GiB baseline');
  select count(*) into n from dba_ts_quotas where
    (username='LAKEHOUSE_DEMO_RO' or (username='LAKEHOUSE_DEMO' and tablespace_name<>'DATA'))
    and max_bytes<>0;
  require_zero(n,'Existing quota differs from baseline; inspect report');
  -- No objects have been published at bootstrap, so no object/column grants are expected.
  select count(*) into n from dba_tab_privs where grantee in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO');
  require_zero(n,'Object grants exist: compare them with the explicit publication list');
  select count(*) into n from dba_col_privs where grantee in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO');
  require_zero(n,'Column grants exist: compare them with the explicit publication list');
  dbms_output.put_line('BOOTSTRAP_VERIFIED');
end;
/
