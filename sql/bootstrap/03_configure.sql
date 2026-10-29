declare
  l_count number;
  l_max_bytes number;
begin
  if sys_context('USERENV','SESSION_USER') <> 'ADMIN'
     or upper(sys_context('USERENV','DB_NAME')) <> 'G47E32783F1F853_CRISTINAVARASADBLAKEHOUSEFY27'
     or upper(sys_context('USERENV','CON_NAME')) <> 'G47E32783F1F853_CRISTINAVARASADBLAKEHOUSEFY27'
     or upper(sys_context('USERENV','SERVICE_NAME')) <> 'G47E32783F1F853_CRISTINAVARASADBLAKEHOUSEFY27_MEDIUM'
  then raise_application_error(-20001, 'Wrong bootstrap target or session user'); end if;
  for r in (
    select 'LAKEHOUSE_DEMO' grantee, 'CREATE SESSION' privilege from dual union all
    select 'LAKEHOUSE_DEMO', 'CREATE TABLE' from dual union all
    select 'LAKEHOUSE_DEMO_RO', 'CREATE SESSION' from dual
  ) loop
    select count(*) into l_count from dba_sys_privs
    where grantee=r.grantee and privilege=r.privilege;
    if l_count=0 then
      execute immediate 'grant ' || r.privilege || ' to ' || r.grantee;
      dbms_output.put_line('GRANTED|' || r.grantee || '|' || r.privilege);
    end if;
  end loop;
  select nvl(max(max_bytes),0) into l_max_bytes from dba_ts_quotas
  where username='LAKEHOUSE_DEMO' and tablespace_name='DATA';
  if l_max_bytes <> -1 and l_max_bytes < 1073741824 then
    execute immediate 'alter user LAKEHOUSE_DEMO quota 1G on DATA';
    dbms_output.put_line('QUOTA|LAKEHOUSE_DEMO|DATA|1G');
  end if;
  -- Preserve existing broader quotas/grants and locked/expired account settings.
  -- 04_verify.sql reports deviations; never revoke or lower existing settings here.
end;
/
