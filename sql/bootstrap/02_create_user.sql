-- Run once per allowlisted username. Substitutions contain PUBLIC information only.
-- Public key is hex-encoded ASCII base64 SPKI (without PEM headers/newlines).
-- Plaintext password is generated inside the database and never returned.
declare
  l_user varchar2(128) := '&&bootstrap_username';
  l_key raw(4000) := hextoraw('&&bootstrap_public_key_hex');
  l_count number;
  l_password varchar2(60);
  l_wrapped raw(2000);
  l_quota varchar2(40);
begin
  if sys_context('USERENV','SESSION_USER') <> 'ADMIN'
     or upper(sys_context('USERENV','DB_NAME')) <> 'G47E32783F1F853_CRISTINAVARASADBLAKEHOUSEFY27'
     or upper(sys_context('USERENV','CON_NAME')) <> 'G47E32783F1F853_CRISTINAVARASADBLAKEHOUSEFY27'
     or upper(sys_context('USERENV','SERVICE_NAME')) <> 'G47E32783F1F853_CRISTINAVARASADBLAKEHOUSEFY27_MEDIUM'
  then raise_application_error(-20001, 'Wrong bootstrap target or session user'); end if;
  if l_user not in ('LAKEHOUSE_DEMO','LAKEHOUSE_DEMO_RO') then
    raise_application_error(-20002, 'Username is not allowlisted');
  end if;
  select count(*) into l_count from dba_users where username=l_user;
  if l_count > 0 then
    dbms_output.put_line('PRESERVED|' || l_user || '|Existing password and account settings unchanged');
    return;
  end if;
  l_password := 'aZ9#' || rawtohex(dbms_crypto.randombytes(24));
  l_wrapped := dbms_crypto.pkencrypt(
    utl_i18n.string_to_raw(l_password,'AL32UTF8'), l_key,
    dbms_crypto.key_type_rsa, dbms_crypto.pkencrypt_rsa_pkcs1_oaep_sha2);
  if l_user='LAKEHOUSE_DEMO' then l_quota := '1G'; else l_quota := '0'; end if;
  begin
    execute immediate 'create user ' || l_user || ' identified by "' || l_password ||
      '" default tablespace DATA temporary tablespace TEMP quota ' || l_quota || ' on DATA';
  exception when others then
    -- Do not propagate SQLERRM, generated DDL, or password variables.
    l_password := null;
    raise_application_error(-20003, 'CREATE USER failed; SQLCODE=' || sqlcode);
  end;
  l_password := null;
  dbms_output.put_line('CREDENTIAL|' || l_user || '|' || rawtohex(l_wrapped));
end;
/
