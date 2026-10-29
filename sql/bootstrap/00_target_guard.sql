begin
  if sys_context('USERENV','SESSION_USER') <> 'ADMIN'
     or upper(sys_context('USERENV','DB_NAME')) <> 'G47E32783F1F853_CRISTINAVARASADBLAKEHOUSEFY27'
     or upper(sys_context('USERENV','CON_NAME')) <> 'G47E32783F1F853_CRISTINAVARASADBLAKEHOUSEFY27'
     or upper(sys_context('USERENV','SERVICE_NAME')) <> 'G47E32783F1F853_CRISTINAVARASADBLAKEHOUSEFY27_MEDIUM'
  then
    raise_application_error(-20001, 'Wrong bootstrap target or session user');
  end if;
end;
/
