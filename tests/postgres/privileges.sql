-- STO-7: expose only the designated sender's necessary fields. D-5.
CREATE ROLE stats_reader NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
GRANT pg_read_all_stats TO stats_reader;
CREATE SCHEMA store_guard AUTHORIZATION administrator;
REVOKE ALL ON SCHEMA store_guard FROM PUBLIC;
GRANT USAGE ON SCHEMA store_guard TO stats_reader, app, migrator;
CREATE FUNCTION store_guard.sample()
RETURNS TABLE(pid integer, usename name, application_name text, client_addr inet,
              state text, sync_state text, flush_lsn pg_lsn)
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog
AS $$ SELECT pid, usename, application_name, client_addr, state, sync_state, flush_lsn
      FROM pg_catalog.pg_stat_replication WHERE application_name='provisiond_sync' $$;
ALTER FUNCTION store_guard.sample() OWNER TO stats_reader;
REVOKE ALL ON FUNCTION store_guard.sample() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION store_guard.sample() TO app, migrator;

-- STO-12/13: bootstrap owns the empty schema; migrations own its objects.
CREATE SCHEMA provisiond AUTHORIZATION migrator;
