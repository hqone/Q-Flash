# PostgreSQL Q-Flash extension

The Q-Flash (Query Flash) module provides logging of query plans to a table in the DB with the key set in the session.

>NOT RECOMMENDED FOR PRODUCTION ENVIRONMENT

## INSTALL
Commands to install:
```console
make PG_CONFIG=/usr/pgsql-10/bin/pg_config clean
make PG_CONFIG=/usr/pgsql-10/bin/pg_config install 
systemctl stop postgresql-10.service
systemctl start postgresql-10.service
```
Then register as C Function in the DB:
```SQL
CREATE FUNCTION qflash_init(TEXT,TEXT) RETURNS bool AS 'q-flash', 'qflash_init' LANGUAGE C STRICT;
```

Finally, create table for logging:
```
SELECT public.qflash_init('public', 'qflash');
GRANT ALL ON TABLE public.qflash TO public;
```

## USAGE

```SQL
LOAD 'q-flash';
SET qflash.enabled = TRUE;
SET qflash.log_namespace_name = 'public';
SET qflash.log_relname = 'qflash';
SET qflash.log_hash = '';
```

`log_hash` can be set with a correlation id like request_id or user_id or something else what for you want to index query plans.
> `SET qflash.log_hash = 'REQUEST_ID';`

