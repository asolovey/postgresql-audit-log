/** Get array of table's primary key columns
**/
CREATE OR REPLACE FUNCTION f_table_pk_columns( table_schema TEXT, table_name TEXT ) RETURNS TEXT[] AS $$
    SELECT
        array_agg( a.attname::TEXT )
    FROM
        pg_index AS i
    JOIN
        pg_attribute AS a ON a.attrelid = i.indrelid AND a.attnum = ANY( i.indkey )
    WHERE
        i.indrelid = ( (CASE WHEN table_schema IS NULL THEN '' ELSE QUOTE_IDENT( table_schema ) || '.' END) || QUOTE_IDENT( table_name ) )::regclass
        AND
        i.indisprimary
    ;
$$ LANGUAGE SQL /*depends on arguments only*/IMMUTABLE;

/** This function returns iterable set of table columns excluding
    user-specified non-auditable columns
**/
CREATE OR REPLACE FUNCTION f_audit_columns( schema_name TEXT, name TEXT, skip_columns TEXT[] ) RETURNS SETOF TEXT AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT column_name
        FROM information_schema.columns AS c
        WHERE   c.table_schema = schema_name
            AND c.table_name   = name
            AND NOT( c.column_name::TEXT = ANY( skip_columns ) )
    LOOP
        RETURN NEXT r.column_name;
    END LOOP;
    RETURN;
END
$$ IMMUTABLE ROWS 200 LANGUAGE plpgsql;

/** Audit log table.
    Stores changes in tables with audit log trigger attached.
**/
CREATE TABLE t_audit_log (
     date_time      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,transaction_id BIGINT NOT NULL DEFAULT txid_current() -- allows grouping changes by transaction
    ,client_ip      INET DEFAULT inet_client_addr()
    ,operation      TEXT NOT NULL
    ,table_schema   TEXT NOT NULL
    ,table_name     TEXT NOT NULL
    ,record_id      TEXT NOT NULL
    ,column_name    TEXT NOT NULL
    ,old_value      TEXT
    ,new_value      TEXT
);

/** Trigger function which creates audit log records on table row updates.
    @param record ID columns    {ARRAY} optional; if empty, uses f_table_pk_columns() to get primary key columns.
    @param *column name to skip {TEXT} optional arguments starting from the second are columns name to be excluded from audit log.

    Use this function in AFTER INSERT OR UPDATE trigger. According to https://www.postgresql.org/docs/current/static/trigger-definition.html

        "If more than one trigger is defined for the same event on the same relation,
        the triggers will be fired in alphabetical order by trigger name"

    Therefore, use a name like "zz_audit" for audit trigger to ensure that it fires after other
    triggers and records changes made by those triggers.

    Examples:
        -- Simple case, default primary key, no excluded columns
        CREATE TRIGGER zz_audit AFTER INSERT OR UPDATE OR DELETE ON <table>
            FOR EACH ROW EXECUTE PROCEDURE f_audit();

        -- No primary key (or it is unusable), so unique row identifier columns are listed explicitly
        CREATE TRIGGER zz_audit AFTER INSERT OR UPDATE OR DELETE ON <table>
            FOR EACH ROW EXECUTE PROCEDURE f_audit('{"user_id", "account_id", "company_id"}');
**/
CREATE OR REPLACE FUNCTION f_audit() RETURNS TRIGGER AS $F$
DECLARE
    rec          RECORD;
    rec_id       TEXT;
    skip_columns TEXT[];
    key_columns  TEXT[];
    key_size     INT;
    key_val      TEXT;
    col_name     TEXT;
    old_val      TEXT;
    new_val      TEXT;
    i            INT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        rec := OLD;
    ELSE
        rec := NEW;
    END IF;

    rec_id := '';

    key_columns := CASE
        WHEN TG_NARGS < 1 OR TG_ARGV[0] IS NULL THEN f_table_pk_columns( TG_TABLE_SCHEMA, TG_TABLE_NAME )
        ELSE TG_ARGV[0]::TEXT[]
    END;
    key_size := ARRAY_LENGTH( key_columns, 1 );

    FOR i IN 1 .. key_size LOOP
        EXECUTE format('SELECT CAST( $1.%I AS TEXT )', key_columns[i] ) INTO key_val USING rec;
        IF key_val IS NULL THEN
            key_val := '#';
        ELSE
            /* escape special characters */
            key_val := regexp_replace( key_val, '([~|#])', E'~\\1', 'g' );
        END IF;

        IF i = 1 THEN
            rec_id := key_val;
        ELSE
            rec_id := rec_id || '|' || key_val;
        END IF;
    END LOOP;

    skip_columns := ARRAY_CAT( key_columns, TG_ARGV[ 1:( TG_NARGS - 1 ) ] );

    IF TG_OP = 'UPDATE' THEN
        FOR col_name IN SELECT * FROM f_audit_columns( TG_TABLE_SCHEMA, TG_TABLE_NAME, skip_columns ) LOOP
            EXECUTE format('SELECT CAST( $1.%I AS TEXT ), CAST( $2.%I AS TEXT )', col_name, col_name )
                INTO old_val, new_val USING OLD, NEW;

            IF old_val IS DISTINCT FROM new_val THEN
                INSERT INTO t_audit_log (
                     operation
                    ,table_schema
                    ,table_name
                    ,record_id
                    ,column_name
                    ,old_value
                    ,new_value
                ) VALUES (
                     TG_OP
                    ,TG_TABLE_SCHEMA
                    ,TG_TABLE_NAME
                    ,rec_id
                    ,col_name
                    ,old_val
                    ,new_val
                );
            END IF;
        END LOOP;
    ELSIF TG_OP = 'INSERT' THEN
        FOR col_name IN SELECT * FROM f_audit_columns( TG_TABLE_SCHEMA, TG_TABLE_NAME, skip_columns ) LOOP
            EXECUTE format('SELECT CAST( $1.%I AS TEXT )', col_name ) INTO new_val USING NEW;

            IF new_val IS NOT NULL THEN
                INSERT INTO t_audit_log (
                     operation
                    ,table_schema
                    ,table_name
                    ,record_id
                    ,column_name
                    ,new_value
                ) VALUES (
                     TG_OP
                    ,TG_TABLE_SCHEMA
                    ,TG_TABLE_NAME
                    ,rec_id
                    ,col_name
                    ,new_val
                );
            END IF;
        END LOOP;
    ELSIF TG_OP = 'DELETE' THEN
        FOR col_name IN SELECT * FROM f_audit_columns( TG_TABLE_SCHEMA, TG_TABLE_NAME, skip_columns ) LOOP
            EXECUTE format('SELECT CAST( $1.%I AS TEXT )', col_name ) INTO old_val USING OLD;

            IF old_val IS NOT NULL THEN
                INSERT INTO t_audit_log (
                     operation
                    ,table_schema
                    ,table_name
                    ,record_id
                    ,column_name
                    ,old_value
                ) VALUES (
                     TG_OP
                    ,TG_TABLE_SCHEMA
                    ,TG_TABLE_NAME
                    ,rec_id
                    ,col_name
                    ,old_val
                );
            END IF;
        END LOOP;
    END IF;

    RETURN NEW;
END
$F$ LANGUAGE plpgsql;
