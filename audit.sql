/** Get array of table's primary key columns
**/
CREATE OR REPLACE FUNCTION "${schema}".f_table_pk_columns(table_schema TEXT, table_name TEXT) RETURNS TEXT[] AS $$
    SELECT
        array_agg(a.attname::TEXT)
    FROM
        pg_index AS i
    JOIN
        pg_attribute AS a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE
        i.indrelid = ((CASE WHEN table_schema IS NULL THEN '' ELSE QUOTE_IDENT(table_schema) || '.' END) || QUOTE_IDENT(table_name))::regclass
        AND
        i.indisprimary
    ;
$$ LANGUAGE SQL /*depends on arguments only*/IMMUTABLE;


/** This function returns iterable set of table columns excluding
    user-specified non-auditable columns
**/
CREATE OR REPLACE FUNCTION "${schema}".f_audit_columns(schema_name TEXT, name TEXT, skip_columns TEXT[]) RETURNS SETOF TEXT AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT column_name
        FROM information_schema.columns AS c
        WHERE   c.table_schema = schema_name
            AND c.table_name   = name
            AND NOT(c.column_name::TEXT = ANY(skip_columns))
    LOOP
        RETURN NEXT r.column_name;
    END LOOP;
    RETURN;
END
$$ IMMUTABLE ROWS 200 LANGUAGE plpgsql;


/** Audit log table.
    Stores changes in tables with audit log trigger attached.
**/
CREATE TABLE "${schema}".t_audit_log (
     date_time      TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp()
    ,transaction_id BIGINT NOT NULL DEFAULT txid_current() -- allows grouping changes by transaction
    ,client_ip      INET DEFAULT inet_client_addr()
    ,identity_id    TEXT DEFAULT NULLIF(CURRENT_SETTING('${schema}.current_identity_id', true), '')
    ,operation      TEXT NOT NULL
    ,table_schema   TEXT NOT NULL
    ,table_name     TEXT NOT NULL
    ,record_id      JSONB NOT NULL
    ,old_values     JSONB
    ,new_values     JSONB
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
CREATE OR REPLACE FUNCTION "${schema}".f_audit() RETURNS TRIGGER AS $$
DECLARE
    rec          RECORD;
    rec_id       JSONB;
    skip_columns TEXT[];
    key_columns  TEXT[];
    key_size     INT;
    key_values   TEXT[];
    key_val      TEXT;
    old_object   JSONB;
    new_object   JSONB;
    i            INT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        rec := OLD;
    ELSE
        rec := NEW;
    END IF;

    key_columns := CASE
        WHEN TG_NARGS < 1 OR TG_ARGV[0] IS NULL THEN "${schema}".f_table_pk_columns(TG_TABLE_SCHEMA, TG_TABLE_NAME)
        ELSE TG_ARGV[0]::TEXT[]
    END;
    key_size := ARRAY_LENGTH(key_columns, 1);

    FOR i IN 1 .. key_size LOOP
        EXECUTE format('SELECT CAST($1.%I AS TEXT)', key_columns[i]) INTO key_val USING rec;
        key_values := array_append(key_values, key_val);
    END LOOP;

    rec_id := JSONB_OBJECT(key_columns, key_values);

    skip_columns := ARRAY_CAT(key_columns, TG_ARGV[1:(TG_NARGS - 1)]);

    IF TG_OP = 'UPDATE' THEN
        SELECT
             jsonb_object_agg(COALESCE(old_values.key, new_values.key), old_values.value)
            ,jsonb_object_agg(COALESCE(old_values.key, new_values.key), new_values.value)
        INTO
             old_object
            ,new_object
        FROM
            jsonb_each(TO_JSONB(OLD) - skip_columns) AS old_values
            FULL OUTER JOIN jsonb_each(TO_JSONB(NEW) - skip_columns) AS new_values ON new_values.key = old_values.key
        WHERE
            new_values.value IS DISTINCT FROM old_values.value
        ;
    ELSIF TG_OP = 'INSERT' THEN
        old_object := NULL;
        new_object := TO_JSONB(NEW) - skip_columns;
    ELSIF TG_OP = 'DELETE' THEN
        old_object := TO_JSONB(OLD) - skip_columns;
        new_object := NULL;
    END IF;

    INSERT INTO "${schema}".t_audit_log (
         operation
        ,table_schema
        ,table_name
        ,record_id
        ,old_values
        ,new_values
    ) VALUES (
         TG_OP
        ,TG_TABLE_SCHEMA
        ,TG_TABLE_NAME
        ,rec_id
        ,old_object
        ,new_object
    );

    RETURN NEW;
END
$$ LANGUAGE plpgsql;


/** Template table with columns common for all autdited tables
    Use `CREATE TABLE (... LIKE "${schema}".__audited INCLUDING ALL)`
**/
CREATE TABLE "${schema}".__audited (
     revision INT NOT NULL DEFAULT 1
    ,created_on TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp()
    ,created_by INT DEFAULT NULLIF(CURRENT_SETTING('${schema}.current_identity_id', true), '')::INT /* REFERENCES t_identity */
    ,updated_on TIMESTAMPTZ
    ,updated_by INT /* REFERENCES t_identity */
);


/** Trigger function to update revision on object updates.
**/
CREATE OR REPLACE FUNCTION "${schema}".f_revision() RETURNS TRIGGER AS $$
DECLARE
    identity_id INT;
BEGIN
    IF NEW.revision IS NOT DISTINCT FROM OLD.revision THEN
        NEW.revision := COALESCE(OLD.revision, 1) + 1;
    ELSIF NEW.revision < OLD.revision THEN
       RAISE 'Cannot update %.%: new revision % is less than old revision %', TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.revision, OLD.revision;
    END IF;
    NEW.updated_on := statement_timestamp();

    identity_id := NULLIF(CURRENT_SETTING('${schema}.current_identity_id', true), '')::INT;

    IF identity_id IS NOT NULL THEN
        NEW.updated_by := identity_id;
    ELSIF NEW.updated_by IS NOT NULL THEN
        NEW.updated_by := NULL;
    END IF;

    RETURN NEW;
END
$$ LANGUAGE plpgsql;
