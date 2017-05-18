# PostgreSQL Table Audit Log

Simple, trigger-based audit log for PostgreSQL tables.

Use `audit.sql` to add audit table `t_audit_log`, trigger function `f_audit`
and a few utility functions to your database. Create `AFTER INSERT OR UPDATE OR DELETE`
trigger on any table to keep the log for.

According to https://www.postgresql.org/docs/current/static/trigger-definition.html

    If more than one trigger is defined for the same event on the same relation,
    the triggers will be fired in alphabetical order by trigger name.

Therefore, use a name like "zz_audit" for audit trigger to ensure that it fires after other
triggers and records changes made by those triggers.

## Examples

### Simple case: default primary key, no excluded columns.

```sql
CREATE TABLE t_user (
     id            SERIAL PRIMARY KEY
    ,name          TEXT NOT NULL
    ,email         TEXT NOT NULL
    ,admin         BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TRIGGER zz_audit AFTER INSERT OR UPDATE OR DELETE ON t_user
    FOR EACH ROW EXECUTE PROCEDURE f_audit();
```

### No primary key (or it is unusable), so unique row identifier columns are listed explicitly

```sql
CREATE TABLE t_tag (
     user_id    INT CHECK( user_id != 0 )
    ,account_id INT CHECK( account_id != 0 )
    ,company_id INT CHECK( company_id != 0 )
    ,name       TEXT NOT NULL
);

/* Cannot use nullable columns in primary key, so unique constraint instead: */
CREATE UNIQUE INDEX u_tag ON t_tag( COALESCE( user_id, 0 ), COALESCE( account_id, 0 ), COALESCE( company_id, 0 ) );

CREATE TRIGGER zz_audit AFTER INSERT OR UPDATE OR DELETE ON t_tag
    FOR EACH ROW EXECUTE PROCEDURE f_audit('{"user_id", "account_id", "company_id"}');
```
