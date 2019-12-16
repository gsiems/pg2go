package meta

import (
	"fmt"

	_ "github.com/lib/pq"

	u "github.com/gsiems/pg2go/util"
)

// PgTableMetadata contains metadata for postgresql tables and views
type PgTableMetadata struct {
	SchemaName  string `db:"schema_name"`
	ObjName     string `db:"obj_name"`
	ObjKind     string `db:"obj_kind"`
	ObjType     string `db:"obj_type"`
	Privs       string `db:"privs"`
	Description string `db:"description"`
	StructName  string
	Columns     []PgColumnMetadata
}

// GetTableMetas returns the metadata for the avaiable tables/views
func GetTableMetas(connStr, schema, objName, user string) (tables []PgTableMetadata, err error) {

	db, errq := OpenDB(connStr)
	if errq != nil {
		err = fmt.Errorf("Expected connection, got error: %q", errq)
		return
	}
	defer db.CloseDB()

	tables, errq = listTableMetas(db, schema, objName, user)
	if errq != nil {
		err = fmt.Errorf("Expected table metadata, got error: %q", errq)
		return
	}
	for i, f := range tables {
		tables[i].StructName = u.ToUpperCamelCase(f.ObjName)

		columns, errq := listTableColumnMetas(db, f.SchemaName, f.ObjName)
		if errq != nil {
			err = fmt.Errorf("Expected column metadata for tables, got error: %q", errq)
			return
		}
		tables[i].Columns = columns
	}
	return
}

// listTypeMetas returns the list of avaiable tables/views
func listTableMetas(db *DB, schema, objName, user string) (d []PgTableMetadata, err error) {
	err = db.Select(&d, `
WITH args AS (
    SELECT coalesce ( $1, '' ) AS schema_name,
            coalesce ( $2, '' ) AS obj_name,
            coalesce ( $3, '' ) AS username
),
obj AS (
    SELECT n.nspname::text AS schema_name,
            c.relname::text AS obj_name,
            c.relkind::text AS obj_kind,
            CASE c.relkind
                WHEN 'r' THEN 'table'
                WHEN 'v' THEN 'view'
                WHEN 'm' THEN 'materialized view'
                WHEN 'i' THEN 'index'
                WHEN 'S' THEN 'sequence'
                WHEN 's' THEN 'special'
                WHEN 'f' THEN 'foreign table'
                WHEN 'p' THEN 'table'
                END AS obj_type,
            pg_catalog.obj_description(c.oid, 'pg_class') AS description,
            unnest ( c.relacl ) AS acl
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n
            ON n.oid = c.relnamespace
        CROSS JOIN args
        WHERE c.relkind IN ( 'r', 'v', 'm', 'S', 's', 'f', 'p', '' )
            AND pg_catalog.pg_table_is_visible ( c.oid )
            AND n.nspname <> 'pg_catalog'
            AND n.nspname <> 'information_schema'
            AND n.nspname !~ '^pg_toast'
            AND ( n.nspname = args.schema_name
                OR args.schema_name = '' )
)
-- when no user is specified then we get potential duplicates based on
-- how many users have privs to the object
SELECT obj.schema_name,
        obj.obj_name,
        obj.obj_kind,
        obj.obj_type,
        regexp_replace ( regexp_replace ( obj.acl::text, '^[^=]+=', '' ), '[/].+', '' ) AS privs,
        coalesce ( obj.description, '' ) AS description
    FROM obj
    CROSS JOIN args
    WHERE ( obj.obj_name LIKE args.obj_name || '=%'
            OR args.obj_name = '' )
        AND ( obj.acl::text LIKE args.obj_name || '=%'
            OR args.obj_name = '' )
        AND ( obj.acl::text NOT LIKE 'postgres=%' )
    ORDER BY obj.schema_name,
        obj.obj_name,
        obj.obj_type
`, schema, objName, user)
	return
}

/*
WITH obj AS (
    SELECT n.nspname::text AS schema_name,
            c.relname::text AS obj_name,
            c.relkind::text AS obj_kind,
            CASE c.relkind
                WHEN 'r' THEN 'table'
                WHEN 'v' THEN 'view'
                WHEN 'm' THEN 'materialized view'
                WHEN 'i' THEN 'index'
                WHEN 'S' THEN 'sequence'
                WHEN 's' THEN 'special'
                WHEN 'f' THEN 'foreign table'
                WHEN 'p' THEN 'table'
                END AS obj_type,
            pg_catalog.obj_description(c.oid, 'pg_class') AS description,
            unnest ( c.relacl ) AS acl
        FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n
            ON n.oid = c.relnamespace
        WHERE c.relkind IN ( 'r', 'v', 'm', 'S', 's', 'f', 'p', '' )
            AND pg_catalog.pg_table_is_visible ( c.oid )
            AND n.nspname <> 'pg_catalog'
            AND n.nspname <> 'information_schema'
            AND n.nspname !~ '^pg_toast'
            AND ( n.nspname = 'tasker'
                OR coalesce ( 'tasker', '' ) = '' )
)
-- when no user is specified then we get portential duplicates based on
-- how many users have privs to the object
SELECT schema_name,
        obj_name,
        obj_kind,
        obj_type,
        regexp_replace ( regexp_replace ( acl::text, '^[^=]+=', '' ), '[/].+', '' ) AS privs,
        coalesce ( description, '' ) AS description
    FROM obj
    WHERE ( obj_name LIKE '' || '=%'
            OR coalesce ( '', '' ) = '' )
        AND ( acl::text LIKE 'tasker_user' || '=%'
            OR coalesce ( 'tasker_user', '' ) = '' )
        AND ( acl::text NOT LIKE 'postgres=%' )
    ORDER BY schema_name,
        obj_name,
        obj_type ;
*/

// listTableColumnMetas returns the metadata for the avaiable table/view columns
func listTableColumnMetas(db *DB, schema, objName string) (d []PgColumnMetadata, err error) {
	err = db.Select(&d, `
WITH args AS (
    SELECT coalesce ( $1, '' ) AS schema_name,
            coalesce ( $2, '' ) AS obj_name
),
cols AS (
    SELECT n.nspname::text AS schema_name,
            c.relname::text AS obj_name,
            a.attname::text AS column_name,
            pg_catalog.format_type ( a.atttypid, a.atttypmod ) AS data_type,
            a.attnotnull AS is_required,
            a.attnum AS ordinal_position,
            pg_catalog.col_description ( a.attrelid, a.attnum ) AS description
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c
            ON ( c.oid = a.attrelid )
        JOIN pg_catalog.pg_namespace n
            ON ( n.oid = c.relnamespace )
        CROSS JOIN args
        WHERE a.attnum > 0
            AND NOT a.attisdropped
            AND n.nspname = args.schema_name
            AND c.relname = args.obj_name
),
pk AS (
    SELECT nr.nspname AS schema_name,
            r.relname AS obj_name,
            regexp_split_to_table ( split_part ( split_part ( pg_get_constraintdef ( c.oid ), '(', 2 ), ')', 1 ), ', +' ) AS column_name
        FROM pg_class r
        INNER JOIN pg_namespace nr
            ON ( nr.oid = r.relnamespace )
        INNER JOIN pg_constraint c
            ON ( c.conrelid = r.oid )
        WHERE r.relkind = 'r'
            AND c.contype = 'p'
            AND c.contype <> 'f'
)
SELECT cols.column_name,
        cols.data_type,
        cols.ordinal_position,
        cols.is_required,
        CASE
            WHEN pk.column_name IS NOT NULL THEN true
            ELSE false
            END AS is_pk,
        coalesce ( cols.description, '' ) AS description
    FROM cols
    LEFT JOIN pk
        ON ( pk.schema_name = cols.schema_name
            AND pk.obj_name = cols.obj_name
            AND pk.column_name = cols.column_name )
    ORDER BY cols.ordinal_position
`, schema, objName)
	return
}
