package meta

import (
	"fmt"

	_ "github.com/lib/pq"

	u "github.com/gsiems/pg2go/util"
)

// PgUsertypeMetadata contains metadata for postgresql uaer defined types
type PgUsertypeMetadata struct {
	SchemaName  string `db:"schema_name"`
	ObjName     string `db:"obj_name"`
	ObjType     string `db:"obj_type"`
	Description string `db:"description"`
	StructName  string
	Columns     []PgColumnMetadata
}

// GetTypeMetas returns the metadata for the avaiable user types
func GetTypeMetas(connStr, schema, objName, user string) (types []PgUsertypeMetadata, err error) {

	db, errq := OpenDB(connStr)
	if errq != nil {
		err = fmt.Errorf("Expected connection, got error: %q", errq)
		return
	}
	defer db.CloseDB()

	types, errq = listTypeMetas(db, schema, objName)
	if errq != nil {
		err = fmt.Errorf("Expected type metadata, got error: %q", errq)
		return
	}
	for i, f := range types {
		types[i].StructName = u.ToUpperCamelCase(f.ObjName)
		columns, errq := listTypeColumnMetas(db, f.SchemaName, f.ObjName)
		if errq != nil {
			err = fmt.Errorf("Expected column metadata for composite types, got error: %q", errq)
			return
		}
		types[i].Columns = columns
	}
	return
}

// listTypeMetas returns the list of avaiable user types
func listTypeMetas(db *DB, schema, objName string) (d []PgUsertypeMetadata, err error) {

	err = db.Select(&d, `
WITH args AS (
    SELECT $1 AS schema_name,
            regexp_split_to_table ( $2, ', *' ) AS obj_name
)
SELECT n.nspname::text AS schema_name,
        pg_catalog.format_type ( t.oid, NULL ) AS obj_name,
        CASE
            WHEN t.typrelid != 0 THEN CAST ( 'tuple' AS pg_catalog.text )
            WHEN t.typlen < 0 THEN CAST ( 'var' AS pg_catalog.text )
            ELSE CAST ( t.typlen AS pg_catalog.text )
            END AS obj_type,
        coalesce ( pg_catalog.obj_description ( t.oid, 'pg_type' ), '' ) AS description
    FROM pg_catalog.pg_type t
    JOIN pg_catalog.pg_namespace n
        ON n.oid = t.typnamespace
    CROSS JOIN args
    WHERE ( t.typrelid = 0
            OR ( SELECT c.relkind = 'c'
                    FROM pg_catalog.pg_class c
                    WHERE c.oid = t.typrelid ) )
        AND NOT EXISTS (
                SELECT 1
                    FROM pg_catalog.pg_type el
                    WHERE el.oid = t.typelem
                    AND el.typarray = t.oid )
        AND n.nspname <> 'pg_catalog'
        AND n.nspname <> 'information_schema'
        AND n.nspname !~ '^pg_toast'
        AND ( n.nspname = args.schema_name
            OR args.schema_name = '' )
        AND ( pg_catalog.format_type ( t.oid, NULL ) = args.obj_name
            OR coalesce ( args.obj_name, '' ) = '' )
    ORDER BY n.nspname,
        pg_catalog.format_type ( t.oid, NULL )
`, schema, objName)
	return
}

// listTypeColumnMetas returns the metadata for the avaiable user type columns
func listTypeColumnMetas(db *DB, schema, objName string) (d []PgColumnMetadata, err error) {

	err = db.Select(&d, `
WITH args AS (
    SELECT $1 AS schema_name,
            $2 AS obj_name
),
cols AS (
    SELECT n.nspname::text AS schema_name,
            pg_catalog.format_type ( t.oid, NULL ) AS obj_name,
            a.attname::text AS column_name,
            pg_catalog.format_type ( a.atttypid, a.atttypmod ) AS data_type,
            a.attnotnull AS is_required,
            a.attnum AS ordinal_position,
            pg_catalog.col_description ( a.attrelid, a.attnum ) AS description
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_type t
            ON a.attrelid = t.typrelid
        JOIN pg_catalog.pg_namespace n
            ON ( n.oid = t.typnamespace )
        CROSS JOIN args
        WHERE a.attnum > 0
            AND NOT a.attisdropped
            AND n.nspname = args.schema_name
            AND pg_catalog.format_type ( t.oid, NULL ) = args.obj_name
)
SELECT cols.column_name,
        cols.data_type,
        cols.ordinal_position,
        cols.is_required,
        false AS is_pk,
        coalesce ( cols.description, '' ) AS description
    FROM cols
    ORDER BY cols.schema_name,
            cols.obj_name,
            cols.ordinal_position
`, schema, objName)
	return
}
