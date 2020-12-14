package meta

import (
	"database/sql"
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
func GetTypeMetas(db *sql.DB, schema, objName, user string, pgVersion int) (types []PgUsertypeMetadata, err error) {

	errq := mapOidToType(db)
	if errq != nil {
		err = fmt.Errorf("Expected oid to type mapping, got error: %q", errq)
		return
	}

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
func listTypeMetas(db *sql.DB, schema, objName string) (d []PgUsertypeMetadata, err error) {

	var u PgUsertypeMetadata

	q := `WITH args AS (
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
    WHERE t.typtype = 'c'
        AND ( t.typrelid = 0
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
`

	rows, err := db.Query(q, schema, objName)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {

		err = rows.Scan(&u.SchemaName,
			&u.ObjName,
			&u.ObjType,
			&u.Description,
		)
		if err != nil {
			return
		}

		d = append(d, u)
	}

	return
}

// listTypeColumnMetas returns the metadata for the avaiable user type columns
func listTypeColumnMetas(db *sql.DB, schema, objName string) (d []PgColumnMetadata, err error) {

	var u PgColumnMetadata

	q := `
WITH args AS (
    SELECT $1 AS schema_name,
            $2 AS obj_name
),
cols AS (
    SELECT n.nspname::text AS schema_name,
            pg_catalog.format_type ( tt.oid, NULL ) AS obj_name,
            a.attname::text AS column_name,
            pg_catalog.format_type ( a.atttypid, a.atttypmod ) AS data_type,
            tc.typname AS type_name,
            tc.typcategory AS type_category,
            a.attnotnull AS is_required,
            a.attnum AS ordinal_position,
            pg_catalog.col_description ( a.attrelid, a.attnum ) AS description
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_type tt
            ON a.attrelid = tt.typrelid
        JOIN pg_catalog.pg_type tc
            ON a.atttypid = tc.oid
        JOIN pg_catalog.pg_namespace n
            ON ( n.oid = tt.typnamespace )
        CROSS JOIN args
        WHERE tt.typtype = 'c'
            AND a.attnum > 0
            AND NOT a.attisdropped
            AND n.nspname = args.schema_name
            AND pg_catalog.format_type ( tt.oid, NULL ) = args.obj_name
)
SELECT cols.column_name,
        cols.data_type,
        cols.type_name,
        cols.type_category,
        cols.ordinal_position,
        cols.is_required,
        false AS is_pk,
        coalesce ( cols.description, '' ) AS description
    FROM cols
    ORDER BY cols.schema_name,
            cols.obj_name,
            cols.ordinal_position
`

	rows, err := db.Query(q, schema, objName)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {

		err = rows.Scan(&u.ColumnName,
			&u.DataType,
			&u.TypeName,
			&u.TypeCategory,
			&u.OrdinalPosition,
			&u.IsRequired,
			&u.IsPk,
			&u.Description,
		)
		if err != nil {
			return
		}

		d = append(d, u)
	}

	return
}

// mapOidToType
func mapOidToType(db *sql.DB) (err error) {

	q := `
SELECT t.oid,
        n.nspname::text AS schema_name,
        t.typname::text AS type_name,
        coalesce ( bt.oid, 0 ) AS base_oid,
        coalesce ( bt.typname::text, '' ) AS base_type_name,
        t.typtype AS type_type,
        t.typcategory AS type_category
    FROM pg_catalog.pg_type t
    JOIN pg_catalog.pg_namespace n
        ON n.oid = t.typnamespace
    LEFT JOIN pg_catalog.pg_type bt
        ON ( bt.oid = t.typbasetype )
    WHERE 1 = 1
        AND n.nspname <> 'information_schema'
        AND n.nspname !~ '^pg_toast'
        AND t.typtype NOT IN ( 'p' )
        AND NOT ( t.typtype = 'c'
            AND n.nspname = 'pg_catalog' )
`

	rows, err := db.Query(q)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var u pgType

		err = rows.Scan(&u.oid,
			&u.schemaName,
			&u.typeName,
			&u.baseOid,
			&u.baseTypeName,
			&u.typeType,
			&u.typeCategory,
		)
		if err != nil {
			return
		}

		addOidToType(u.oid, &u)
	}

	return
}
