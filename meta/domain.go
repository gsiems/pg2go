package meta

import (
	"database/sql"

	_ "github.com/lib/pq"
)

// PgColumnMetadata contains metadata for domains
type PgDomainMetadata struct {
	SchemaName   string `db:"schema_name"`
	ObjName      string `db:"obj_name"`
	DataType     string `db:"data_type"`
	TypeName     string `db:"type_name"`
	TypeCategory string `db:"type_category"`
	IsRequired   bool   `db:"is_required"`
	Description  string `db:"description"`
}

// GetDomainMetas returns the metadata for the avaiable domains
func GetDomainMetas(db *sql.DB, schema, objName, user string) (d []PgDomainMetadata, err error) {

	var u PgDomainMetadata

	q := `
WITH args AS (
    SELECT $1 AS schema_name,
            regexp_split_to_table ( $2, ', *' ) AS obj_name
)
SELECT n.nspname::text AS schema_name,
        t.typname::text AS obj_name,
        pg_catalog.format_type ( t.typbasetype, t.typtypmod ) AS data_type,
        ltrim ( tc.typname, '_' ) AS type_name,
        tc.typcategory AS type_category,
        t.typnotnull AS is_required,
        coalesce ( d.description, '' ) AS description
    FROM pg_catalog.pg_type t
    JOIN pg_catalog.pg_namespace n
        ON ( n.oid = t.typnamespace )
    JOIN pg_catalog.pg_type tc
        ON ( tc.oid = t.typbasetype )
    LEFT JOIN pg_catalog.pg_description d
        ON ( d.classoid = t.tableoid
            AND d.objoid = t.oid
            AND d.objsubid = 0 )
    WHERE t.typtype = 'd'
        AND n.nspname <> 'pg_catalog'
        AND n.nspname <> 'information_schema'
        AND n.nspname !~ '^pg_toast'
        AND ( n.nspname = args.schema_name
            OR args.schema_name = '' )
        AND ( obj.obj_name = args.obj_name
            OR coalesce ( args.obj_name, '' ) = '' )
`

	rows, err := db.Query(q, schema, objName)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {

		err = rows.Scan(&u.SchemaName,
			&u.ObjName,
			&u.DataType,
			&u.TypeName,
			&u.TypeCategory,
			&u.IsRequired,
			&u.Description,
		)
		if err != nil {
			return
		}

		d = append(d, u)
	}

	return
}
