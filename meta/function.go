package meta

import (
	"fmt"
	"strings"

	_ "github.com/lib/pq"

	u "github.com/gsiems/pg2go/util"
)

// PgFunctionMetadata contains metadata for postgresql functions
type PgFunctionMetadata struct {
	SchemaName       string `db:"schema_name"`
	ObjName          string `db:"obj_name"`
	ResultTypes      string `db:"result_types"`
	ArgumentTypes    string `db:"argument_types"`
	Privs            string `db:"privs"`
	Description      string `db:"description"`
	StructName       string
	ResultType       string
	ResultColumns    []PgColumnMetadata
	CallingArguments []PgColumnMetadata
}

// GetFunctionMetas returns the metadata for the avaiable functions
func GetFunctionMetas(connStr, schema, objName, user string) (funcs []PgFunctionMetadata, err error) {

	db, errq := OpenDB(connStr)
	if errq != nil {
		err = fmt.Errorf("Expected connection, got error: %q", errq)
		return
	}
	defer db.CloseDB()

	funcs, errq = listFunctionMetas(db, schema, objName, user)
	if errq != nil {
		err = fmt.Errorf("Expected function metadata, got error: %q", errq)
		return
	}
	for i, f := range funcs {
		funcs[i].StructName = u.ToUpperCamelCase(f.ObjName)
		funcs[i].CallingArguments = getCallingArguments(f)

		if strings.HasPrefix(f.ResultTypes, "TABLE(") {
			funcs[i].ResultColumns = getResultColumns(f)
		} else {
			funcs[i].ResultType = f.ResultTypes
		}
	}
	return
}

func getCallingArguments(f PgFunctionMetadata) (c []PgColumnMetadata) {

	if f.ArgumentTypes != "" {
		parms := strings.Split(f.ArgumentTypes, ", ")
		for i, p := range parms {
			t := strings.SplitN(p, " ", 2)
			if len(t) == 2 {
				c = append(c, PgColumnMetadata{ColumnName: t[0], DataType: t[1], OrdinalPosition: i})
			}
		}
	}
	return
}

func getResultColumns(f PgFunctionMetadata) (c []PgColumnMetadata) {

	if strings.HasPrefix(f.ResultTypes, "TABLE(") {
		parms := strings.Split(strings.Replace(strings.Replace(f.ResultTypes, "TABLE(", "", 1), ")", "", 1), ", ")
		for i, p := range parms {
			t := strings.SplitN(p, " ", 2)
			if len(t) == 2 {
				c = append(c, PgColumnMetadata{ColumnName: t[0], DataType: t[1], OrdinalPosition: i})
			}
		}
	}
	return
}

// listFunctionMetas returns the metadata for the avaiable functions
func listFunctionMetas(db *DB, schema, objName, user string) (d []PgFunctionMetadata, err error) {
	err = db.Select(&d, `
WITH args AS (
    SELECT coalesce ( $1, '' ) AS schema_name,
            coalesce ( $2, '' ) AS obj_name,
            coalesce ( $3, '' ) AS username
),
obj AS (
    SELECT n.nspname::text AS schema_name,
            p.proname::text AS obj_name,
            pg_catalog.pg_get_function_result ( p.oid ) AS result_types,
            pg_catalog.pg_get_function_arguments ( p.oid ) AS argument_types,
            pg_catalog.obj_description(p.oid, 'pg_proc') AS description,
            unnest ( p.proacl ) AS acl
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n
            ON n.oid = p.pronamespace
        CROSS JOIN args
        WHERE NOT p.proisagg
            AND NOT p.proiswindow
            AND NOT p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype
            AND n.nspname <> 'pg_catalog'
            AND n.nspname <> 'information_schema'
            AND n.nspname !~ '^pg_toast'
            AND ( n.nspname = args.schema_name
                OR args.schema_name = '' )
            AND ( p.proname = args.obj_name
                OR args.obj_name = '' )
)
SELECT obj.schema_name,
        obj.obj_name,
        coalesce ( obj.result_types, '' ) AS result_types,
        coalesce ( obj.argument_types, '' ) AS argument_types,
        coalesce ( regexp_replace ( regexp_replace ( obj.acl::text, '^[^=]+=', '' ), '[/].+', '' ), '' ) AS privs,
        coalesce ( obj.description, '' ) AS description
    FROM obj
    CROSS JOIN args
    WHERE ( obj.acl::text LIKE args.username || '=%'
            OR args.username = '' )
    ORDER BY obj.schema_name,
        obj.obj_name,
        obj.argument_types
`, schema, objName, user)
	return
}