package meta

import (
	"database/sql"
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
	argTypes         string
	argModes         string
	argNames         string
	ResultColumns    []PgColumnMetadata
	CallingArguments []PgColumnMetadata
}

// GetFunctionMetas returns the metadata for the avaiable functions
func GetFunctionMetas(db *sql.DB, schema, objName, user string) (funcs []PgFunctionMetadata, err error) {

	funcs, errq := listFunctionMetas(db, schema, objName, user)
	if errq != nil {
		err = fmt.Errorf("Expected function metadata, got error: %q", errq)
		return
	}
	for i, f := range funcs {
		/*
			fmt.Println("\n-------------------------------------------------------------------")
			fmt.Printf("GetFunctionMetas: %q.%q\n", f.SchemaName, f.ObjName)
			fmt.Printf("    ArgumentTypes: %q\n", f.ArgumentTypes)
			fmt.Printf("    ResultTypes: %q\n", f.ResultTypes)
			fmt.Printf("    argTypes: %q\n", f.argTypes)
			fmt.Printf("    argModes: %q\n", f.argModes)
			fmt.Printf("    argNames: %q\n", f.argNames)
		*/
		funcs[i].StructName = u.ToUpperCamelCase(f.ObjName)

		if funcs[i].argTypes != "" {
			var fat []PgColumnMetadata
			var frt []PgColumnMetadata

			argtypes := strings.Split(funcs[i].argTypes, ",")
			argmodes := strings.Split(funcs[i].argModes, ",")
			argnames := strings.Split(funcs[i].argNames, ",")

			for j, argtype := range argtypes {

				c, errq := popTypeMeta(db, argtype)
				if errq != nil {
					err = fmt.Errorf("Expected function type metadata, got error: %q", errq)
					return
				}
				c.OrdinalPosition = j + 1
				c.ColumnName = argnames[j]

				if argmodes[j] == "i" {
					fat = append(fat, c)
				} else {
					frt = append(frt, c)
				}
				/*
					fmt.Println("        -------------------")
					fmt.Printf("        ArgName: %q\n", argnames[j])
					fmt.Printf("        ArgMode: %q\n", argmodes[j])
					fmt.Printf("        ArgType: %q\n", argtypes[j])
					fmt.Printf("        DataType: %q\n", c.DataType)
					fmt.Printf("        TypeName: %q\n", c.TypeName)
					fmt.Printf("        TypeCategory: %q\n", c.TypeCategory)
				*/
			}

			funcs[i].ResultColumns = frt
			funcs[i].CallingArguments = fat
		}
	}

	return
}

// listFunctionMetas returns the metadata for the avaiable functions
func listFunctionMetas(db *sql.DB, schema, objName, user string) (d []PgFunctionMetadata, err error) {

	var u struct {
		SchemaName    sql.NullString
		ObjName       sql.NullString
		ResultTypes   sql.NullString
		ArgumentTypes sql.NullString
		Privs         sql.NullString
		Description   sql.NullString
		argTypes      sql.NullString
		argModes      sql.NullString
		argNames      sql.NullString
	}

	q := `WITH args AS (
    SELECT $1 AS schema_name,
            regexp_split_to_table ( $2, ', *' ) AS obj_name,
            $3 AS username
),
proc AS (
    SELECT n.nspname::text AS schema_name,
            p.proname::text AS obj_name,
            pg_catalog.pg_get_function_result ( p.oid ) AS result_types,
            pg_catalog.pg_get_function_arguments ( p.oid ) AS argument_types,
            pg_catalog.obj_description(p.oid, 'pg_proc') AS description,
            p.proacl,
            CASE
                WHEN p.proallargtypes IS NOT NULL
                    THEN regexp_replace ( p.proallargtypes::text, '[{}]', '', 'g' )
                END AS all_arg_types,
            CASE
                WHEN p.proargmodes IS NOT NULL
                    THEN regexp_replace ( p.proargmodes::text, '[{}]', '', 'g' )
                END AS all_arg_modes,
            CASE
                WHEN p.proargnames IS NOT NULL
                    THEN regexp_replace ( p.proargnames::text, '[{}]', '', 'g' )
                END AS all_arg_names,
            CASE
                WHEN p.proargtypes IS NOT NULL AND p.proargtypes::text <> ''
                    THEN regexp_replace ( p.proargtypes::text, '[ ]+', ',', 'g' )
                END AS in_arg_types,
            CASE
                WHEN p.proargtypes IS NOT NULL AND p.proargtypes::text <> ''
                    THEN regexp_replace ( regexp_replace ( p.proargtypes::text, '[^ ]+', 'i', 'g' ), '[ ]+', ',', 'g' )
                END AS in_arg_modes,
            CASE
                WHEN p.prorettype IS NOT NULL AND p.prorettype::text <> ''
                    THEN p.prorettype::text
                END AS ret_arg_type,
            CASE
                WHEN t.typname IS NOT NULL AND t.typname::text <> ''
                    THEN t.typname::text
                END AS ret_arg_name
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n
            ON n.oid = p.pronamespace
        LEFT JOIN pg_catalog.pg_type t
            ON ( t.oid = p.prorettype )
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
                OR coalesce ( args.obj_name, '' ) = '' )
),
obj AS (
    SELECT schema_name,
            obj_name,
            result_types,
            argument_types,
            description,
            unnest ( proacl ) AS acl,
            CASE
                WHEN coalesce ( all_arg_types, '' ) <> '' THEN all_arg_types
                WHEN coalesce ( in_arg_types, '' ) <> '' AND coalesce ( ret_arg_type, '' ) <> '' THEN in_arg_types || ',' || ret_arg_type
                WHEN coalesce ( in_arg_types, '' ) <> '' THEN in_arg_types
                WHEN coalesce ( ret_arg_type, '' ) <> '' THEN ret_arg_type
                END AS arg_types,
            CASE
                WHEN coalesce ( all_arg_types, '' ) <> '' THEN all_arg_modes
                WHEN coalesce ( in_arg_types, '' ) <> '' AND coalesce ( ret_arg_type, '' ) <> '' THEN in_arg_modes || ',o'
                WHEN coalesce ( in_arg_types, '' ) <> '' THEN in_arg_modes
                WHEN coalesce ( ret_arg_type, '' ) <> '' THEN 'o'
                END AS arg_modes,
            CASE
                WHEN coalesce ( all_arg_types, '' ) <> '' THEN all_arg_names
                WHEN coalesce ( in_arg_types, '' ) <> '' AND coalesce ( ret_arg_type, '' ) <> '' THEN all_arg_names || ',' || ret_arg_name
                WHEN coalesce ( in_arg_types, '' ) <> '' THEN all_arg_names
                WHEN coalesce ( ret_arg_type, '' ) <> '' THEN ret_arg_name
                END AS arg_names
        FROM proc
)
SELECT obj.schema_name,
        obj.obj_name,
        coalesce ( obj.result_types, '' ) AS result_types,
        coalesce ( obj.argument_types, '' ) AS argument_types,
        coalesce ( regexp_replace ( regexp_replace ( obj.acl::text, '^[^=]+=', '' ), '[/].+', '' ), '' ) AS privs,
        coalesce ( obj.description, '' ) AS description,
        arg_types,
        arg_modes,
        arg_names
    FROM obj
    CROSS JOIN args
    WHERE obj.acl::text LIKE args.username || '=%'
    ORDER BY obj.schema_name,
        obj.obj_name,
        obj.argument_types
`

	//prorettype (oid)	proargtypes (oidvector)

	rows, err := db.Query(q, schema, objName, user)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {

		err = rows.Scan(&u.SchemaName,
			&u.ObjName,
			&u.ResultTypes,
			&u.ArgumentTypes,
			&u.Privs,
			&u.Description,
			&u.argTypes,
			&u.argModes,
			&u.argNames,
		)
		if err != nil {
			return
		}

		d = append(d, PgFunctionMetadata{
			SchemaName:    u.SchemaName.String,
			ObjName:       u.ObjName.String,
			ResultTypes:   u.ResultTypes.String,
			ArgumentTypes: u.ArgumentTypes.String,
			Privs:         u.Privs.String,
			Description:   u.Description.String,
			argTypes:      u.argTypes.String,
			argModes:      u.argModes.String,
			argNames:      u.argNames.String,
		})
	}

	return
}

// popTypeMeta returns the metadata for the specified type
func popTypeMeta(db *sql.DB, arg_type string) (u PgColumnMetadata, err error) {

	q := `
SELECT pg_catalog.format_type ( oid, null ) AS data_type,
        typname AS type_name,
        typcategory AS type_category
    FROM pg_catalog.pg_type
    WHERE oid::text = $1
`

	rows, err := db.Query(q, arg_type)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {

		err = rows.Scan(&u.DataType,
			&u.TypeName,
			&u.TypeCategory,
		)

	}

	return
}
