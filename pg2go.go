package main

import (
	"database/sql"
	"flag"
	"fmt"
	"strings"

	_ "github.com/lib/pq"

	m "github.com/gsiems/pg2go/meta"
	u "github.com/gsiems/pg2go/util"
)

type cArgs struct {
	packageName string
	schemaName  string
	objName     string
	appUser     string
	dbName      string
	dbHost      string
	dbPort      int
	dbUser      string
	help        bool
}

func main() {

	var args cArgs

	flag.StringVar(&args.packageName, "package", "main", "The package name (defaults to main).")

	flag.StringVar(&args.schemaName, "schema", "", "The database schema to generate structs for (defaults to all).")
	flag.StringVar(&args.objName, "objects", "", "The comma-separated list of the database objects to generate a structs for (defaults to all).")
	flag.StringVar(&args.appUser, "app-user", "", "The name of the application user (required). Only code for those objects that this user has privileges for will be generated.")

	flag.StringVar(&args.dbName, "database", "", "The name of the database to connect to (required).")
	flag.StringVar(&args.dbHost, "host", "localhost", "The database host to connect to.")
	flag.IntVar(&args.dbPort, "port", 5432, "The port to connect to.")
	flag.StringVar(&args.dbUser, "U", "", "The database user to connect as when generating code (required).")

	flag.Parse()

	if args.help {
		flag.PrintDefaults()
	}

	if args.dbUser == "" || args.dbName == "" || args.dbHost == "" || args.appUser == "" {
		fmt.Println("Insufficient connections parameters specified.")
		flag.PrintDefaults()
	}

	connStr := fmt.Sprintf("user=%s dbname=%s host=%s port=%d", args.dbUser, args.dbName, args.dbHost, args.dbPort)

	dbPool, err := sql.Open("postgres", connStr)
	u.DieOnErrf("Expected database connection, got error %q.\n", err)
	defer dbPool.Close()

	err = dbPool.Ping()
	u.DieOnErrf("Expected database ping, got error %q.\n", err)

	types, err := m.GetTypeMetas(dbPool, args.schemaName, args.objName, args.appUser)
	u.DieOnErrf("FAILED! %q.\n", err)
	err = genTypeCode(args, types)
	u.DieOnErrf("FAILED! %q.\n", err)

	tables, err := m.GetTableMetas(dbPool, args.schemaName, args.objName, args.appUser)
	u.DieOnErrf("FAILED! %q.\n", err)
	err = genTableCode(args, tables)
	u.DieOnErrf("FAILED! %q.\n", err)

	funcs, err := m.GetFunctionMetas(dbPool, args.schemaName, args.objName, args.appUser)
	u.DieOnErrf("FAILED! %q.\n", err)
	err = genFunctionCode(args, funcs)
	u.DieOnErrf("FAILED! %q.\n", err)

}

func genTypeCode(args cArgs, d []m.PgUsertypeMetadata) (err error) {

	/*
		If no schema was specified in the calling args then we can
		potentially get duplicate structures. We *could* join the
		schema name and type name for creating the structure name
		however there is the issue of not fully qualifying references
		to the type in the Postgresql code (either because it is in the
		same schema or is in the search_path)-- this could be mitigated
		by linking by type OIDs rather than type names...
	*/
	seen := make(map[string]int)

	for _, f := range d {

		if len(f.Columns) == 0 {
			continue
		}

		// ensure the structure has not been generated already
		_, ok := seen[f.StructName]
		if ok {
			continue
		}
		seen[f.StructName] = 1

		cb := u.NewLineBuf()

		appendHeader(args, cb)

		errq := genTypeStruct(args, f, cb)
		if errq != nil {
			fmt.Printf("Failed to generate code for type %q.%q\n", f.SchemaName, f.ObjName)
			continue
		}

		u.WriteFile(args.packageName, f.StructName, cb)
	}

	return
}

func genTableCode(args cArgs, d []m.PgTableMetadata) (err error) {

	/*
		If no schema was specified in the calling args then we can
		potentially get duplicate structures. We *could* join the
		schema name and table name for creating the structure name
		however there is the issue of not fully qualifying references
		to the type in the Postgresql code (either because it is in the
		same schema or is in the search_path)
	*/
	seen := make(map[string]int)

	for _, f := range d {

		if len(f.Columns) == 0 {
			continue
		}

		// ensure the structure has not been generated already
		_, ok := seen[f.StructName]
		if ok {
			continue
		}
		seen[f.StructName] = 1

		cb := u.NewLineBuf()

		appendHeader(args, cb)

		errq := genTableStruct(args, f, cb)
		if errq != nil {
			fmt.Printf("Failed to generate code for table %q.%q\n", f.SchemaName, f.ObjName)
			continue
		}

		/*
		   a -> insert
		   r -> select
		   w -> update
		   d -> delete
		*/

		//err = genTableSelectList(args, f, cb)
		//if err != nil {
		//	return
		//}

		u.WriteFile(args.packageName, f.StructName, cb)
	}
	return
}

func genFunctionCode(args cArgs, d []m.PgFunctionMetadata) (err error) {
	/*
		If no schema was specified in the calling args then we can
		potentially get duplicate structures. We *could* join the
		schema name and function name for creating the structure name
		however there is the issue of not fully qualifying references
		to the type in the Postgresql code (either because it is in the
		same schema or is in the search_path). It is also possible to
		have duplicated structures if there is functional overloading
		in the database... Another problem is if a function returns a
		TABLE that matches an exiting table or view as it is unknown
		how to cleanly/automatically recognize that the table/view
		struct is the same as the function struct.
	*/
	seen := make(map[string]int)

	for _, f := range d {

		// Functions with zero or one return arguments don't require a struct
		if len(f.ResultColumns) < 2 {
			continue
		}

		// ensure the structure has not been generated already
		_, ok := seen[f.StructName]
		if ok {
			continue
		}
		seen[f.StructName] = 1

		cb := u.NewLineBuf()

		appendHeader(args, cb)

		errq := genFunctionStruct(args, f, cb)
		if errq != nil {
			fmt.Printf("Failed to generate code for function %q.%q\n", f.SchemaName, f.ObjName)
			continue
		}

		u.WriteFile(args.packageName, fmt.Sprintf("f%s", f.StructName), cb)
	}
	return
}

func appendHeader(args cArgs, cb *u.LineBuf) {

	cb.Append(fmt.Sprintf("package %s", args.packageName))
	cb.Append("")

	cb.Append(fmt.Sprintf("// Postgresql structs generated for the following:"))
	cb.Append(fmt.Sprintf("// Host: %s", args.dbHost))
	cb.Append(fmt.Sprintf("// Database: %s", args.dbName))
	if args.schemaName != "" {
		cb.Append(fmt.Sprintf("// Schema: %s", args.schemaName))
	}
	if args.objName != "" {
		cb.Append(fmt.Sprintf("// Object Name: %s", args.objName))
	}
	if args.appUser != "" {
		cb.Append(fmt.Sprintf("// App user: %s", args.appUser))
	}

	cb.Append("")
	cb.Append("import (")
	cb.Append("\t\"database/sql\"")
	cb.Append("\t\"time\"")
	cb.Append("")
	cb.Append("\t\"github.com/jackc/pgtype\"")
	cb.Append("\t_ \"github.com/lib/pq\"")
	cb.Append(")")
	cb.Append("")

}

func genTypeStruct(args cArgs, f m.PgUsertypeMetadata, cb *u.LineBuf) (err error) {

	var stanza string
	stanza, err = m.GetStructStanzas(f.Columns)
	if err != nil {
		return
	}

	cb.Append(fmt.Sprintf("// %s struct for the %s.%s %s type", f.StructName, f.SchemaName, f.ObjName, f.ObjType))
	if f.Description != "" {
		cb.Append(fmt.Sprintf("// %s", strings.ReplaceAll(f.Description, "\n", "\n// ")))
	}
	cb.Append(fmt.Sprintf("type %s struct {", f.StructName))
	cb.Append(stanza)
	cb.Append("}")
	cb.Append("")
	return
}

func genTableStruct(args cArgs, f m.PgTableMetadata, cb *u.LineBuf) (err error) {
	var stanza string
	stanza, err = m.GetStructStanzas(f.Columns)
	if err != nil {
		return
	}

	cb.Append(fmt.Sprintf("// %s struct for the %s.%s %s", f.StructName, f.SchemaName, f.ObjName, f.ObjType))
	if f.Description != "" {
		cb.Append(fmt.Sprintf("// %s", strings.ReplaceAll(f.Description, "\n", "\n// ")))
	}
	cb.Append(fmt.Sprintf("type %s struct {", f.StructName))
	cb.Append(stanza)
	cb.Append("}")
	cb.Append("")
	return
}

func genFunctionStruct(args cArgs, f m.PgFunctionMetadata, cb *u.LineBuf) (err error) {

	var stanza string
	stanza, err = m.GetStructStanzas(f.ResultColumns)
	if err != nil {
		return
	}

	cb.Append(fmt.Sprintf("// %s struct for the result set from the %s.%s function", f.StructName, f.SchemaName, f.ObjName))
	if f.Description != "" {
		cb.Append(fmt.Sprintf("// %s", strings.ReplaceAll(f.Description, "\n", "\n// ")))
	}
	cb.Append(fmt.Sprintf("type %s struct {", f.StructName))
	cb.Append(stanza)
	cb.Append("}")
	cb.Append("")

	return
}
