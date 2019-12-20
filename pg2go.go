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
	useNullTypes bool
	packageName  string
	schemaName   string
	objName      string
	appUser      string
	dbName       string
	dbHost       string
	dbPort       string
	dbUser       string
}

func main() {

	var args cArgs

	flag.BoolVar(&args.useNullTypes, "n", false, "Use null datatypes in structures.")
	flag.StringVar(&args.packageName, "package", "main", "The package name (defaults to main).")

	flag.StringVar(&args.schemaName, "s", "", "The database schema to generate structs for (defaults to all).")
	flag.StringVar(&args.objName, "o", "", "The comma-separated list of the database objects to generate a structs for (defaults to all).")
	flag.StringVar(&args.appUser, "u", "", "The name of the application user. Only structs for those objects that the user has privileges for will be generated.")

	flag.StringVar(&args.dbName, "d", "", "The database name to connect to.")
	flag.StringVar(&args.dbHost, "h", "", "The database host to connect to.")
	flag.StringVar(&args.dbPort, "p", "5432", "The port to connect on.")
	flag.StringVar(&args.dbUser, "U", "", "The database user to connect as.")

	flag.Parse()

	if args.dbUser == "" || args.dbName == "" || args.dbHost == "" || args.appUser == "" {
		fmt.Println("Insufficient connections parameters specified.")
		flag.PrintDefaults()
	}

	connStr := fmt.Sprintf("user=%s dbname=%s host=%s port=%s", args.dbUser, args.dbName, args.dbHost, args.dbPort)

	dbPool, err := sql.Open("postgres", connStr)
	u.DieOnErrf("Expected database connection, got error %q.\n", err)
	defer dbPool.Close()

	err = dbPool.Ping()
	u.DieOnErrf("Expected database ping, got error %q.\n", err)

	types, err := m.GetTypeMetas(dbPool, args.schemaName, args.objName, args.appUser)
	u.DieOnErrf("FAILED! %q.\n", err)
	genTypeStructs(args, types)

	tables, err := m.GetTableMetas(dbPool, args.schemaName, args.objName, args.appUser)
	u.DieOnErrf("FAILED! %q.\n", err)
	genTableStructs(args, tables)

	funcs, err := m.GetFunctionMetas(dbPool, args.schemaName, args.objName, args.appUser)
	u.DieOnErrf("FAILED! %q.\n", err)
	genFunctionStructs(args, funcs)

}

func initCodeBuf(args cArgs) (cb *u.LineBuf) {

	cb = u.InitLineBuf()
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
	if args.useNullTypes {
		cb.Append("\t\"database/sql\"")
	} else {
		cb.Append("\t\"time\"")
	}

	cb.Append(")")
	cb.Append("")

	return cb
}

func genTypeStructs(args cArgs, d []m.PgUsertypeMetadata) {

	for _, f := range d {

		if len(f.Columns) == 0 {
			continue
		}

		cb := initCodeBuf(args)

		cb.Append(fmt.Sprintf("// %s struct for the %s.%s %s type", f.StructName, f.SchemaName, f.ObjName, f.ObjType))
		if f.Description != "" {
			cb.Append(fmt.Sprintf("// %s", strings.ReplaceAll(f.Description, "\n", "\n// ")))
		}
		cb.Append(fmt.Sprintf("type %s struct {", f.StructName))

		cb.Append(m.GetStructStanzas(args.useNullTypes, false, f.Columns))

		cb.Append("}")
		cb.Append("")

		u.WriteFile(args.packageName, f.StructName, cb)
	}
}

func genTableStructs(args cArgs, d []m.PgTableMetadata) {

	// If no app user was specified then we can potentially get dulplicate structures
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

		cb := initCodeBuf(args)

		cb.Append(fmt.Sprintf("// %s struct for the %s.%s %s", f.StructName, f.SchemaName, f.ObjName, f.ObjType))
		if f.Description != "" {
			cb.Append(fmt.Sprintf("// %s", strings.ReplaceAll(f.Description, "\n", "\n// ")))
		}
		cb.Append(fmt.Sprintf("type %s struct {", f.StructName))

		cb.Append(m.GetStructStanzas(args.useNullTypes, false, f.Columns))

		cb.Append("}")
		cb.Append("")

		u.WriteFile(args.packageName, f.StructName, cb)

	}
}

func genFunctionStructs(args cArgs, d []m.PgFunctionMetadata) {

	// If no app user was specified then we can potentially get
	// duplicate structures. It is also possible to have duplicated
	// structures if there is functional overloading in the database.
	seen := make(map[string]int)

	for _, f := range d {

		if len(f.ResultColumns) == 0 {
			continue
		}

		// ensure the structure has not been generated already
		_, ok := seen[f.StructName]
		if ok {
			continue
		}
		seen[f.StructName] = 1

		cb := initCodeBuf(args)

		cb.Append(fmt.Sprintf("// %s struct for the result set from the %s.%s function\n", f.StructName, f.SchemaName, f.ObjName))
		if f.Description != "" {
			cb.Append(fmt.Sprintf("// %s", strings.ReplaceAll(f.Description, "\n", "\n// ")))
		}

		cb.Append(fmt.Sprintf("type %s struct {", f.StructName))

		cb.Append(m.GetStructStanzas(args.useNullTypes, false, f.ResultColumns))

		cb.Append("}")
		cb.Append("")

		u.WriteFile(args.packageName, fmt.Sprintf("f%s", f.StructName), cb)

	}
}
