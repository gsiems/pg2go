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
	noNulls     bool
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

	flag.BoolVar(&args.noNulls, "no-nulls", false, "Use only go datatypes in structures.")
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
	genTypeStructs(args, types)

	tables, err := m.GetTableMetas(dbPool, args.schemaName, args.objName, args.appUser)
	u.DieOnErrf("FAILED! %q.\n", err)
	genTableCode(args, tables)

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
	cb.Append("\t\"database/sql\"")
	cb.Append("\t\"time\"")
	cb.Append("")
	cb.Append("\t_ \"github.com/lib/pq\"")
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

		cb.Append(m.GetStructStanzas(args.noNulls, false, f.Columns))

		cb.Append("}")
		cb.Append("")

		u.WriteFile(args.packageName, f.StructName, cb)
	}
}

func genTableCode(args cArgs, d []m.PgTableMetadata) {

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

		genTableStruct(args, f, cb)

		/*
		   a -> insert
		   r -> select
		   w -> update
		   d -> delete
		*/

		genTableSelectList(args, f, cb)

		u.WriteFile(args.packageName, f.StructName, cb)
	}
}

func genTableSelectList(args cArgs, f m.PgTableMetadata, cb *u.LineBuf) {

	goFuncName := fmt.Sprintf("List%s", f.StructName)

	cb.Append("")
	cb.Append(fmt.Sprintf("// %s returns the data from the %s.%s %s", goFuncName, f.SchemaName, f.ObjName, f.ObjType))
	cb.Append(fmt.Sprintf("func (db *DB) %s() (d []%s, err error) {", goFuncName, f.StructName))

	if args.noNulls {
		cb.Append("\tvar u []struct {")
		cb.Append(m.GetStructStanzas(args.noNulls, true, f.Columns))
		cb.Append("\t}")

		cb.Append("\terr = db.Select(&u, `")

		var ary []string
		for _, col := range f.Columns {
			ary = append(ary, col.ColumnName)
		}
		cb.Append(fmt.Sprintf("SELECT %s", strings.Join(ary, ",\n        ")))
		cb.Append(fmt.Sprintf("    FROM %s.%s`,", f.SchemaName, f.ObjName))
		cb.Append("\t)")

		cb.Append("\tfor _, rec := range u {")
		cb.Append(fmt.Sprintf("\t\td = append(d, %s{", f.StructName))

		for _, col := range f.Columns {
			goVarName := u.ToUpperCamelCase(col.ColumnName)
			strucVarType := u.ToIntVarType(col.DataType)
			cb.Append(fmt.Sprintf("\t\t\t%s: rec.%s.%s,", goVarName, goVarName, strucVarType))
		}

		cb.Append("\t\t})")
		cb.Append("\t}")
		cb.Append("\treturn")
		cb.Append("}")
		cb.Append("")

	} else {
		cb.Append("\terr = db.Select(&d, `")

		var ary []string
		for _, col := range f.Columns {
			ary = append(ary, col.ColumnName)
		}
		cb.Append(fmt.Sprintf("SELECT %s", strings.Join(ary, ",\n        ")))
		cb.Append(fmt.Sprintf("    FROM %s.%s`,", f.SchemaName, f.ObjName))
		cb.Append("\t)")
		cb.Append("\treturn")
		cb.Append("}")
		cb.Append("")
	}

	return
}

func genTableStruct(args cArgs, f m.PgTableMetadata, cb *u.LineBuf) {

	cb.Append(fmt.Sprintf("// %s struct for the %s.%s %s", f.StructName, f.SchemaName, f.ObjName, f.ObjType))
	if f.Description != "" {
		cb.Append(fmt.Sprintf("// %s", strings.ReplaceAll(f.Description, "\n", "\n// ")))
	}
	cb.Append(fmt.Sprintf("type %s struct {", f.StructName))

	cb.Append(m.GetStructStanzas(args.noNulls, false, f.Columns))

	cb.Append("}")
	cb.Append("")

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

		cb.Append(m.GetStructStanzas(args.noNulls, false, f.ResultColumns))

		cb.Append("}")
		cb.Append("")

		u.WriteFile(args.packageName, fmt.Sprintf("f%s", f.StructName), cb)

	}
}
