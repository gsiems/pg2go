package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	_ "github.com/lib/pq"

	m "github.com/gsiems/pg2go/meta"
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
	if err != nil {
		log.Fatalf("FAILED! Expected database connection, got error: %q.\n", err)
	}
	defer dbPool.Close()

	err = dbPool.Ping()
	if err != nil {
		log.Fatalf("FAILED! Expected database ping, got error: %q.\n", err)
	}

	genHeader(args)

	types, err := m.GetTypeMetas(dbPool, args.schemaName, args.objName, args.appUser)
	if err != nil {
		log.Fatalf("FAILED! %q.\n", err)
	}
	genTypeStructs(args, types)

	tables, err := m.GetTableMetas(dbPool, args.schemaName, args.objName, args.appUser)
	if err != nil {
		log.Fatalf("FAILED! %q.\n", err)
	}
	genTableStructs(args, tables)

	funcs, err := m.GetFunctionMetas(dbPool, args.schemaName, args.objName, args.appUser)
	if err != nil {
		log.Fatalf("FAILED! %q.\n", err)
	}
	genFunctionStructs(args, funcs)

}

func genHeader(args cArgs) {
	fmt.Printf("package %s\n", args.packageName)
	fmt.Println()

	fmt.Println("// Postgresql structs generated for the following:")
	fmt.Printf("// Host: %s\n", args.dbHost)
	fmt.Printf("// Database: %s\n", args.dbName)
	if args.schemaName != "" {
		fmt.Printf("// Schema: %s\n", args.schemaName)
	}
	if args.objName != "" {
		fmt.Printf("// Object Name: %s\n", args.objName)
	}
	if args.appUser != "" {
		fmt.Printf("// App user: %s\n", args.appUser)
	}

	fmt.Println()
	fmt.Println("import (")
	if args.useNullTypes {
		fmt.Println("\t\"database/sql\"")
	} else {
		fmt.Println("\t\"time\"")
	}
	fmt.Println(")")

}

func genTypeStructs(args cArgs, d []m.PgUsertypeMetadata) {

	for _, f := range d {

		if len(f.Columns) == 0 {
			continue
		}

		fmt.Println()
		fmt.Printf("// %s struct for the %s.%s %s type\n", f.StructName, f.SchemaName, f.ObjName, f.ObjType)
		if f.Description != "" {
			fmt.Printf("// %s\n", strings.ReplaceAll(f.Description, "\n", "\n// "))
		}
		fmt.Printf("type %s struct {\n", f.StructName)

		fmt.Print(m.GetStructStanzas(args.useNullTypes, false, f.Columns))

		fmt.Println("}")
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

		fmt.Println()
		fmt.Printf("// %s struct for the %s.%s %s\n", f.StructName, f.SchemaName, f.ObjName, f.ObjType)
		if f.Description != "" {
			fmt.Printf("// %s\n", strings.ReplaceAll(f.Description, "\n", "\n// "))
		}
		fmt.Printf("type %s struct {\n", f.StructName)

		fmt.Print(m.GetStructStanzas(args.useNullTypes, false, f.Columns))

		fmt.Println("}")
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

		fmt.Println()
		fmt.Printf("// %s struct for the result set from the %s.%s function\n", f.StructName, f.SchemaName, f.ObjName)
		if f.Description != "" {
			fmt.Printf("// %s\n", strings.ReplaceAll(f.Description, "\n", "\n// "))
		}

		fmt.Printf("type %s struct {\n", f.StructName)

		fmt.Print(m.GetStructStanzas(args.useNullTypes, false, f.ResultColumns))

		fmt.Println("}")
	}
}
