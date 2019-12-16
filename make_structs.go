package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	m "github.com/gsiems/pg2go/meta"
)

type cArgs struct {
	genRels      bool
	genTypes     bool
	genFuncs     bool
	useNullTypes bool
	schemaName   string
	objName      string
	appUser      string
	dbName       string
	dbHost       string
	dbUser       string
}

func main() {

	var args cArgs

	flag.BoolVar(&args.genRels, "r", false, "Generate structs for tables and views.")
	flag.BoolVar(&args.genTypes, "t", false, "Generate structs for user defined types.")
	flag.BoolVar(&args.genFuncs, "f", false, "Generate structs for result-set returning functions.")

	flag.BoolVar(&args.useNullTypes, "n", false, "Use null datatypes in structures.")

	flag.StringVar(&args.schemaName, "s", "", "The database schema to generate structs for (defaults to all).")
	flag.StringVar(&args.objName, "o", "", "The name of the database object to generate a struct for (defaults to all).")
	flag.StringVar(&args.appUser, "u", "", "The name of the application user. If specified then only structs for those objects that the user has privileges for will be generated.")

	flag.StringVar(&args.dbName, "d", "", "The the database name to connect to.")
	flag.StringVar(&args.dbHost, "h", "", "The database host to connect to.")
	flag.StringVar(&args.dbUser, "U", "", "The database user to connect as.")

	flag.Parse()

	if !args.genTypes && !args.genRels && !args.genFuncs {
		fmt.Println("No structure types specified. Select some combination of table, type, and function structures to generate.")
	}

	if args.dbUser == "" || args.dbName == "" || args.dbHost == "" {
		fmt.Println("Insufficient connections parameters specified. Specify the user, host, and database to connect to.")
	}

	if (!args.genTypes && !args.genRels && !args.genFuncs) || args.dbUser == "" || args.dbName == "" || args.dbHost == "" {
		flag.PrintDefaults()
	}

	connStr := fmt.Sprintf("user=%s dbname=%s host=%s", args.dbUser, args.dbName, args.dbHost)

	genHeader(args)

	if args.genTypes {
		types, err := m.GetTypeMetas(connStr, args.schemaName, args.objName, args.appUser)
		if err != nil {
			log.Fatalf("FAILED! %q.\n", err)
		}
		genTypeStructs(args, types)
	}

	if args.genRels {
		tables, err := m.GetTableMetas(connStr, args.schemaName, args.objName, args.appUser)
		if err != nil {
			log.Fatalf("FAILED! %q.\n", err)
		}
		genTableStructs(args, tables)
	}

	if args.genFuncs {
		funcs, err := m.GetFunctionMetas(connStr, args.schemaName, args.objName, args.appUser)
		if err != nil {
			log.Fatalf("FAILED! %q.\n", err)
		}
		genFunctionStructs(args, funcs)
	}
}

func genHeader(args cArgs) {
	fmt.Println("package main")
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
	fmt.Print("// Generated structs for: ")
	var ary []string

	if args.genTypes {
		ary = append(ary, "user defined types")
	}
	if args.genRels {
		ary = append(ary, "tables/views")
	}
	if args.genFuncs {
		ary = append(ary, "functions")
	}
	fmt.Printf("%s\n", strings.Join(ary, ", "))

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

		fmt.Print(m.GetStructStanzas(args.useNullTypes, f.Columns))

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

		fmt.Print(m.GetStructStanzas(args.useNullTypes, f.Columns))

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

		fmt.Print(m.GetStructStanzas(args.useNullTypes, f.ResultColumns))

		fmt.Println("}")
	}
}
