package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	m "github.com/gsiems/pg2go/meta"
	u "github.com/gsiems/pg2go/util"
)

type cArgs struct {
	genRels    bool
	genTypes   bool
	genFuncs   bool
	schemaName string
	objName    string
	appUser    string
	dbName     string
	dbHost     string
	dbUser     string
}

func main() {

	var args cArgs

	flag.BoolVar(&args.genRels, "r", false, "Generate structs for tables and views.")
	flag.BoolVar(&args.genTypes, "t", false, "Generate structs for user defined types.")
	flag.BoolVar(&args.genFuncs, "f", false, "Generate structs for result-set returning functions.")

	flag.StringVar(&args.schemaName, "s", "", "The database schema to generate structs for (defaults to all).")
	flag.StringVar(&args.objName, "o", "", "The name of the database object to generate a struct for (defaults to all).")
	flag.StringVar(&args.appUser, "u", "", "The name of the application user. If specified then only structs for those objects that the user has privileges for will be generated.")

	flag.StringVar(&args.dbName, "d", "", "The the database name to connect to.")
	flag.StringVar(&args.dbHost, "h", "", "The database host to connect to.")
	flag.StringVar(&args.dbUser, "U", "", "The database user to connect as.")

	flag.Parse()

	genHeader(args)

	if !args.genTypes && !args.genRels && !args.genFuncs {
		fmt.Println("No structure types specified. Select some combination of table, type, and function structures to generate.")
		flag.PrintDefaults()
	}

	if args.dbUser == "" || args.dbName == "" || args.dbHost == "" {
		fmt.Println("Insufficient connections parameters specified. Specify the user, host, and database to connect to.")
		flag.PrintDefaults()
	}

	connStr := fmt.Sprintf("user=%s dbname=%s host=%s", args.dbUser, args.dbName, args.dbHost)

	if args.genTypes {
		types, err := m.GetTypeMetas(connStr, args.schemaName, args.objName, args.appUser)
		if err != nil {
			log.Fatalf("FAILED! %q.\n", err)
		}
		genTypeStructs(types)
	}

	if args.genRels {
		tables, err := m.GetTableMetas(connStr, args.schemaName, args.objName, args.appUser)
		if err != nil {
			log.Fatalf("FAILED! %q.\n", err)
		}
		genTableStructs(tables)
	}

	if args.genFuncs {
		funcs, err := m.GetFunctionMetas(connStr, args.schemaName, args.objName, args.appUser)
		if err != nil {
			log.Fatalf("FAILED! %q.\n", err)
		}
		genFunctionStructs(funcs)
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
	fmt.Println("\tnull \"gopkg.in/guregu/null.v3\"")
	fmt.Println(")")
}

func genTypeStructs(d []m.PgUsertypeMetadata) {

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

		fmt.Print(getStanzas(f.Columns))

		fmt.Println("}")
	}
}

func genTableStructs(d []m.PgTableMetadata) {

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

		fmt.Print(getStanzas(f.Columns))

		fmt.Println("}")
	}
}

func genFunctionStructs(d []m.PgFunctionMetadata) {

	for _, f := range d {

		if len(f.ResultColumns) == 0 {
			continue
		}

		fmt.Println()
		fmt.Printf("// %s struct for the result set from the %s.%s function\n", f.StructName, f.SchemaName, f.ObjName)
		if f.Description != "" {
			fmt.Printf("// %s\n", strings.ReplaceAll(f.Description, "\n", "\n// "))
		}

		fmt.Printf("type %s struct {\n", f.StructName)

		fmt.Print(getStanzas(f.ResultColumns))

		fmt.Println("}")
	}
}

func getMaxLens(cols []m.PgColumnMetadata) (maxDbNameLen, maxVarNameLen, maxVarTypeLen int) {
	for _, col := range cols {
		goVarName := u.ToUpperCamelCase(col.ColumnName)
		maxDbNameLen = maxStringLen(col.ColumnName, maxDbNameLen)
		maxVarNameLen = maxStringLen(goVarName, maxVarNameLen)
		maxVarTypeLen = maxStringLen(u.ToNullVarType(col.DataType), maxVarTypeLen)
	}
	return
}

func getStanzas(cols []m.PgColumnMetadata) string {

	var ary []string
	maxDbNameLen, maxVarNameLen, maxVarTypeLen := getMaxLens(cols)

	for _, col := range cols {
		stanza := makeStanza(col, maxDbNameLen, maxVarNameLen, maxVarTypeLen)
		ary = append(ary, stanza)
	}
	return strings.Join(ary, "")
}

func makeStanza(col m.PgColumnMetadata, maxDbNameLen, maxVarNameLen, maxVarTypeLen int) string {

	var ary []string

	goVarName := u.ToUpperCamelCase(col.ColumnName)
	jsonName := u.ToLowerCamelCase(col.ColumnName)

	VarNameToken := u.Lpad(goVarName, maxVarNameLen+1)
	VarTypeToken := u.Lpad(u.ToNullVarType(col.DataType), maxVarTypeLen+1)
	JSONToken := u.Lpad("`json:\""+jsonName+"\"", maxVarNameLen+9)
	DbToken := u.Lpad("db:\""+col.ColumnName+"\"`", maxDbNameLen+6)

	ary = append(ary, "\t")
	ary = append(ary, VarNameToken)
	ary = append(ary, VarTypeToken)
	ary = append(ary, JSONToken)
	ary = append(ary, DbToken)
	ary = append(ary, " // [")
	ary = append(ary, col.DataType)
	ary = append(ary, "]")

	if col.IsPk {
		ary = append(ary, " [PK]")
	}
	if col.IsRequired {
		ary = append(ary, " [Not Null]")
	}

	if col.Description != "" {
		ary = append(ary, fmt.Sprintf(" %s", strings.ReplaceAll(col.Description, "\n", "\n//                                           ")))
	}
	ary = append(ary, "\n")

	return strings.Join(ary, "")
}

func maxStringLen(s string, sz int) int {
	if len(s) > sz {
		return len(s)
	}
	return sz
}
