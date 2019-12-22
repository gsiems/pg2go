package meta

import (
	"fmt"
	"strings"

	u "github.com/gsiems/pg2go/util"
	"github.com/jmoiron/sqlx"
)

// DB contains an sqlx database connection
type DB struct {
	*sqlx.DB
}

// OpenDB opens a database connection and returns a DB reference.
func OpenDB(dsn string) (*DB, error) {
	db, err := sqlx.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return &DB{db}, db.Ping()
}

// CloseDB closes a DB reference.
func (db *DB) CloseDB() error {
	return db.DB.Close()
}

func GetStructStanzas(noNulls, internal bool, cols []PgColumnMetadata) string {

	var ary []string
	maxDbNameLen, maxVarNameLen, maxVarTypeLen := getMaxLens(noNulls, cols)

	for _, col := range cols {
		if internal {
			stanza := makeQueryStanza(noNulls, col, maxDbNameLen, maxVarNameLen, maxVarTypeLen)
			ary = append(ary, stanza)
		} else {
			stanza := makePublicStanza(noNulls, col, maxDbNameLen, maxVarNameLen, maxVarTypeLen)
			ary = append(ary, stanza)
		}
	}
	return strings.Join(ary, "\n")
}

func makePublicStanza(noNulls bool, col PgColumnMetadata, maxDbNameLen, maxVarNameLen, maxVarTypeLen int) string {

	var ary []string

	goVarName := u.ToUpperCamelCase(col.ColumnName)
	jsonName := u.ToLowerCamelCase(col.ColumnName)

	VarNameToken := u.Lpad(goVarName, maxVarNameLen+1)
	VarTypeToken := ""
	if noNulls {
		VarTypeToken = u.Lpad(u.ToGoVarType(col.DataType), maxVarTypeLen+1)
	} else {
		VarTypeToken = u.Lpad(u.ToNullVarType(col.DataType), maxVarTypeLen+1)
	}

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

	return strings.Join(ary, "")
}

func makeQueryStanza(noNulls bool, col PgColumnMetadata, maxDbNameLen, maxVarNameLen, maxVarTypeLen int) string {

	var ary []string

	goVarName := u.ToUpperCamelCase(col.ColumnName)

	VarNameToken := u.Lpad(goVarName, maxVarNameLen+1)
	VarTypeToken := u.Lpad(u.ToNullVarType(col.DataType), maxVarTypeLen+1)

	ary = append(ary, "\t\t")
	ary = append(ary, VarNameToken)
	ary = append(ary, VarTypeToken)

	return strings.Join(ary, "")
}

func getMaxLens(noNulls bool, cols []PgColumnMetadata) (maxDbNameLen, maxVarNameLen, maxVarTypeLen int) {
	for _, col := range cols {
		goVarName := u.ToUpperCamelCase(col.ColumnName)
		maxDbNameLen = maxStringLen(col.ColumnName, maxDbNameLen)
		maxVarNameLen = maxStringLen(goVarName, maxVarNameLen)

		if noNulls {
			maxVarTypeLen = maxStringLen(u.ToGoVarType(col.DataType), maxVarTypeLen)
		} else {
			maxVarTypeLen = maxStringLen(u.ToNullVarType(col.DataType), maxVarTypeLen)
		}

	}
	return
}

func maxStringLen(s string, sz int) int {
	if len(s) > sz {
		return len(s)
	}
	return sz
}
