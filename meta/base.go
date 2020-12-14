package meta

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"

	u "github.com/gsiems/pg2go/util"
)

// DB contains an database/sql connection
type DB struct {
	*sql.DB
}

// OpenDB opens a database connection and returns a DB reference.
func OpenDB(dsn string) (*DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return &DB{db}, db.Ping()
}

// CloseDB closes a DB reference.
func (db *DB) CloseDB() error {
	return db.DB.Close()
}

func DbVersion(db *sql.DB) (v int, err error) {

	rows, err := db.Query("SELECT current_setting('server_version_num')::int")
	if err != nil {
		return
	}
	defer rows.Close()

	rows.Next()
	err = rows.Scan(&v)
	return v, err
}


func GetStructStanzas(cols []PgColumnMetadata) (s string, err error) {

	var ary []string
	maxDbNameLen, maxVarNameLen, maxVarTypeLen, err := getMaxLens(cols)
	if err != nil {
		return
	}

	var stanza string
	for _, col := range cols {
		stanza, err = makeStructStanza(col, maxDbNameLen, maxVarNameLen, maxVarTypeLen)
		if err != nil {
			return
		}
		ary = append(ary, stanza)
	}
	s = strings.Join(ary, "\n")
	return
}

func makeStructStanza(col PgColumnMetadata, maxDbNameLen, maxVarNameLen, maxVarTypeLen int) (s string, err error) {

	var ary []string

	goVarName := u.ToUpperCamelCase(col.ColumnName)
	jsonName := u.ToLowerCamelCase(col.ColumnName)

	var varType string
	varType, err = TranslateType(col.TypeName)
	if err != nil {
		err = fmt.Errorf("makeStructStanza - %s: %s", col.ColumnName, err)
		return
	}

	JSONToken := u.Lpad("`json:\""+jsonName+"\"", maxVarNameLen+9)
	DbToken := u.Lpad("db:\""+col.ColumnName+"\"`", maxDbNameLen+6)

	ary = append(ary, "\t")
	ary = append(ary, u.Lpad(goVarName, maxVarNameLen+1))
	ary = append(ary, u.Lpad(varType, maxVarTypeLen+1))
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

	s = strings.Join(ary, "")
	return
}

func getMaxLens(cols []PgColumnMetadata) (maxDbNameLen, maxVarNameLen, maxVarTypeLen int, err error) {

	for _, col := range cols {
		goVarName := u.ToUpperCamelCase(col.ColumnName)
		maxDbNameLen = maxStringLen(col.ColumnName, maxDbNameLen)
		maxVarNameLen = maxStringLen(goVarName, maxVarNameLen)

		var varType string
		varType, err = TranslateType(col.TypeName)
		if err != nil {
			err = fmt.Errorf("getMaxLens - %s: %s", col.ColumnName, err)
			return
		}
		maxVarTypeLen = maxStringLen(varType, maxVarTypeLen)
	}
	return
}

func maxStringLen(s string, sz int) int {
	if len(s) > sz {
		return len(s)
	}
	return sz
}
