package util

import (
	"fmt"
	"strings"
)

func ToUpperCamelCase(pgV string) string {

	ary := strings.Split(pgV, "_")
	for i, v := range ary {
		switch strings.ToLower(v) {
		case "id", "html", "json":
			ary[i] = strings.ToUpper(v)
		default:
			ary[i] = strings.Title(v)
		}
	}
	return strings.Join(ary, "")
}

func ToLowerCamelCase(pgV string) string {

	ary := strings.Split(pgV, "_")
	for i, v := range ary {
		if i == 0 {
			ary[i] = strings.ToLower(v)
		} else {
			switch strings.ToLower(v) {
			case "id", "html", "json":
				ary[i] = strings.ToUpper(v)
			default:
				ary[i] = strings.Title(v)
			}
		}
	}
	return strings.Join(ary, "")
}

func Lpad(s string, l int) string {
	f := fmt.Sprintf("%%-%dv", l)
	return fmt.Sprintf(f, s)
}

func ToNullVarType(pgV string) string {

	//type NullBool
	//type NullFloat64
	//type NullInt32
	//type NullInt64
	//type NullString
	//type NullTime

	switch pgV {
	case "date", "time":
		return "sql.NullTime"

	case "boolean":
		return "sql.NullBool"

	case "int", "smallint", "integer":
		return "sql.NullInt32"

	case "bigint":
		return "sql.NullInt64"

	}

	if strings.Contains(pgV, "timestamp") {
		return "sql.NullTime"
	}

	return "sql.NullString"
}

func ToGoVarType(pgV string) string {

	switch pgV {
	case "date", "time":
		return "Time"

	case "boolean":
		return "bool"

	case "int", "smallint", "integer":
		return "int"

	case "bigint":
		return "int64"

	}

	if strings.Contains(pgV, "timestamp") {
		return "Time"
	}

	return "string"
}
