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

	switch pgV {
	case "date":
		return "null.Time"

	case "boolean":
		return "null.Bool"

	case "int", "smallint", "integer", "bigint":
		return "null.Int"

	}

	if strings.Contains(pgV, "timestamp") {
		return "null.Time"
	}

	return "null.String"
}
