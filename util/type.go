package util

import (
	"strings"
)

func ToNullVarType(pgV string) string {

	switch pgV {
	case "date", "time", "interval":
		return "sql.NullTime"

	case "boolean":
		return "sql.NullBool"

	case "int", "smallint", "integer":
		return "sql.NullInt32"

	case "bigint":
		return "sql.NullInt64"

	case "float4", "float8", "numeric", "money":
		return "sql.NullFloat"

	}

	if strings.Contains(pgV, "timestamp") {
		return "sql.NullTime"
	}

	return "sql.NullString"
}

func ToIntVarType(pgV string) string {

	switch pgV {
	case "date", "time", "interval":
		return "Time"

	case "boolean":
		return "Bool"

	case "int", "smallint", "integer":
		return "Int32"

	case "bigint":
		return "Int64"

	case "float8", "numeric", "money":
		return "Float64"

	case "float4":
		return "Float32"

	}

	if strings.Contains(pgV, "timestamp") {
		return "Time"
	}

	return "String"
}

func ToGoVarType(pgV string) string {

	switch pgV {
	case "date", "time", "interval":
		return "time.Time"

	case "boolean":
		return "bool"

	case "smallint":
		return "int"

	case "int", "integer":
		return "int32"

	case "bigint":
		return "int64"

	case "float4":
		return "float32"

	case "float8", "numeric", "money":
		return "float64"

	}

	if strings.Contains(pgV, "timestamp") {
		return "time.Time"
	}

	return "string"
}
