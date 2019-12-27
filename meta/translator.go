package meta

import (
	"fmt"
	//"github.com/jackc/pgtype"
)

type pgType struct {
	oid          int
	schemaName   string
	typeName     string
	baseOid      int
	baseTypeName string
	typeType     string
	typeCategory string
}

type Translator struct {
	userDomains map[string]string
	userTypes   map[string]*PgUsertypeMetadata
	pgTypes     map[string]string
	oidToType   map[int]*pgType
}

var tc Translator

func init() {
	if tc.userDomains == nil {
		tc.userDomains = make(map[string]string)
	}

	if tc.pgTypes == nil {
		tc.pgTypes = make(map[string]string)

		tc.pgTypes = map[string]string{
			"_aclitem":     "pgtype.ACLItemArray",
			"_bool":        "pgtype.BoolArray",
			"_bpchar":      "pgtype.BPCharArray",
			"_bytea":       "pgtype.ByteaArray",
			"_cidr":        "pgtype.CIDRArray",
			"_date":        "pgtype.DateArray",
			"_float4":      "pgtype.Float4Array",
			"_float8":      "pgtype.Float8Array",
			"_inet":        "pgtype.InetArray",
			"_int2":        "pgtype.Int2Array",
			"_int4":        "pgtype.Int4Array",
			"_int8":        "pgtype.Int8Array",
			"_numeric":     "pgtype.NumericArray",
			"_text":        "pgtype.TextArray",
			"_timestamp":   "pgtype.TimestampArray",
			"_timestamptz": "pgtype.TimestamptzArray",
			"_uuid":        "pgtype.UUIDArray",
			"_varchar":     "pgtype.VarcharArray",
			"aclitem":      "pgtype.ACLItem",
			"bit":          "pgtype.Bit",
			"bool":         "pgtype.Bool",
			"box":          "pgtype.Box",
			"bpchar":       "pgtype.BPChar",
			"bytea":        "pgtype.Bytea",
			"char":         "pgtype.QChar",
			"cid":          "pgtype.CID",
			"cidr":         "pgtype.CIDR",
			"circle":       "pgtype.Circle",
			"date":         "pgtype.Date",
			"daterange":    "pgtype.Daterange",
			"float4":       "pgtype.Float4",
			"float8":       "pgtype.Float8",
			"hstore":       "pgtype.Hstore",
			"inet":         "pgtype.Inet",
			"int2":         "pgtype.Int2",
			"int4":         "pgtype.Int4",
			"int4range":    "pgtype.Int4range",
			"int8":         "pgtype.Int8",
			"int8range":    "pgtype.Int8range",
			"interval":     "pgtype.Interval",
			"json":         "pgtype.JSON",
			"jsonb":        "pgtype.JSONB",
			"line":         "pgtype.Line",
			"lseg":         "pgtype.Lseg",
			"macaddr":      "pgtype.Macaddr",
			"name":         "pgtype.Name",
			"numeric":      "pgtype.Numeric",
			"numrange":     "pgtype.Numrange",
			"oid":          "pgtype.OIDValue",
			"path":         "pgtype.Path",
			"point":        "pgtype.Point",
			"polygon":      "pgtype.Polygon",
			"record":       "pgtype.Record",
			"text":         "pgtype.Text",
			"tid":          "pgtype.TID",
			"timestamp":    "pgtype.Timestamp",
			"timestamptz":  "pgtype.Timestamptz",
			"tsrange":      "pgtype.Tsrange",
			"tstzrange":    "pgtype.Tstzrange",
			"unknown":      "pgtype.Unknown",
			"uuid":         "pgtype.UUID",
			"varbit":       "pgtype.Varbit",
			"varchar":      "pgtype.Varchar",
			"xid":          "pgtype.XID",
		}
	}
}

func addUserDomain(domainName, pgTypeName string) {

	if tc.userDomains == nil {
		tc.userDomains = make(map[string]string)
	}

	tc.userDomains[domainName] = pgTypeName
}

func addOidToType(oid int, p *pgType) {

	if tc.oidToType == nil {
		tc.oidToType = make(map[int]*pgType)
	}

	tc.oidToType[oid] = p
}

/*
func addOidToType(oid int, pgTypeName string) {

	if tc.oidToType == nil {
		tc.oidToType = make(map[int]string)
	}

	tc.oidToType[oid] = pgTypeName
}
*/

func TranslateType(typeName string) (n string, err error) {

	n, ok := tc.pgTypes[typeName]
	if ok {
		return
	}

	d, ok := tc.userDomains[typeName]
	if ok {
		n, err = TranslateType(d)
		if err != nil {
			return
		}
	}

	err = fmt.Errorf("Unable to translate Pg type name %q", typeName)
	return
}
