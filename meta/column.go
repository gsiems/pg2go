package meta

// PgColumnMetadata contains metadata for database columns
type PgColumnMetadata struct {
	ColumnName      string `db:"column_name"`
	DataType        string `db:"data_type"`
	TypeName        string `db:"type_name"`
	TypeCategory    string `db:"type_category"`
	OrdinalPosition int    `db:"ordinal_position"`
	IsRequired      bool   `db:"is_required"`
	IsPk            bool   `db:"is_pk"`
	Description     string `db:"description"`
}
