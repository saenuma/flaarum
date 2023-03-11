package flaarum_shared

type FieldStruct struct {
	FieldName  string
	FieldType  string
	Required   bool
	Unique     bool
	NotIndexed bool
}

type FKeyStruct struct {
	FieldName    string
	PointedTable string
	OnDelete     string // expects one of "on_delete_restrict", "on_delete_delete"
}

type TableStruct struct {
	TableName   string
	Fields      []FieldStruct
	ForeignKeys []FKeyStruct
}

type WhereStruct struct {
	FieldName   string
	Relation    string // eg. '=', '!=', '<', etc.
	FieldValue  string
	Joiner      string   // one of 'and', 'or', 'orf'
	FieldValues []string // for 'in' and 'nin' queries
}

type StmtStruct struct {
	TableName      string
	Fields         []string
	Expand         bool
	Distinct       bool
	StartIndex     int64
	Limit          int64
	OrderBy        string
	OrderDirection string // one of 'asc' or 'desc'
	WhereOptions   []WhereStruct
}
