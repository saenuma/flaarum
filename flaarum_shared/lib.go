package flaarum_shared

import (
	"strings"
	"github.com/pkg/errors"
	"fmt"
)

const (
	BROWSER_DATE_FORMAT = "2006-01-02"
	BROWSER_DATETIME_FORMAT = "2006-01-02T15:04"
	STRING_MAX_LENGTH = 100
)

type FieldStruct struct {
	FieldName string
	FieldType string
	Required bool
	Unique bool
}

type FKeyStruct struct {
	FieldName string
	PointedTable string
	OnDelete string // expects one of "on_delete_restrict", "on_delete_empty", "on_delete_delete"
}

type TableStruct struct {
	TableName string
	Fields []FieldStruct
	ForeignKeys []FKeyStruct
	UniqueGroups [][]string
}


func ParseTableStructureStmt(stmt string) (TableStruct, error) {
	ts := TableStruct{}
	stmt = strings.TrimSpace(stmt)
	if ! strings.HasPrefix(stmt, "table:") {
		return ts, errors.New("Bad Statement: structure statements starts with 'table: '")
	}

	line1 := strings.Split(stmt, "\n")[0]
	tableName := strings.TrimSpace(line1[len("table:") :])
	ts.TableName = tableName

	fieldsBeginPart := strings.Index(stmt, "fields:")
	if fieldsBeginPart == -1 {
		return ts, errors.New("Bad Statement: structures statements must have a 'fields:' section.")
	}

	fieldsBeginPart += len("fields:")
	fieldsEndPart := strings.Index(stmt[fieldsBeginPart: ], "::")
	if fieldsEndPart == -1 {
		return ts, errors.New("Bad Statement: fields section must end with a '::'.")
	}
	fieldsPart := stmt[fieldsBeginPart: fieldsBeginPart + fieldsEndPart]
	fss := make([]FieldStruct, 0)
	for _, part := range strings.Split(fieldsPart, "\n") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		parts := strings.Fields(part)
		if len(parts) < 2 {
			return ts, errors.New("Bad Statement: a fields definition must have a minimum of two words.")
		}
		if parts[0] == "id" || parts[0] == "_version" {
			return ts, errors.New("Bad Statement: the fields 'id' and '_version' are automatically created. Hence can't be used.")
		}
		if FindIn([]string{"int", "float", "string", "text", "bool", "date", "datetime"}, parts[1]) == -1 {
			return ts, errors.New(fmt.Sprintf("Bad Statement: the field type '%s' is not allowed in flaarum.", parts[1]))
		}
		fs := FieldStruct{FieldName: parts[0], FieldType: parts[1]}
		if len(parts) > 2 {
			for _, otherPart := range parts[2:] {
				if otherPart == "required" {
					fs.Required = true
				} else if otherPart == "unique" {
					fs.Unique = true
				}
			}
		}

		fss = append(fss, fs)
	}
	ts.Fields = fss

	fkeyPartBegin := strings.Index(stmt, "foreign_keys:")
	if fkeyPartBegin != -1 {
		fkeyPartBegin += len("foreign_keys:")
		fkeyPartEnd := strings.Index(stmt[fkeyPartBegin: ], "::")
		if fkeyPartEnd == -1 {
			return ts, errors.New("Bad Statement: a 'foreign_keys:' section must end with a '::'.")
		}
		fkeyPart := stmt[fkeyPartBegin: fkeyPartBegin + fkeyPartEnd]
		fkeyStructs := make([]FKeyStruct, 0)
		for _, part := range strings.Split(fkeyPart, "\n") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			parts := strings.Fields(part)
			if len(parts) != 3 {
				return ts, errors.New("Bad Statement: a line in a 'foreign_keys:' section must have three words.")
			}
			fks := FKeyStruct{parts[0], parts[1], parts[2]}
			fkeyStructs = append(fkeyStructs, fks)
		}
		ts.ForeignKeys = fkeyStructs
	}

	uniqueGroupPartBegin := strings.Index(stmt, "unique_groups:")
	if uniqueGroupPartBegin != -1 {
		uniqueGroupPartBegin += len("unique_groups:")
		uniqueGroupPartEnd := strings.Index(stmt[uniqueGroupPartBegin: ], "::")
		if uniqueGroupPartEnd == -1 {
			return ts, errors.New("Bad Statement: a 'unique_groups:' section must end with a '::'.")
		}
		uniqueGroupPart := stmt[uniqueGroupPartBegin: uniqueGroupPartBegin + uniqueGroupPartEnd]
		uniqueGroups := make([][]string, 0)
		for _, part := range strings.Split(uniqueGroupPart, "\n") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			parts := strings.Fields(part)
			if len(parts) == 1 {
				return ts, errors.New("Bad Statement: a unique group definition must be two or more words.")
			}
			uniqueGroups = append(uniqueGroups, parts)
		}
		ts.UniqueGroups = uniqueGroups
	}

	return ts, nil
}


func FindIn(container []string, elem string) int {
	for i, o := range container {
		if o == elem {
			return i
		}
	}
	return -1
}
