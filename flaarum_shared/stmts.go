package flaarum_shared

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

func NameValidate(name string) error {
	if strings.Contains(name, ".") || strings.Contains(name, " ") || strings.Contains(name, "\t") ||
		strings.Contains(name, "\n") || strings.Contains(name, ":") || strings.Contains(name, "/") ||
		strings.Contains(name, "~") {
		return errors.New("object name must not contain space, '.', ':', '/', ~ ")
	}

	return nil
}

func ParseTableStructureStmt(stmt string) (TableStruct, error) {
	ts := TableStruct{}
	stmt = strings.TrimSpace(stmt)
	if !strings.HasPrefix(stmt, "table:") {
		return ts, errors.New("Bad Statement: structure statements starts with 'table: '")
	}

	line1 := strings.Split(stmt, "\n")[0]
	tableName := strings.TrimSpace(line1[len("table:"):])
	ts.TableName = tableName

	if err := NameValidate(tableName); err != nil {
		return ts, err
	}

	fieldsBeginPart := strings.Index(stmt, "fields:")
	if fieldsBeginPart == -1 {
		return ts, errors.New("Bad Statement: structures statements must have a 'fields:' section.")
	}

	fieldsBeginPart += len("fields:")
	fieldsEndPart := strings.Index(stmt[fieldsBeginPart:], "::")
	if fieldsEndPart == -1 {
		return ts, errors.New("Bad Statement: fields section must end with a '::'.")
	}
	fieldsPart := stmt[fieldsBeginPart : fieldsBeginPart+fieldsEndPart]
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

		if err := NameValidate(parts[0]); err != nil {
			return ts, err
		}

		if FindIn([]string{"int", "float", "string", "bool", "date", "datetime", "email", "url", "ipaddr", "text"}, parts[1]) == -1 {
			return ts, errors.New(fmt.Sprintf("Bad Statement: the field type '%s' is not allowed in flaarum.", parts[1]))
		}
		fs := FieldStruct{FieldName: parts[0], FieldType: parts[1]}
		if len(parts) > 2 {
			for _, otherPart := range parts[2:] {
				if otherPart == "required" {
					fs.Required = true
				} else if otherPart == "unique" {
					fs.Unique = true
				} else if otherPart == "nindex" {
					fs.NotIndexed = true
				}
			}
		}
		fss = append(fss, fs)
	}
	ts.Fields = fss

	fkeyPartBegin := strings.Index(stmt, "foreign_keys:")
	if fkeyPartBegin != -1 {
		fkeyPartBegin += len("foreign_keys:")
		fkeyPartEnd := strings.Index(stmt[fkeyPartBegin:], "::")
		if fkeyPartEnd == -1 {
			return ts, errors.New("Bad Statement: a 'foreign_keys:' section must end with a '::'.")
		}
		fkeyPart := stmt[fkeyPartBegin : fkeyPartBegin+fkeyPartEnd]
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

	return ts, nil
}

func specialSplitLine(line string) ([]string, error) {
	line = strings.TrimSpace(line)
	chars := strings.Split(line, "")

	splits := make([]string, 0)
	var tmpWord string

	index := 0
	for {
		if index >= len(chars) {
			if tmpWord != "" {
				splits = append(splits, tmpWord)
				tmpWord = ""
			}
			break
		}
		ch := chars[index]
		if ch == "'" {
			nextQuoteIndex := strings.Index(line[index+1:], "'")
			if nextQuoteIndex == -1 {
				return splits, errors.New(fmt.Sprintf("The line \"%s\" has a quote and no second quote.", line))
			}
			tmpWord = line[index+1 : index+nextQuoteIndex+1]
			splits = append(splits, tmpWord)
			tmpWord = ""
			index += nextQuoteIndex + 2
			continue
		} else if ch == " " || ch == "\t" {
			if tmpWord != "" {
				splits = append(splits, tmpWord)
				tmpWord = ""
			}
		} else {
			tmpWord += ch
		}
		index += 1
		continue
	}

	return splits, nil
}

func parseWhereSubStmt(wherePart string) ([]WhereStruct, error) {
	whereStructs := make([]WhereStruct, 0)
	for _, part := range strings.Split(wherePart, "\n") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		parts, err := specialSplitLine(part)
		if err != nil {
			return nil, err
		}
		if len(parts) < 2 {
			return nil, errors.New(fmt.Sprintf("The part \"%s\" is not up to two words.", part))
		}
		var whereStruct WhereStruct
		if len(whereStructs) == 0 {
			whereStruct = WhereStruct{FieldName: parts[0], Relation: parts[1]}
			if whereStruct.Relation == "in" {
				whereStruct.FieldValues = parts[2:]
			} else {
				whereStruct.FieldValue = parts[2]
			}
		} else {
			whereStruct = WhereStruct{Joiner: parts[0], FieldName: parts[1], Relation: parts[2]}
			if whereStruct.Relation == "in" {
				whereStruct.FieldValues = parts[3:]
			} else {
				whereStruct.FieldValue = parts[3]
			}
		}
		whereStructs = append(whereStructs, whereStruct)
	}

	return whereStructs, nil
}

func ParseSearchStmt(stmt string) (StmtStruct, error) {
	stmt = strings.TrimSpace(stmt)
	stmtStruct := StmtStruct{}
	for _, part := range strings.Split(stmt, "\n") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		if strings.HasPrefix(part, "table:") {
			parts := strings.Fields(part[len("table:"):])
			if len(parts) == 0 {
				return stmtStruct, errors.New("The 'table:' part is required and accepts a table name followed by two optional words")
			}
			stmtStruct.TableName = parts[0]
			if len(parts) > 1 {
				for _, p := range parts[1:] {
					if p == "expand" {
						stmtStruct.Expand = true
					} else if p == "distinct" {
						stmtStruct.Distinct = true
					}
				}
			}
		} else if strings.HasPrefix(part, "fields:") {
			stmtStruct.Fields = strings.Fields(part[len("fields:"):])
		} else if strings.HasPrefix(part, "start_index:") {
			startIndexStr := strings.TrimSpace(part[len("start_index:"):])
			startIndex, err := strconv.ParseInt(startIndexStr, 10, 64)
			if err != nil {
				return stmtStruct, errors.New(fmt.Sprintf("The data '%s' for the 'start_index:' part is not a number.",
					startIndexStr))
			}
			stmtStruct.StartIndex = startIndex
		} else if strings.HasPrefix(part, "limit:") {
			limitStr := strings.TrimSpace(part[len("limit:"):])
			limit, err := strconv.ParseInt(limitStr, 10, 64)
			if err != nil {
				return stmtStruct, errors.New(fmt.Sprintf("The data '%s' for the 'limit:' part is not a number.",
					limitStr))
			}
			stmtStruct.Limit = limit
		} else if strings.HasPrefix(part, "order_by:") {
			parts := strings.Fields(part[len("order_by:"):])
			if len(parts) != 2 {
				return stmtStruct, errors.New("The words for 'order_by:' part must be two: a field and either of 'asc' or 'desc'")
			}
			stmtStruct.OrderBy = parts[0]
			if parts[1] != "asc" && parts[1] != "desc" {
				return stmtStruct, errors.New(fmt.Sprintf("The order direction must be either of 'asc' or 'desc'. Instead found '%s'",
					parts[1]))
			}
			stmtStruct.OrderDirection = parts[1]
		}
	}

	haveMulti := strings.Index(stmt, "statements_relation:")
	if haveMulti != -1 {
		stmt = strings.TrimSpace(stmt)
		stmtStruct := StmtStruct{}
		var statmentRelation string
		for _, part := range strings.Split(stmt, "\n") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}

			if strings.HasPrefix(part, "statements_relation:") {
				opt := part[len("statements_relation:"):]
				opt = strings.TrimSpace(opt)
				if opt != "and" && opt != "or" {
					return stmtStruct, errors.New("statements_relation only accepts either 'and' or 'or'")
				} else {
					statmentRelation = opt
				}
				break
			}
		}

		whereOpts := make([][]WhereStruct, 0)
		// where1
		where1PartBegin := strings.Index(stmt, "where1:")
		where1PartEnd := strings.Index(stmt[where1PartBegin:], "::")
		if where1PartEnd == -1 {
			return stmtStruct, errors.New("Every where section must end with '::'")
		}
		where1Part := stmt[where1PartBegin : where1PartBegin+where1PartEnd]
		where1Structs, err := parseWhereSubStmt(where1Part)
		if err != nil {
			return stmtStruct, err
		}

		whereOpts = append(whereOpts, where1Structs)

		// where2
		where2PartBegin := strings.Index(stmt, "where2:")
		if where2PartBegin == -1 {
			return stmtStruct, errors.New("A statement with 'final_stmt:' must have 'where1:' and 'where2:' sections")
		}

		where2PartEnd := strings.Index(stmt[where2PartBegin:], "::")
		if where2PartEnd == -1 {
			return stmtStruct, errors.New("Every where section must end with '::'")
		}
		where2Part := stmt[where2PartBegin : where2PartBegin+where2PartEnd]
		where2Structs, err := parseWhereSubStmt(where2Part)
		if err != nil {
			return stmtStruct, err
		}

		whereOpts = append(whereOpts, where2Structs)

		where3PartBegin := strings.Index(stmt, "where3:")
		if where3PartBegin != -1 {
			where3PartEnd := strings.Index(stmt[where3PartBegin:], "::")
			if where3PartEnd == -1 {
				return stmtStruct, errors.New("Every where section must end with '::'")
			}
			where3Part := stmt[where3PartBegin : where3PartBegin+where3PartEnd]
			where3Structs, err := parseWhereSubStmt(where3Part)
			if err != nil {
				return stmtStruct, err
			}

			whereOpts = append(whereOpts, where3Structs)
		}

		where4PartBegin := strings.Index(stmt, "where4:")
		if where4PartBegin != -1 {
			where4PartEnd := strings.Index(stmt[where4PartBegin:], "::")
			if where4PartEnd == -1 {
				return stmtStruct, errors.New("Every where section must end with '::'")
			}
			where4Part := stmt[where4PartBegin : where4PartBegin+where4PartEnd]
			where4Structs, err := parseWhereSubStmt(where4Part)
			if err != nil {
				return stmtStruct, err
			}

			whereOpts = append(whereOpts, where4Structs)
		}

		stmtStruct.EndStruct = EndingStmtStructMulti{whereOpts, statmentRelation}
	} else {

		wherePartBegin := strings.Index(stmt, "where:")
		if wherePartBegin != -1 {
			wherePart := stmt[wherePartBegin+len("where:"):]
			whereStructs, err := parseWhereSubStmt(wherePart)
			if err != nil {
				return stmtStruct, err
			}

			stmtStruct.EndStruct = EndingStmtStructSingle{whereStructs}
		}

	}

	return stmtStruct, nil
}

func FormatTableStruct(tableStruct TableStruct) string {
	stmt := "table: " + tableStruct.TableName + "\n"
	stmt += "fields:\n"
	for _, fieldStruct := range tableStruct.Fields {
		stmt += "  " + fieldStruct.FieldName + " " + fieldStruct.FieldType
		if fieldStruct.Required {
			stmt += " required"
		}
		if fieldStruct.Unique {
			stmt += " unique"
		}
		if fieldStruct.NotIndexed {
			stmt += " nindex"
		}
		stmt += "\n"
	}
	stmt += "::\n"
	if len(tableStruct.ForeignKeys) > 0 {
		stmt += "foreign_keys:\n"
		for _, fks := range tableStruct.ForeignKeys {
			stmt += "  " + fks.FieldName + " " + fks.PointedTable + " " + fks.OnDelete + "\n"
		}
		stmt += "::\n"
	}

	return stmt
}
