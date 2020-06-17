// This package contains functions shared by the parts of this project
package flaarum_shared

import (
	"strings"
	"github.com/pkg/errors"
	"fmt"
	"strconv"
	"os"
	"path/filepath"
	"io/ioutil"
	"github.com/adam-hanna/arrayOperations"
	"math/rand"
	"time"
	"encoding/json"
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


func GetDataPath() (string, error) {
	dd := os.Getenv("SNAP_DATA")
	if strings.HasPrefix(dd, "/var/snap/go") || dd == "" {
		hd, err := os.UserHomeDir()
		if err != nil {
			return "", errors.Wrap(err, "os error")
		}
		dd = filepath.Join(hd, ".flaarum_data")	
	}
	return dd, nil
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
			nextQuoteIndex := strings.Index(line[index + 1 :], "'")
			if nextQuoteIndex == -1 {
				return splits, errors.New(fmt.Sprintf("The line \"%s\" has a quote and no second quote.", line))
			}
			tmpWord = line[index + 1: index + nextQuoteIndex + 1]
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


type WhereStruct struct {
	FieldName string
	Relation string // eg. '=', '!=', '<', etc.
	FieldValue string 
	Joiner string // one of 'and', 'or', 'orf'
	FieldValues []string // for 'in' and 'nin' queries
}


type StmtStruct struct {
	TableName string
	Fields []string
	Expand bool
	Distinct bool
	StartIndex int64
	Limit int64
	OrderBy string
	OrderDirection string // one of 'asc' or 'desc'
	WhereOptions []WhereStruct
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
			parts := strings.Fields(part[len("table:"): ])
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
			stmtStruct.Fields = strings.Fields(part[len("fields:") :])
		} else if strings.HasPrefix(part, "start_index:") {
			startIndexStr := strings.TrimSpace(part[len("start_index:") :])
			startIndex, err := strconv.ParseInt(startIndexStr, 10, 64)
			if err != nil {
				return stmtStruct, errors.New(fmt.Sprintf("The data '%s' for the 'start_index:' part is not a number.",
					startIndexStr))
			}
			stmtStruct.StartIndex = startIndex
		} else if strings.HasPrefix(part, "limit:") {
			limitStr := strings.TrimSpace(part[len("limit:") :])
			limit, err := strconv.ParseInt(limitStr, 10, 64)
			if err != nil {
				return stmtStruct, errors.New(fmt.Sprintf("The data '%s' for the 'limit:' part is not a number.",
					limitStr))
			}
			stmtStruct.Limit = limit
		} else if strings.HasPrefix(part, "order_by:") {
			parts := strings.Fields(part[len("order_by:") :])
			if len(parts) != 2 {
				return stmtStruct, errors.New("The words for 'order_by:' part must be two: a field and either of 'asc' or 'desc'")
			}
			stmtStruct.OrderBy = parts[0]
			if parts[1] != "asc" || parts[1] != "desc" {
				return stmtStruct, errors.New(fmt.Sprintf("The order direction must be either of 'asc' or 'desc'. Instead found '%s'",
					parts[1]))
			}
			stmtStruct.OrderDirection = parts[1]
		}
	}

	wherePartBegin := strings.Index(stmt, "where:")
	if wherePartBegin != -1 {
		whereStructs := make([]WhereStruct, 0)
		wherePart := stmt[wherePartBegin + len("where:") :]
		for _, part := range strings.Split(wherePart, "\n") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}

			parts, err := specialSplitLine(part)
			if err != nil {
				return stmtStruct, err
			}
			if len(parts) < 3 {
				return stmtStruct, errors.New(fmt.Sprintf("The part \"%s\" is not up to three words.", part))
			}
			var whereStruct WhereStruct
			if len(whereStructs) == 0 {
				whereStruct = WhereStruct{FieldName: parts[0], Relation: parts[1],}
				if whereStruct.Relation == "in" || whereStruct.Relation == "nin" {
					whereStruct.FieldValues = parts[2:]
				} else {
					whereStruct.FieldValue = parts[2]
				}
			} else {
				whereStruct = WhereStruct{Joiner: parts[0], FieldName: parts[1], Relation: parts[2],}
				if whereStruct.Relation == "in" || whereStruct.Relation == "nin" {
					whereStruct.FieldValues = parts[3:]
				} else {
					whereStruct.FieldValue = parts[3]
				}
			}
			whereStructs = append(whereStructs, whereStruct)
		}
		stmtStruct.WhereOptions = whereStructs
	}

	return stmtStruct, nil
}


func MakeSafeIndexName(v string) string {
  return strings.ReplaceAll(v, "/", "~~a~~")
}


func DoesTableExists(projName, tableName string) bool {
  dataPath, _ := GetDataPath()
  if _, err := os.Stat(filepath.Join(dataPath, projName, tableName)); os.IsNotExist(err) {
    return false
  } else {
    return true
  }
}


func MakeIndex(projName, tableName, fieldName, newData, rowId string) error {
  dataPath, _ := GetDataPath()
  indexFolder := filepath.Join(dataPath, projName, tableName, "indexes", fieldName)
  err := os.MkdirAll(indexFolder, 0777)
  if err != nil {
    return errors.Wrap(err, "create directory failed.")
  }
  indexPath := filepath.Join(indexFolder, MakeSafeIndexName(newData))
  if _, err := os.Stat(indexPath); os.IsNotExist(err) {
    err = ioutil.WriteFile(indexPath, []byte(rowId), 0777)
    if err != nil {
      return errors.Wrap(err, "file write failed.")
    }
  } else {
    raw, err := ioutil.ReadFile(indexPath)
    if err != nil {
      return errors.Wrap(err, "read failed.")
    }
    previousEntries := strings.Split(string(raw), "\n")
    newEntries := arrayOperations.UnionString(previousEntries, []string{rowId})
    err = ioutil.WriteFile(indexPath, []byte(strings.Join(newEntries, "\n")), 0777)
    if err != nil {
      return errors.Wrap(err, "write failed.")
    }
  }

  return nil
}


func UntestedRandomString(length int) string {
  var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
  const charset = "abcdefghijklmnopqrstuvwxyz1234567890"

  b := make([]byte, length)
  for i := range b {
    b[i] = charset[seededRand.Intn(len(charset))]
  }
  return string(b)
}


func GetSetting(settingName string) (string, error) {
  raw, err := ioutil.ReadFile("/etc/flaarum.json")
  if err != nil {
    return "", errors.Wrap(err, "read failed.")
  }

  settingObj := make(map[string]string)
  err = json.Unmarshal(raw, &settingObj)
  if err != nil {
    return "", errors.Wrap(err, "json failed.")
  }

  return settingObj[settingName], nil
}
