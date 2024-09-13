package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/internal"
)

func validateAndMutateDataMap(projName, tableName string, dataMap, oldValues map[string]string) (map[string]string, error) {
	tableStruct, err := getCurrentTableStructureParsed(projName, tableName)
	if err != nil {
		return nil, err
	}

	fieldsDescs := make(map[string]internal.FieldStruct)
	for _, fd := range tableStruct.Fields {
		fieldsDescs[fd.FieldName] = fd
	}

	for k := range dataMap {
		if k == "id" || k == "_version" {
			continue
		}
		_, ok := fieldsDescs[k]
		if !ok {
			return nil, errors.New(fmt.Sprintf("The field '%s' is not part of this table structure", k))
		}
	}

	for _, fd := range tableStruct.Fields {
		k := fd.FieldName
		v, ok := dataMap[k]

		if ok && v != "" {
			if fd.FieldType == "string" {
				if len(v) > 220 {
					return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is longer than 220 characters", v, k))
				}
				if strings.Contains(v, "\n") || strings.Contains(v, "\r\n") {
					return nil, errors.New(fmt.Sprintf("The value of field '%s' contains new line.", k))
				}
			}

			if fd.FieldType == "int" {

				_, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is not of type 'int'", v, k))
				}
			}

		}

		if !ok && fd.Required {
			return nil, errors.New(fmt.Sprintf("The field '%s' is required.", k))
		}

	}

	// validate unique property
	for _, fd := range tableStruct.Fields {
		newValue, ok1 := dataMap[fd.FieldName]
		if newValue == "" {
			delete(dataMap, fd.FieldName)
		}
		if oldValues != nil {
			oldValue, ok2 := oldValues[fd.FieldName]
			if ok1 && ok2 && oldValue == newValue {
				continue
			}
		}
		if fd.Unique && ok1 {
			innerStmt := fmt.Sprintf(`
      	table: %s
      	where:
      		%s = %s
      	`, tableName, fd.FieldName, newValue)
			toCheckRows, err := innerSearch(projName, innerStmt)
			if err != nil {
				return nil, err
			}

			if len(*toCheckRows) > 0 {
				return nil, errors.New(fmt.Sprintf("The data '%s' is not unique to field '%s'.", newValue, fd.FieldName))
			}
		}
	}

	// validate all foreign keys
	for _, fkd := range tableStruct.ForeignKeys {
		v, ok := dataMap[fkd.FieldName]
		if ok {
			innerStmt := fmt.Sprintf(`
				table: %s
				where:
					id = %s
				`, fkd.PointedTable, v)

			toCheckRows, err := innerSearch(projName, innerStmt)
			if err != nil {
				return nil, err
			}
			if len(*toCheckRows) == 0 {
				return nil, errors.New(fmt.Sprintf("The data with id '%s' does not exist in table '%s'", v, fkd.PointedTable))
			}
		}
	}

	return dataMap, nil
}

func insertRow(w http.ResponseWriter, r *http.Request) {

	projName := r.PathValue("proj")
	tableName := r.PathValue("tbl")

	r.FormValue("email")

	toInsert := make(map[string]string)
	for k := range r.PostForm {
		if k == "key-str" || k == "id" || k == "_version" {
			continue
		}
		if r.FormValue(k) == "" {
			continue
		}
		toInsert[k] = r.FormValue(k)
	}

	currentVersionNum, err := getCurrentVersionNum(projName, tableName)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	toInsert["_version"] = fmt.Sprintf("%d", currentVersionNum)

	dataPath, _ := internal.GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)
	if !internal.DoesPathExists(tablePath) {
		internal.PrintError(w, errors.New(fmt.Sprintf("Table '%s' of Project '%s' does not exists.", tableName, projName)))
		return
	}

	// check if data conforms with table structure
	toInsert, err = validateAndMutateDataMap(projName, tableName, toInsert, nil)
	if err != nil {
		printValError(w, err)
		return
	}

	createTableMutexIfNecessary(projName, tableName)
	fullTableName := projName + ":" + tableName
	tablesMutexes[fullTableName].Lock()
	defer tablesMutexes[fullTableName].Unlock()

	var nextId int64
	lastIdPath := filepath.Join(tablePath, "lastId.txt")

	if !internal.DoesPathExists(lastIdPath) {
		nextId = 1
	} else {
		raw, err := os.ReadFile(lastIdPath)
		if err != nil {
			internal.PrintError(w, err)
			return
		}
		lastId, _ := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)

		nextId = lastId + 1
	}

	nextIdStr := strconv.FormatInt(nextId, 10)

	err = internal.SaveRowData(projName, tableName, nextIdStr, toInsert)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	os.WriteFile(lastIdPath, []byte(nextIdStr), 0777)

	// create indexes
	for k, v := range toInsert {
		if !internal.IsNotIndexedField(projName, tableName, k) {
			err := internal.MakeIndex(projName, tableName, k, v, nextIdStr)
			if err != nil {
				internal.PrintError(w, err)
				return
			}
		}

	}

	fmt.Fprint(w, nextIdStr)

}
