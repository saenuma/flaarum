package main

import (
	"net/http"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"strconv"
	"encoding/json"
	"path/filepath"
	"io/ioutil"
	"fmt"
	"os"
	"strings"
	"time"
	"github.com/bankole7782/flaarum/flaarum_shared"
)


func insertRow(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]
	tableName := vars["tbl"]

	r.FormValue("email")

	toInsert := make(map[string]string)
	for k, _ := range r.PostForm {
		if k == "keyStr" || k == "id" || k == "_version" {
			continue
		}
		if r.FormValue(k) == "" {
			continue
		}
		toInsert[k] = r.FormValue(k)
	}

	dataPath, _ := GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)
	if ! doesPathExists(tablePath) {
		printError(w, errors.New(fmt.Sprintf("Table '%s' of Project '%s' does not exists.", tableName, projName)))
		return
	}

	createTableMutexIfNecessary(projName, tableName)
	fullTableName := projName + ":" + tableName
	tablesMutexes[fullTableName].Lock()
	defer tablesMutexes[fullTableName].Unlock()

	currentVersionNum, err := getCurrentVersionNum(projName, tableName)
	if err != nil {
		printError(w, err)
		return
	}

	// check if data conforms with table structure
	tableStruct, err := getTableStructureParsed(projName, tableName, currentVersionNum)
	if err != nil {
		printError(w, err)
		return
	}

	fieldNames := make([]string, 0)
	for _, fieldStruct := range tableStruct.Fields {
		fieldNames = append(fieldNames, fieldStruct.FieldName)

		value, ok := toInsert[fieldStruct.FieldName]
		if fieldStruct.Required {
			if ! ok {
				printError(w, errors.New(fmt.Sprintf("The field '%s' is required in table '%s' of project '%s'", 
					fieldStruct.FieldName, tableName, projName)))
				return
			}
		}

		if fieldStruct.Unique && fieldStruct.FieldType != "text" {
			if doesPathExists(filepath.Join(tablePath, "indexes", fieldStruct.FieldName, value)) {
				printError(w, errors.New(fmt.Sprintf("The field '%s' with value '%s' is not unique to table '%s' of project '%s'",
					fieldStruct.FieldName, value, tableName, projName)))
				return
			}
		}

		// check if the right data type is sent to each field
		switch (fieldStruct.FieldType) {
		case "int":
			_, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				printError(w, errors.New(fmt.Sprintf("The data '%s' to field '%s' is not of type 'int' as defined in the structure", 
					value, fieldStruct.FieldName)))
				return
			}
		case "float":
			_, err = strconv.ParseFloat(value, 64)
			if err != nil {
				printError(w, errors.New(fmt.Sprintf("The data '%s' to field '%s' is not of type 'float' as defined in the structure", 
					value, fieldStruct.FieldName)))
				return				
			}
		case "bool":
			if value != "t" && value != "f" {
				printError(w, errors.New(fmt.Sprintf("The data '%s' to field '%s' is not one of 't' or 'f'.", 
					value, fieldStruct.FieldName)))
				return
			}
		case "date":
			_, err := time.Parse(flaarum_shared.BROWSER_DATE_FORMAT, value)
			if err != nil {
				printError(w, errors.New(fmt.Sprintf("The data '%s' to field '%s' is not in date format: '%s'.",
					value, fieldStruct.FieldName, flaarum_shared.BROWSER_DATE_FORMAT)))
				return
			}			
		case "datetime":
			_, err := time.Parse(flaarum_shared.BROWSER_DATETIME_FORMAT, value)
			if err != nil {
				printError(w, errors.New(fmt.Sprintf("The data '%s' to field '%s' is not in date format: '%s'.",
					value, fieldStruct.FieldName, flaarum_shared.BROWSER_DATETIME_FORMAT)))
				return
			}
		case "string":
			if len(value) > flaarum_shared.STRING_MAX_LENGTH {
				printError(w, errors.New(fmt.Sprintf("The data '%s' to field '%s' is too long for string type. Max Length is '%d'",
					value, fieldStruct.FieldName, flaarum_shared.STRING_MAX_LENGTH)))
				return
			}
		}
	}

	for field, _ := range toInsert {
		if flaarum_shared.FindIn(fieldNames, field) == -1 {
			printError(w, errors.New(fmt.Sprintf("The field '%s' is not in the structure of table '%s' of project '%s'", 
				field, tableName, projName)))
			return
		}
	}

	// add extra search fields for date and datetime types
	for _, fieldStruct := range tableStruct.Fields {
		if fieldStruct.FieldType == "date" {
			value, ok := toInsert[fieldStruct.FieldName]
			if ok {
				valueInTimeType, _ := time.Parse(flaarum_shared.BROWSER_DATETIME_FORMAT, value)
				toInsert[fieldStruct.FieldName + "_year"] = strconv.Itoa(valueInTimeType.Year())
				toInsert[fieldStruct.FieldName + "_month"] = strconv.Itoa(int(valueInTimeType.Month()))
				toInsert[fieldStruct.FieldName + "_day"] = strconv.Itoa(valueInTimeType.Day())
			}
		}
		if fieldStruct.FieldType == "datetime" {
			value, ok := toInsert[fieldStruct.FieldName]
			if ok {
				valueInTimeType, _ := time.Parse(flaarum_shared.BROWSER_DATETIME_FORMAT, value)
				toInsert[fieldStruct.FieldName + "_year"] = strconv.Itoa(valueInTimeType.Year())
				toInsert[fieldStruct.FieldName + "_month"] = strconv.Itoa(int(valueInTimeType.Month()))
				toInsert[fieldStruct.FieldName + "_day"] = strconv.Itoa(valueInTimeType.Day())
				toInsert[fieldStruct.FieldName + "_hour"] = strconv.Itoa(valueInTimeType.Hour())
				toInsert[fieldStruct.FieldName + "_date"] = valueInTimeType.Format(flaarum_shared.BROWSER_DATE_FORMAT)
			}
		}
	}


	var nextId int64
	if ! doesPathExists(filepath.Join(tablePath, "lastId")) {
		nextId = 1
	} else {
		raw, err := ioutil.ReadFile(filepath.Join(tablePath, "lastId"))
		if err != nil {
			printError(w, errors.Wrap(err, "ioutil error"))
			return
		}

		rawNum, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			printError(w, errors.Wrap(err, "strconv error"))
			return
		}
		nextId = rawNum + 1
	}

	toInsert["_version"] = fmt.Sprintf("%d", currentVersionNum)

	jsonBytes, err := json.Marshal(toInsert)
	if err != nil {
		printError(w, errors.Wrap(err, "json error"))
		return
	}

	nextIdStr := strconv.FormatInt(nextId, 10)
	err = ioutil.WriteFile(filepath.Join(tablePath, "data", nextIdStr), jsonBytes, 0777)
	if err != nil {
		printError(w, errors.Wrap(err, "ioutil error"))
		return
	}

	// create indexes
	for field, value := range toInsert {
		err = os.MkdirAll(filepath.Join(tablePath, "indexes", field), 0777)
		if err != nil {
			printError(w, errors.Wrap(err, "os error"))
			return
		}

		indexesPath := filepath.Join(tablePath, "indexes", field, makeSafeIndexValue(value))
		if ! doesPathExists(indexesPath) {
			err = ioutil.WriteFile(indexesPath, []byte(nextIdStr), 0777)
			if err != nil {
				printError(w, errors.Wrap(err, "ioutil error"))
				return
			}
		} else {
			raw, err := ioutil.ReadFile(indexesPath)
			if err != nil {
				printError(w, errors.Wrap(err, "ioutil error"))
				return
			}
			indexedIds := strings.Split(string(raw), "\n")
			indexedIds = append(indexedIds, nextIdStr)

			err = ioutil.WriteFile(indexesPath, []byte(strings.Join(indexedIds, "\n")), 0777)
			if err != nil {
				printError(w, errors.Wrap(err, "ioutil error"))
				return
			}
		}
	}

	// store last id
	err = ioutil.WriteFile(filepath.Join(tablePath, "lastId"), []byte(nextIdStr), 0777)
	if err != nil {
		printError(w, errors.Wrap(err, "ioutil error"))
		return
	}

	fmt.Fprintf(w, nextIdStr)
}


func saveRowData(projName, tableName, rowId string, toWrite map[string]string) error {
  tablePath := getTablePath(projName, tableName)
  jsonBytes, err := json.Marshal(&toWrite)
  if err != nil {
    return errors.Wrap(err, "json error")
  }
  err = ioutil.WriteFile(filepath.Join(tablePath, "data", rowId), jsonBytes, 0777)
  if err != nil {
    return errors.Wrap(err, "write file failed.")
  }

  return nil
}