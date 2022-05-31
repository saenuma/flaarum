package main

import (
	"net/http"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"strconv"
	"path/filepath"
	"fmt"
	"strings"
	"time"
	"github.com/saenuma/flaarum/flaarum_shared"
  "github.com/mcnijman/go-emailaddress"
  "net"
  "net/url"
  "os"
)


func validateAndMutateDataMap(projName, tableName string, dataMap, oldValues map[string]string) (map[string]string, error) {
  tableStruct, err := getCurrentTableStructureParsed(projName, tableName)
  if err != nil {
    return nil, err
  }

  dataPath, _ := flaarum_shared.GetDataPath()

  fieldsDescs := make(map[string]flaarum_shared.FieldStruct)
  for _, fd := range tableStruct.Fields {
    fieldsDescs[fd.FieldName] = fd
  }

  for k, _ := range dataMap {
    if k == "id" || k == "_version" {
      continue
    }
    _, ok := fieldsDescs[k]
    if ! ok {
      return nil, errors.New(fmt.Sprintf("The field '%s' is not part of this table structure", k))
    }
  }

  for _, fd := range tableStruct.Fields {
    k := fd.FieldName
    v, ok := dataMap[k]

    if ok && v != "" {
			if fd.FieldType == "string" || fd.FieldType == "email" || fd.FieldType == "url" || fd.FieldType == "ipaddr" {
				if len(v) > 220 {
					return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is longer than 220 characters", v, k))
				}
				if strings.Contains(v, "\n") {
					return nil, errors.New(fmt.Sprintf("The value of field '%s' contains new line.", k))
				}
			}

      if fd.FieldType == "int" {
        _, err := strconv.ParseInt(v, 10, 64)
        if err != nil {
          return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is not of type 'int'", v, k))
        }
      } else if fd.FieldType == "float" {
        _, err := strconv.ParseFloat(v, 64)
        if err != nil {
          return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is not of type 'float'", v, k))
        }
      } else if fd.FieldType == "bool" {
        if v != "t" && v != "f" {
          return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is not in the short bool format.", v, k))
        }
      } else if fd.FieldType == "date" {
        valueInTimeType, err := time.Parse(flaarum_shared.DATE_FORMAT, v)
        if err != nil {
          return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is not in date format.", v, k))
        }
        dataMap[k + "_year"] = strconv.Itoa(valueInTimeType.Year())
        dataMap[k + "_month"] = strconv.Itoa(int(valueInTimeType.Month()))
        dataMap[k + "_day"] = strconv.Itoa(valueInTimeType.Day())
      } else if fd.FieldType == "datetime" {
        valueInTimeType, err := time.Parse(flaarum_shared.DATETIME_FORMAT, v)
        if err != nil {
          return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is not in datetime format.", v, k))
        }
        dataMap[k + "_year"] = strconv.Itoa(valueInTimeType.Year())
        dataMap[k + "_month"] = strconv.Itoa(int(valueInTimeType.Month()))
        dataMap[k + "_day"] = strconv.Itoa(valueInTimeType.Day())
        dataMap[k + "_hour"] = strconv.Itoa(valueInTimeType.Hour())
        dataMap[k + "_date"] = valueInTimeType.Format(flaarum_shared.DATE_FORMAT)
				dataMap[k + "_tzname"], _ = valueInTimeType.Zone()
      } else if fd.FieldType == "email" {
        _, err := emailaddress.Parse(v)
        if err != nil {
          return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is not in email format.", v, k))
        }
      } else if fd.FieldType == "ipaddr" {
        ipType := net.ParseIP(v)
        if ipType != nil {
          return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is not an ip address.", v, k))
        }
      } else if fd.FieldType == "url" {
        _, err := url.Parse(v)
        if err != nil {
          return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is not a valid url.", v, k))
        }
      }

    }

    if ok == false && fd.Required {
      return nil, errors.New(fmt.Sprintf("The field '%s' is required.", k))
    }

  }

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
      indexPath := filepath.Join(dataPath, projName, tableName, "indexes", fd.FieldName, makeSafeIndexName(newValue))
      if doesPathExists(indexPath) {
        return nil, errors.New(fmt.Sprintf("The data '%s' is not unique to field '%s'.", newValue, fd.FieldName))
      }
    }
  }

  for _, fkd := range tableStruct.ForeignKeys {
    v, ok := dataMap[fkd.FieldName]
    if ok {
      dataPath := filepath.Join(dataPath, projName, fkd.PointedTable, "data", v)
      if ! doesPathExists(dataPath) {
        return nil,  errors.New(fmt.Sprintf("The data with id '%s' does not exist in table '%s'", v, fkd.PointedTable))
      }
    }
  }

	for _, ug := range tableStruct.UniqueGroups {
    wherePartFragment := ""

    for i, fieldName := range ug {

      newValue, ok1 := dataMap[fieldName]
      var value string
      if ok1 {
        value = newValue
      }

      var relation string
      if i >= 1 {
      	relation = "and"
      }

      wherePartFragment += fmt.Sprintf("%s %s = '%s' \n", relation, fieldName, value)
    }

		if oldValues == nil {
			// run this during inserts
      innerStmt := fmt.Sprintf(`
      	table: %s
      	where:
      		%s
      	`, tableName, wherePartFragment)
      toCheckRows, err := innerSearch(projName, innerStmt)
      if err != nil {
        return nil, err
      }

      if len(*toCheckRows) > 0 {
        return nil, errors.New(
          fmt.Sprintf("The fields '%s' form a unique group and their data taken together is not unique.",
          strings.Join(ug, ", ")))
      }

		} else {
			// run this during updates
			uniqueGroupFieldsEqualityStatus := true

			for _, fieldName := range ug {
				if dataMap[fieldName] != oldValues[fieldName] {
					uniqueGroupFieldsEqualityStatus = false
					break
				}
			}


			if uniqueGroupFieldsEqualityStatus == false {
				innerStmt := fmt.Sprintf(`
					table: %s
					where:
					%s
					`, tableName, wherePartFragment)
				toCheckRows, err := innerSearch(projName, innerStmt)
				if err != nil {
					return nil, err
				}

	      if len(*toCheckRows) > 0 {
	        return nil, errors.New(
	          fmt.Sprintf("The fields '%s' form a unique group and their data taken together is not unique.",
	          strings.Join(ug, ", ")))
	      }
			}
		}


  }


  return dataMap, nil
}


func insertRow(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]
	tableName := vars["tbl"]

	r.FormValue("email")

	toInsert := make(map[string]string)
	for k, _ := range r.PostForm {
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
		printError(w, err)
		return
	}

	toInsert["_version"] = fmt.Sprintf("%d", currentVersionNum)

	dataPath, _ := GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)
	if ! doesPathExists(tablePath) {
		printError(w, errors.New(fmt.Sprintf("Table '%s' of Project '%s' does not exists.", tableName, projName)))
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
  if ! doesPathExists(filepath.Join(tablePath, "lastId")) {
    nextId = 1
  } else {
    raw, err := os.ReadFile(filepath.Join(tablePath, "lastId"))
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

  nextIdStr := strconv.FormatInt(nextId, 10)

  err = saveRowData(projName, tableName, nextIdStr, toInsert)
  if err != nil {
    printError(w, err)
    return
  }

  // create indexes
  for k, v := range toInsert {
    if isNotIndexedField(projName, tableName, k) {
				// do nothing.
    } else {
      err := makeIndex(projName, tableName, k, v, nextIdStr)
      if err != nil {
        printError(w, err)
        return
      }
    }

  }


  // store last id
  err = os.WriteFile(filepath.Join(tablePath, "lastId"), []byte(nextIdStr), 0777)
  if err != nil {
    printError(w, errors.Wrap(err, "ioutil error"))
    return
  }

  fmt.Fprintf(w, nextIdStr)

}


func saveRowData(projName, tableName, rowId string, toWrite map[string]string) error {
  tablePath := getTablePath(projName, tableName)

	out := "\n"
	for k, v := range toWrite {
		ft := flaarum_shared.GetFieldType(projName, tableName, k)
		if ft == "text" {
			out += fmt.Sprintf("%s:\n%s\n%s:\n", k, v, k)
		} else {
			out += fmt.Sprintf("%s: %v\n", k, v)
		}
	}

  err := os.WriteFile(filepath.Join(tablePath, "data", rowId), []byte(out), 0777)
  if err != nil {
    return errors.Wrap(err, "write file failed.")
  }

  return nil
}


func makeIndex(projName, tableName, fieldName, newData, rowId string) error {
  return flaarum_shared.MakeIndex(projName, tableName, fieldName, newData, rowId)
}
