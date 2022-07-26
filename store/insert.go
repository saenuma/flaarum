package main

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/mcnijman/go-emailaddress"
	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/flaarum_shared"
)

func validateAndMutateDataMap(projName, tableName string, dataMap, oldValues map[string]string) (map[string]string, error) {
	tableStruct, err := getCurrentTableStructureParsed(projName, tableName)
	if err != nil {
		return nil, err
	}

	fieldsDescs := make(map[string]flaarum_shared.FieldStruct)
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
			if fd.FieldType == "string" || fd.FieldType == "email" || fd.FieldType == "url" || fd.FieldType == "ipaddr" {
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
				dataMap[k+"_year"] = strconv.Itoa(valueInTimeType.Year())
				dataMap[k+"_month"] = strconv.Itoa(int(valueInTimeType.Month()))
				dataMap[k+"_day"] = strconv.Itoa(valueInTimeType.Day())
			} else if fd.FieldType == "datetime" {
				valueInTimeType, err := time.Parse(flaarum_shared.DATETIME_FORMAT, v)
				if err != nil {
					return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is not in datetime format.", v, k))
				}
				dataMap[k+"_year"] = strconv.Itoa(valueInTimeType.Year())
				dataMap[k+"_month"] = strconv.Itoa(int(valueInTimeType.Month()))
				dataMap[k+"_day"] = strconv.Itoa(valueInTimeType.Day())
				dataMap[k+"_hour"] = strconv.Itoa(valueInTimeType.Hour())
				dataMap[k+"_date"] = valueInTimeType.Format(flaarum_shared.DATE_FORMAT)
				dataMap[k+"_tzname"], _ = valueInTimeType.Zone()
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

		if ok && fd.Required {
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

	// validate unique groups
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

			if !uniqueGroupFieldsEqualityStatus {
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
		printError(w, err)
		return
	}

	toInsert["_version"] = fmt.Sprintf("%d", currentVersionNum)

	dataPath, _ := GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)
	if !doesPathExists(tablePath) {
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
	dataF1Path := filepath.Join(tablePath, "data.flaa1")
	if !doesPathExists(dataF1Path) {
		nextId = 1
	} else {
		elemsMap, err := ParseDataF1File(dataF1Path)
		if err != nil {
			printError(w, err)
			return
		}

		elemsKeys := make([]int64, 0, len(elemsMap))
		for k := range elemsMap {
			elemKey, err := strconv.ParseInt(k, 10, 64)
			if err != nil {
				printError(w, err)
				continue
			}
			elemsKeys = append(elemsKeys, elemKey)
		}

		sort.Slice(elemsKeys, func(i, j int) bool {
			return elemsKeys[i] < elemsKeys[j]
		})

		lastId := elemsKeys[len(elemsKeys)-1]

		nextId = lastId + 1
	}

	nextIdStr := strconv.FormatInt(nextId, 10)

	err = saveRowData(projName, tableName, nextIdStr, toInsert)
	if err != nil {
		printError(w, err)
		return
	}

	// create indexes
	for k, v := range toInsert {
		if !isNotIndexedField(projName, tableName, k) {
			err := MakeIndex(projName, tableName, k, v, nextIdStr)
			if err != nil {
				printError(w, err)
				return
			}
		}

	}

	fmt.Fprint(w, nextIdStr)

}

func saveRowData(projName, tableName, rowId string, toWrite map[string]string) error {
	tablePath := getTablePath(projName, tableName)

	dataLumpPath := filepath.Join(tablePath, "data.flaa2")
	dataForCurrentRow := flaarum_shared.EncodeRowData(projName, tableName, toWrite)
	var begin int64
	var end int64
	if doesPathExists(dataLumpPath) {
		dataLumpHandle, err := os.OpenFile(dataLumpPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return errors.Wrap(err, "os error")
		}
		defer dataLumpHandle.Close()

		stat, err := dataLumpHandle.Stat()
		if err != nil {
			return errors.Wrap(err, "os error")
		}

		size := stat.Size()
		dataLumpHandle.Write([]byte(dataForCurrentRow))
		begin = size
		end = int64(len([]byte(dataForCurrentRow))) + size
	} else {
		err := os.WriteFile(dataLumpPath, []byte(dataForCurrentRow), 0777)
		if err != nil {
			return errors.Wrap(err, "os error")
		}

		begin = 0
		end = int64(len([]byte(dataForCurrentRow)))
	}

	elem := DataF1Elem{rowId, begin, end}
	err := AppendDataF1File(projName, tableName, "data", elem)
	if err != nil {
		return errors.Wrap(err, "os error")
	}

	return nil
}
