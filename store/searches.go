package main

import (
	"net/http"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/flaarum_shared"
	"path/filepath"
	"encoding/json"
	"fmt"
	"strings"
	"github.com/adam-hanna/arrayOperations"
	"sort"
	"os"
	"strconv"
	"time"
)


func searchTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]

	stmtStruct, err := flaarum_shared.ParseSearchStmt(r.FormValue("stmt"))
	if err != nil {
		printError(w, err)
		return
	}

	dataPath, _ := GetDataPath()
	tablePath := filepath.Join(dataPath, projName, stmtStruct.TableName)
	if ! doesPathExists(tablePath) {
		printError(w, errors.New(fmt.Sprintf("Table '%s' of Project '%s' does not exists.", stmtStruct.TableName, projName)))
		return
	}

	rets, err := innerSearch(projName, r.FormValue("stmt"))
	if err != nil {
		printError(w, err)
		return
	}

	if r.FormValue("query-one") == "t" {
		if len(*rets) == 0 {
			printError(w, errors.New("The search returned nothing."))
			return
		}
		jsonBytes, err := json.Marshal((*rets)[0])
		if err != nil {
			printError(w, errors.Wrap(err, "json error"))
			return
		}
		fmt.Fprintf(w, string(jsonBytes))
	} else {
		jsonBytes, err := json.Marshal(rets)
		if err != nil {
			printError(w, errors.Wrap(err, "json error"))
			return
		}
		fmt.Fprintf(w, string(jsonBytes))
	}
}


func findIdsContainingTrueWhereValues(projName, tableName, field string, trueWhereValues []string) ([]string, error) {
  retIds := make([]string, 0)
  for _, tmpId := range trueWhereValues {
    indexesPath := filepath.Join(getTablePath(projName, tableName), "indexes", field, tmpId)
    if _, err := os.Stat(indexesPath); os.IsNotExist(err) {

    } else {
      raw, err := os.ReadFile(indexesPath)
      if err != nil {
        return nil, errors.Wrap(err, "read file failed.")
      }
      retIds = append(retIds, strings.Split(string(raw), "\n")...)
    }

  }
  return retIds, nil
}


func getNeededIdsFromIntIndexes(container []flaarum_shared.IntIndexes) []string {
	retInts := make([]int64, 0)
	for _, item := range container {
		retInts = append(retInts, item.Ids...)
	}

	ret := make([]string, 0)
	for _, intYeah := range retInts {
		ret = append(ret, strconv.FormatInt(intYeah, 10))
	}

	return ret
}


func getNeededIdsFromTimeIndexes(container []flaarum_shared.TimeIndexes) []string {
	retInts := make([]int64, 0)
	for _, item := range container {
		retInts = append(retInts, item.Ids...)
	}

	ret := make([]string, 0)
	for _, intYeah := range retInts {
		ret = append(ret, strconv.FormatInt(intYeah, 10))
	}

	return ret
}


func innerSearch(projName, stmt string) (*[]map[string]string, error) {
	stmtStruct, err := flaarum_shared.ParseSearchStmt(stmt)
	if err != nil {
		return nil, err
	}

	dataPath, _ := GetDataPath()
	tablePath := filepath.Join(dataPath, projName, stmtStruct.TableName)
	tableName := stmtStruct.TableName

  createTableMutexIfNecessary(projName, stmtStruct.TableName)
  fullTableName := projName + ":" + stmtStruct.TableName
  tablesMutexes[fullTableName].RLock()
  defer tablesMutexes[fullTableName].RUnlock()

	expDetails := make(map[string]string)

	tableStruct, err := getCurrentTableStructureParsed(projName, stmtStruct.TableName)
	if err != nil {
		return nil, err
	}

	if stmtStruct.Expand {
		for _, fKeyStruct := range tableStruct.ForeignKeys {
      if ! doesPathExists(filepath.Join(dataPath, projName, fKeyStruct.PointedTable)) {
        return nil, errors.New(fmt.Sprintf("table '%s' of project '%s' does not exists.", fKeyStruct.PointedTable, projName))
      }
			expDetails[fKeyStruct.FieldName] = fKeyStruct.PointedTable
		}
	}

	retIds := make([]string, 0)

	if len(stmtStruct.WhereOptions) == 0 {
		dataFIs, err := os.ReadDir(filepath.Join(tablePath, "data"))
		if err != nil {
			return nil, errors.Wrap(err, "ioutil error.")
		}
		for _, dataFI := range dataFIs {
			retIds = append(retIds, dataFI.Name())
		}

	} else {
		// validation
		fieldNamesToFieldTypes := make(map[string]string)
		fieldNamesToNotIndexedStatus := make(map[string]bool)

		for _, fieldStruct := range tableStruct.Fields {
			fieldNamesToFieldTypes[fieldStruct.FieldName] = fieldStruct.FieldType
		}
		for _, fieldStruct := range tableStruct.Fields {
			fieldNamesToNotIndexedStatus[fieldStruct.FieldName] = fieldStruct.NotIndexed
		}
		for i, whereStruct := range stmtStruct.WhereOptions {
			if i != 0 {
				if whereStruct.Joiner != "and" && whereStruct.Joiner != "or" {
					return nil, errors.New("Invalid statement: joiner must be one of 'and', 'or'.")
				}
			}

			if fieldNamesToFieldTypes[whereStruct.FieldName] == "string" || fieldNamesToFieldTypes[whereStruct.FieldName] == "text" ||
				fieldNamesToFieldTypes[whereStruct.FieldName] == "bool" || fieldNamesToFieldTypes[whereStruct.FieldName] == "ipaddr" ||
				fieldNamesToFieldTypes[whereStruct.FieldName] == "email" || fieldNamesToFieldTypes[whereStruct.FieldName] == "url" {

					if whereStruct.Relation == ">" || whereStruct.Relation == ">=" || whereStruct.Relation == "<" || whereStruct.Relation == "<=" {
						return nil, errors.New(fmt.Sprintf("Invalid statement: The type '%s' does not support the query relation '%s'",
							fieldNamesToFieldTypes[whereStruct.FieldName], whereStruct.Relation))
					}

				}

			if fieldNamesToFieldTypes[whereStruct.FieldName] != "string" && whereStruct.Relation == "like" {
				return nil, errors.New(fmt.Sprintf("The field type '%s' does not support the query relation 'like'", fieldNamesToFieldTypes[whereStruct.FieldName]))
			}

			if whereStruct.FieldName == "id" {
				if whereStruct.Relation == ">" || whereStruct.Relation == ">=" || whereStruct.Relation == "<" || whereStruct.Relation == "<=" {
					return nil, errors.New(fmt.Sprintf("Invalid statement: The 'id' field does not support the query relation '%s'",
						whereStruct.Relation))
				}
				if whereStruct.Relation == "like" {
					return nil, errors.New("Invalid statment: the 'id' field does not support the query relation 'like'")
				}
			}
			if fieldNamesToNotIndexedStatus[whereStruct.FieldName] == true {
				return nil, errors.New(fmt.Sprintf("The field '%s' is not searchable because it has the 'nindex' attribute",
					whereStruct.FieldName))
			}

		}

		// main search
		beforeFilter := make([][]string, 0)

		for _, whereStruct := range stmtStruct.WhereOptions {
			if whereStruct.Relation == "=" {

        if whereStruct.FieldName == "id" {
          beforeFilter = append(beforeFilter, []string{whereStruct.FieldValue})
        } else if strings.Contains(whereStruct.FieldName, ".") {
          trueWhereValues := make([]string, 0)
          parts := strings.Split(whereStruct.FieldName, ".")

          indexFileName := makeSafeIndexName(whereStruct.FieldValue)
          pTbl, ok := expDetails[parts[0]]
          if ! ok {
            continue
          }
          indexesPath := filepath.Join(getTablePath(projName, pTbl), "indexes", parts[1], indexFileName)
          if _, err := os.Stat(indexesPath); os.IsNotExist(err) {
            // do nothing
          } else {
            raw, err := os.ReadFile(indexesPath)
            if err != nil {
              return nil, errors.Wrap(err, "read file failed.")
            }
            trueWhereValues = strings.Split(string(raw), "\n")
          }

          stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
          if err != nil {
            return nil, err
          }
          beforeFilter = append(beforeFilter, stringIds)

        } else {
          indexFileName := makeSafeIndexName(whereStruct.FieldValue)
          indexesPath := filepath.Join(tablePath, "indexes", whereStruct.FieldName, indexFileName)
          if _, err := os.Stat(indexesPath); os.IsNotExist(err) {
            beforeFilter = append(beforeFilter, make([]string, 0))
          } else {
            raw, err := os.ReadFile(indexesPath)
            if err != nil {
              return nil, errors.Wrap(err, "read file failed.")
            }
            beforeFilter = append(beforeFilter, strings.Split(string(raw), "\n"))
          }
        }

      } else if whereStruct.Relation == "!=" {
        if whereStruct.FieldName == "id" {
          rows, err := os.ReadDir(filepath.Join(tablePath, "data"))
          if err != nil {
            return nil, errors.Wrap(err, "read file failed.")
          }
          stringIds := make([]string, 0)
          for _, row := range rows {
            if row.Name() != whereStruct.FieldValue {
              stringIds = append(stringIds, row.Name())
            }
          }
          beforeFilter = append(beforeFilter, stringIds)
        } else if strings.Contains(whereStruct.FieldName, ".") {
          trueWhereValues := make([]string, 0)
          parts := strings.Split(whereStruct.FieldName, ".")

          indexFileName := makeSafeIndexName(whereStruct.FieldValue)
          pTbl, ok := expDetails[parts[0]]
          if ! ok {
            continue
          }

          allIndexes, err := os.ReadDir(filepath.Join(getTablePath(projName, pTbl), "indexes", parts[1]))
          if err != nil {
            return nil, errors.Wrap(err, "read dir failed.")
          }
          for _, indexFI := range allIndexes {
            if indexFI.Name() != indexFileName {
              raw, err := os.ReadFile(filepath.Join(getTablePath(projName, pTbl), "indexes", parts[1], indexFI.Name()))
              if err != nil {
                return nil, errors.Wrap(err, "read file failed.")
              }
              trueWhereValues = arrayOperations.UnionString(trueWhereValues, strings.Split(string(raw), "\n"))
            }
          }

          stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
          if err != nil {
            return nil, err
          }
          beforeFilter = append(beforeFilter, stringIds)

        } else {
          indexFileName := makeSafeIndexName(whereStruct.FieldValue)
          allIndexes, err := os.ReadDir(filepath.Join(tablePath, "indexes", whereStruct.FieldName))
          if err != nil {
            return nil, errors.Wrap(err, "read dir failed.")
          }
          stringIds := make([]string, 0)
          for _, indPath := range allIndexes {
            if indPath.Name() != indexFileName {
              raw, err := os.ReadFile(filepath.Join(tablePath, "indexes", whereStruct.FieldName, indPath.Name()))
              if err != nil {
                return nil, errors.Wrap(err, "read file failed.")
              }
              stringIds = arrayOperations.UnionString(stringIds, strings.Split(string(raw), "\n"))
            }
          }
          beforeFilter = append(beforeFilter, stringIds)
        }

      } else if whereStruct.Relation == ">" || whereStruct.Relation == ">=" {

        if strings.Contains(whereStruct.FieldName, ".") {
          trueWhereValues := make([]string, 0)
          parts := strings.Split(whereStruct.FieldName, ".")

          pTbl, ok := expDetails[parts[0]]
          if ! ok {
            continue
          }

					resolvedFieldName := parts[1]

					currentFieldType := flaarum_shared.GetFieldType(projName, pTbl, resolvedFieldName)

					if currentFieldType == "date" || currentFieldType == "datetime" {
						timeIndexesFile := filepath.Join(getTablePath(projName, pTbl), "timeindexes", resolvedFieldName)
						timeIndexes, err := flaarum_shared.ReadTimeIndexesFromFile(timeIndexesFile, currentFieldType)
						if err != nil {
							return nil, err
						}

						var whereStructFieldValueTime time.Time
						if currentFieldType == "date" {
							whereStructFieldValueTime, err = time.Parse(flaarum_shared.DATE_FORMAT, whereStruct.FieldValue)
							if err != nil {
								return nil, errors.Wrap(err, "time parsing error")
							}
						} else {
							whereStructFieldValueTime, err = time.Parse(flaarum_shared.DATETIME_FORMAT, whereStruct.FieldValue)
							if err != nil {
								return nil, errors.Wrap(err, "time parsing error")
							}
						}

						exactMatch := false
						brokeLoop := false
						index := 0
						for i, elem := range timeIndexes {
							if elem.GoTimeValue.Equal(whereStructFieldValueTime) {
								exactMatch = true
								brokeLoop = true
								index = i
								break
							}
							if elem.GoTimeValue.After(whereStructFieldValueTime) {
								index = i
								brokeLoop = true
								break
							}
						}


						if brokeLoop && exactMatch {
							if whereStruct.Relation == ">" {
								newIndex := index + 1
								if len(timeIndexes) != newIndex {
									elems := getNeededIdsFromTimeIndexes(timeIndexes[newIndex: ])
									trueWhereValues = append(trueWhereValues, elems...)
								}
							} else if whereStruct.Relation == ">=" {
								elems := getNeededIdsFromTimeIndexes(timeIndexes[index: ])
								trueWhereValues = append(trueWhereValues, elems...)
							}
						} else if brokeLoop {
							elems := getNeededIdsFromTimeIndexes(timeIndexes[index: ])
							trueWhereValues = append(trueWhereValues, elems...)
						}

	          stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
	          if err != nil {
	            return nil, err
	          }

						beforeFilter = append(beforeFilter, stringIds)

					} else if currentFieldType == "int" || currentFieldType == "float" {


						intIndexesFile := filepath.Join(getTablePath(projName, pTbl), "intindexes", resolvedFieldName)
						intIndexes, err := flaarum_shared.ReadIntIndexesFromFile(intIndexesFile)
						if err != nil {
							return nil, err
						}

						var whereStructFieldValueInt int64
						if flaarum_shared.GetFieldType(projName, tableName, whereStruct.FieldName) == "float" {
							tmp, err := strconv.ParseFloat(whereStruct.FieldValue, 64)
							if err != nil {
								return nil, errors.Wrap(err, "strconv error")
							}
							whereStructFieldValueInt = int64(tmp)
						} else {
							whereStructFieldValueInt, err = strconv.ParseInt(whereStruct.FieldValue, 10, 64)
							if err != nil {
								return nil, errors.Wrap(err, "strconv error")
							}
						}

						exactMatch := false
						brokeLoop := false
						index := 0
						for i, elem := range intIndexes {
							if elem.IntIndex == whereStructFieldValueInt {
								exactMatch = true
								brokeLoop = true
								index = i
								break
							}
							if elem.IntIndex > whereStructFieldValueInt {
								index = i
								brokeLoop = true
								break
							}
						}


						if brokeLoop && exactMatch {
							if whereStruct.Relation == ">" {
								newIndex := index + 1
								if len(intIndexes) != newIndex {
									elems := getNeededIdsFromIntIndexes(intIndexes[newIndex: ])
									trueWhereValues = append(trueWhereValues, elems...)
								}
							} else if whereStruct.Relation == ">=" {
								elems := getNeededIdsFromIntIndexes(intIndexes[index: ])
								trueWhereValues = append(trueWhereValues, elems...)
							}
						} else if brokeLoop {
							elems := getNeededIdsFromIntIndexes(intIndexes[index: ])
							trueWhereValues = append(trueWhereValues, elems...)
						}

	          stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
	          if err != nil {
	            return nil, err
	          }

	          beforeFilter = append(beforeFilter, stringIds)
					}

        } else {
					stringIds := make([]string, 0)

					currentFieldType := flaarum_shared.GetFieldType(projName, tableName, whereStruct.FieldName)

					if currentFieldType == "date" || currentFieldType == "datetime" {
						timeIndexesFile := filepath.Join(dataPath, projName, tableName, "timeindexes", whereStruct.FieldName)
						timeIndexes, err := flaarum_shared.ReadTimeIndexesFromFile(timeIndexesFile, currentFieldType)
						if err != nil {
							return nil, err
						}

						var whereStructFieldValueTime time.Time
						if currentFieldType == "date" {
							whereStructFieldValueTime, err = time.Parse(flaarum_shared.DATE_FORMAT, whereStruct.FieldValue)
							if err != nil {
								return nil, errors.Wrap(err, "time parsing error")
							}
						} else {
							whereStructFieldValueTime, err = time.Parse(flaarum_shared.DATETIME_FORMAT, whereStruct.FieldValue)
							if err != nil {
								return nil, errors.Wrap(err, "time parsing error")
							}
						}

						exactMatch := false
						brokeLoop := false
						index := 0
						for i, elem := range timeIndexes {
							if elem.GoTimeValue.Equal(whereStructFieldValueTime) {
								exactMatch = true
								brokeLoop = true
								index = i
								break
							}
							if elem.GoTimeValue.After(whereStructFieldValueTime) {
								index = i
								brokeLoop = true
								break
							}
						}


						if brokeLoop && exactMatch {
							if whereStruct.Relation == ">" {
								newIndex := index + 1
								if len(timeIndexes) != newIndex {
									elems := getNeededIdsFromTimeIndexes(timeIndexes[newIndex: ])
									stringIds = append(stringIds, elems...)
								}
							} else if whereStruct.Relation == ">=" {
								elems := getNeededIdsFromTimeIndexes(timeIndexes[index: ])
								stringIds = append(stringIds, elems...)
							}
						} else if brokeLoop {
							elems := getNeededIdsFromTimeIndexes(timeIndexes[index: ])
							stringIds = append(stringIds, elems...)
						}

						beforeFilter = append(beforeFilter, stringIds)

					} else if currentFieldType == "int" || currentFieldType == "float" {

						intIndexesFile := filepath.Join(dataPath, projName, tableName, "intindexes", whereStruct.FieldName)
						intIndexes, err := flaarum_shared.ReadIntIndexesFromFile(intIndexesFile)
						if err != nil {
							return nil, err
						}

						var whereStructFieldValueInt int64
						if flaarum_shared.GetFieldType(projName, tableName, whereStruct.FieldName) == "float" {
							tmp, err := strconv.ParseFloat(whereStruct.FieldValue, 64)
							if err != nil {
								return nil, errors.Wrap(err, "strconv error")
							}
							whereStructFieldValueInt = int64(tmp)
						} else {
							whereStructFieldValueInt, err = strconv.ParseInt(whereStruct.FieldValue, 10, 64)
							if err != nil {
								return nil, errors.Wrap(err, "strconv error")
							}
						}

						exactMatch := false
						brokeLoop := false
						index := 0
						for i, elem := range intIndexes {
							if elem.IntIndex == whereStructFieldValueInt {
								exactMatch = true
								brokeLoop = true
								index = i
								break
							}
							if elem.IntIndex > whereStructFieldValueInt {
								index = i
								brokeLoop = true
								break
							}
						}


						if brokeLoop && exactMatch {
							if whereStruct.Relation == ">" {
								newIndex := index + 1
								if len(intIndexes) != newIndex {
									elems := getNeededIdsFromIntIndexes(intIndexes[newIndex: ])
									stringIds = append(stringIds, elems...)
								}
							} else if whereStruct.Relation == ">=" {
								elems := getNeededIdsFromIntIndexes(intIndexes[index: ])
								stringIds = append(stringIds, elems...)
							}
						} else if brokeLoop {
							elems := getNeededIdsFromIntIndexes(intIndexes[index: ])
							stringIds = append(stringIds, elems...)
						}

						beforeFilter = append(beforeFilter, stringIds)

					}
        }


      } else if whereStruct.Relation == "<" || whereStruct.Relation == "<=" {

				if strings.Contains(whereStruct.FieldName, ".") {
          trueWhereValues := make([]string, 0)
          parts := strings.Split(whereStruct.FieldName, ".")

          pTbl, ok := expDetails[parts[0]]
          if ! ok {
            continue
          }

					resolvedFieldName := parts[1]

					currentFieldType := flaarum_shared.GetFieldType(projName, pTbl, resolvedFieldName)

					if currentFieldType == "date" || currentFieldType == "datetime" {
						timeIndexesFile := filepath.Join(getTablePath(projName, pTbl), "timeindexes", resolvedFieldName)
						timeIndexes, err := flaarum_shared.ReadTimeIndexesFromFile(timeIndexesFile, currentFieldType)
						if err != nil {
							return nil, err
						}

						var whereStructFieldValueTime time.Time
						if currentFieldType == "date" {
							whereStructFieldValueTime, err = time.Parse(flaarum_shared.DATE_FORMAT, whereStruct.FieldValue)
							if err != nil {
								return nil, errors.Wrap(err, "time parsing error")
							}
						} else {
							whereStructFieldValueTime, err = time.Parse(flaarum_shared.DATETIME_FORMAT, whereStruct.FieldValue)
							if err != nil {
								return nil, errors.Wrap(err, "time parsing error")
							}
						}

						exactMatch := false
						brokeLoop := false
						index := 0
						for i, elem := range timeIndexes {
							if elem.GoTimeValue.Equal(whereStructFieldValueTime) {
								exactMatch = true
								brokeLoop = true
								index = i
								break
							}
							if elem.GoTimeValue.After(whereStructFieldValueTime) {
								index = i
								brokeLoop = true
								break
							}
						}


						if brokeLoop && exactMatch {
							if whereStruct.Relation == "<=" {
								newIndex := index + 1
								if len(timeIndexes) != newIndex {
									elems := getNeededIdsFromTimeIndexes(timeIndexes[0 : newIndex])
									trueWhereValues = append(trueWhereValues, elems...)
								}
							} else if whereStruct.Relation == "<" {
								elems := getNeededIdsFromTimeIndexes(timeIndexes[0 : index])
								trueWhereValues = append(trueWhereValues, elems...)
							}
						} else if brokeLoop {
							elems := getNeededIdsFromTimeIndexes(timeIndexes[0 : index])
							trueWhereValues = append(trueWhereValues, elems...)
						}

						stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
						if err != nil {
							return nil, err
						}

						beforeFilter = append(beforeFilter, stringIds)

					} else if currentFieldType == "int" || currentFieldType == "float" {

						intIndexesFile := filepath.Join(getTablePath(projName, pTbl), "intindexes", resolvedFieldName)
						intIndexes, err := flaarum_shared.ReadIntIndexesFromFile(intIndexesFile)
						if err != nil {
							return nil, err
						}

						var whereStructFieldValueInt int64
						if flaarum_shared.GetFieldType(projName, tableName, whereStruct.FieldName) == "float" {
							tmp, err := strconv.ParseFloat(whereStruct.FieldValue, 64)
							if err != nil {
								return nil, errors.Wrap(err, "strconv error")
							}
							whereStructFieldValueInt = int64(tmp)
						} else {
							whereStructFieldValueInt, err = strconv.ParseInt(whereStruct.FieldValue, 10, 64)
							if err != nil {
								return nil, errors.Wrap(err, "strconv error")
							}
						}

						exactMatch := false
						brokeLoop := false
						index := 0
						for i, elem := range intIndexes {
							if elem.IntIndex == whereStructFieldValueInt {
								exactMatch = true
								brokeLoop = true
								index = i
								break
							}
							if elem.IntIndex > whereStructFieldValueInt {
								index = i
								brokeLoop = true
								break
							}
						}


						if brokeLoop && exactMatch {
							if whereStruct.Relation == "<=" {
								newIndex := index + 1
								if len(intIndexes) != newIndex {
									elems := getNeededIdsFromIntIndexes(intIndexes[0: newIndex])
									trueWhereValues = append(trueWhereValues, elems...)
								}
							} else if whereStruct.Relation == "<" {
								elems := getNeededIdsFromIntIndexes(intIndexes[0: index ])
								trueWhereValues = append(trueWhereValues, elems...)
							}
						} else if brokeLoop {
							elems := getNeededIdsFromIntIndexes(intIndexes[0 : index ])
							trueWhereValues = append(trueWhereValues, elems...)
						}

	          stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
	          if err != nil {
	            return nil, err
	          }

	          beforeFilter = append(beforeFilter, stringIds)
					}

        } else {
					stringIds := make([]string, 0)

					currentFieldType := flaarum_shared.GetFieldType(projName, tableName, whereStruct.FieldName)

					if currentFieldType == "date" || currentFieldType == "datetime" {
						timeIndexesFile := filepath.Join(dataPath, projName, tableName, "intindexes", whereStruct.FieldName)
						timeIndexes, err := flaarum_shared.ReadTimeIndexesFromFile(timeIndexesFile, currentFieldType)
						if err != nil {
							return nil, err
						}

						var whereStructFieldValueTime time.Time
						if currentFieldType == "date" {
							whereStructFieldValueTime, err = time.Parse(flaarum_shared.DATE_FORMAT, whereStruct.FieldValue)
							if err != nil {
								return nil, errors.Wrap(err, "time parsing error")
							}
						} else {
							whereStructFieldValueTime, err = time.Parse(flaarum_shared.DATETIME_FORMAT, whereStruct.FieldValue)
							if err != nil {
								return nil, errors.Wrap(err, "time parsing error")
							}
						}

						exactMatch := false
						brokeLoop := false
						index := 0
						for i, elem := range timeIndexes {
							if elem.GoTimeValue.Equal(whereStructFieldValueTime) {
								exactMatch = true
								brokeLoop = true
								index = i
								break
							}
							if elem.GoTimeValue.After(whereStructFieldValueTime) {
								index = i
								brokeLoop = true
								break
							}
						}


						if brokeLoop && exactMatch {
							if whereStruct.Relation == "<=" {
								newIndex := index + 1
								if len(timeIndexes) != newIndex {
									elems := getNeededIdsFromTimeIndexes(timeIndexes[0 : newIndex])
									stringIds = append(stringIds, elems...)
								}
							} else if whereStruct.Relation == "<" {
								elems := getNeededIdsFromTimeIndexes(timeIndexes[0 : index])
								stringIds = append(stringIds, elems...)
							}
						} else if brokeLoop {
							elems := getNeededIdsFromTimeIndexes(timeIndexes[0 : index])
							stringIds = append(stringIds, elems...)
						}

						beforeFilter = append(beforeFilter, stringIds)

					} else if currentFieldType == "int" || currentFieldType == "float" {

						intIndexesFile := filepath.Join(dataPath, projName, tableName, "intindexes", whereStruct.FieldName)
						intIndexes, err := flaarum_shared.ReadIntIndexesFromFile(intIndexesFile)
						if err != nil {
							return nil, err
						}

						var whereStructFieldValueInt int64
						if flaarum_shared.GetFieldType(projName, tableName, whereStruct.FieldName) == "float" {
							tmp, err := strconv.ParseFloat(whereStruct.FieldValue, 64)
							if err != nil {
								return nil, errors.Wrap(err, "strconv error")
							}
							whereStructFieldValueInt = int64(tmp)
						} else {
							whereStructFieldValueInt, err = strconv.ParseInt(whereStruct.FieldValue, 10, 64)
							if err != nil {
								return nil, errors.Wrap(err, "strconv error")
							}
						}

						exactMatch := false
						brokeLoop := false
						index := 0
						for i, elem := range intIndexes {
							if elem.IntIndex == whereStructFieldValueInt {
								exactMatch = true
								brokeLoop = true
								index = i
								break
							}
							if elem.IntIndex > whereStructFieldValueInt {
								index = i
								brokeLoop = true
								break
							}
						}


						if brokeLoop && exactMatch {
							if whereStruct.Relation == "<=" {
								newIndex := index + 1
								if len(intIndexes) != newIndex {
									elems := getNeededIdsFromIntIndexes(intIndexes[0 : newIndex])
									stringIds = append(stringIds, elems...)
								}
							} else if whereStruct.Relation == "<" {
								elems := getNeededIdsFromIntIndexes(intIndexes[0 : index])
								stringIds = append(stringIds, elems...)
							}
						} else if brokeLoop {
							elems := getNeededIdsFromIntIndexes(intIndexes[0 : index])
							stringIds = append(stringIds, elems...)
						}

						beforeFilter = append(beforeFilter, stringIds)
	        }
				}

      } else if whereStruct.Relation == "in" {

        stringIds := make([]string, 0)

        if whereStruct.FieldName == "id" {
          stringIds = whereStruct.FieldValues

        } else if strings.Contains(whereStruct.FieldName, ".") {

          trueWhereValues := make([]string, 0)
          parts := strings.Split(whereStruct.FieldName, ".")

          for _, inval := range whereStruct.FieldValues {

            indexFileName := makeSafeIndexName(inval)
            pTbl, ok := expDetails[parts[0]]
            if ! ok {
              continue
            }
            indexesPath := filepath.Join(getTablePath(projName, pTbl), "indexes", parts[1], indexFileName)
            if _, err := os.Stat(indexesPath); os.IsNotExist(err) {
              // do nothing
            } else {
              raw, err := os.ReadFile(indexesPath)
              if err != nil {
                return nil, errors.Wrap(err, "read file failed.")
              }
              trueWhereValues = arrayOperations.UnionString(trueWhereValues, strings.Split(string(raw), "\n"))

            }
          }

          stringIds, err = findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
          if err != nil {
            return nil, err
          }
          beforeFilter = append(beforeFilter, stringIds)
        } else {
          for _, inval := range whereStruct.FieldValues {
            indexFileName := makeSafeIndexName(inval)
            indexesPath := filepath.Join(tablePath, "indexes", whereStruct.FieldName, indexFileName)
            if _, err := os.Stat(indexesPath); os.IsNotExist(err) {
              // do nothing
            } else {
              raw, err := os.ReadFile(indexesPath)
              if err != nil {
                return nil, errors.Wrap(err, "read file failed.")
              }
              stringIds = arrayOperations.UnionString(stringIds, strings.Split(string(raw), "\n"))
            }

          }
        }

        beforeFilter = append(beforeFilter, stringIds)

      } else if whereStruct.Relation == "nin" {

        if whereStruct.FieldName == "id" {
          rows, err := os.ReadDir(filepath.Join(tablePath, "data"))
          if err != nil {
            return nil, errors.Wrap(err, "read file failed.")
          }

          allIds := make([]string, 0)
          stringIds := make([]string, 0)
          for _, row := range rows {
            allIds = append(allIds, row.Name())
          }

          for _, idStr := range allIds {
            if flaarum_shared.FindIn(whereStruct.FieldValues, idStr) == -1 {
              stringIds = append(stringIds, idStr)
            }
          }

          beforeFilter = append(beforeFilter, stringIds)
        } else if strings.Contains(whereStruct.FieldName, ".") {
          trueWhereValues := make([]string, 0)

          parts := strings.Split(whereStruct.FieldName, ".")
          pTbl, ok := expDetails[parts[0]]
          if ! ok {
            continue
          }

          safeVals := make([]string, 0)
          for _, val := range whereStruct.FieldValues {
            safeVals = append(safeVals, makeSafeIndexName(val))
          }

          gottenIndexes := make([]string, 0)
          allIndexesFIs, err := os.ReadDir(filepath.Join(getTablePath(projName, pTbl), "indexes", parts[1]))
          if err != nil {
            return nil, errors.Wrap(err, "ioutil error.")
          }
          for _, indexFI := range allIndexesFIs {
            if flaarum_shared.FindIn(safeVals, indexFI.Name()) == -1 {
              gottenIndexes = append(gottenIndexes, indexFI.Name())
            }
          }

          for _, indexedValue := range gottenIndexes {
            raw, err := os.ReadFile(filepath.Join(getTablePath(projName, pTbl), "indexes", parts[1], indexedValue))
            if err != nil {
              return nil, errors.Wrap(err, "ioutil error.")
            }

            trueWhereValues = arrayOperations.UnionString(trueWhereValues, strings.Split(string(raw), "\n"))
          }

          stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
          if err != nil {
            return nil, err
          }
          beforeFilter = append(beforeFilter, stringIds)

        } else {
          stringIds := make([]string, 0)

          safeVals := make([]string, 0)
          for _, val := range whereStruct.FieldValues {
            safeVals = append(safeVals, makeSafeIndexName(val))
          }

          gottenIndexes := make([]string, 0)
          allIndexesFIs, err := os.ReadDir(filepath.Join(tablePath, "indexes", whereStruct.FieldName))
          if err != nil {
            return nil, errors.Wrap(err, "ioutil error.")
          }
          for _, indexFI := range allIndexesFIs {
            if flaarum_shared.FindIn(safeVals, indexFI.Name()) == -1 {
              gottenIndexes = append(gottenIndexes, indexFI.Name())
            }
          }

          for _, indexedValue := range gottenIndexes {
            raw, err := os.ReadFile(filepath.Join(tablePath, "indexes", whereStruct.FieldName, indexedValue))
            if err != nil {
              return nil, errors.Wrap(err, "ioutil error.")
            }

            stringIds = arrayOperations.UnionString(stringIds, strings.Split(string(raw), "\n"))
          }

          beforeFilter = append(beforeFilter, stringIds)

        }


      } else if whereStruct.Relation == "isnull" {

        if strings.Contains(whereStruct.FieldName, ".") {
          parts := strings.Split(whereStruct.FieldName, ".")
          pTbl, ok := expDetails[parts[0]]
          if ! ok {
            continue
          }

          allForeignIds := make([]string, 0)
          allForeignRowFIs, err := os.ReadDir(filepath.Join(getTablePath(projName, pTbl), "data"))
          if err != nil {
            return nil, errors.Wrap(err, "read dir failed.")
          }
          for _, foreignFI := range allForeignRowFIs {
            allForeignIds = append(allForeignIds, foreignFI.Name())
          }

          exemptedIds := make([]string, 0)
          allIndexes, err := os.ReadDir(filepath.Join(getTablePath(projName, pTbl), "indexes", parts[1]))
          if err != nil {
            return nil, errors.Wrap(err, "read dir failed.")
          }
          for _, indexFI := range allIndexes {
            raw, err := os.ReadFile(filepath.Join(getTablePath(projName, pTbl), "indexes", parts[1], indexFI.Name()))
            if err != nil {
              return nil, errors.Wrap(err, "read file failed.")
            }
            exemptedIds = arrayOperations.UnionString(exemptedIds, strings.Split(string(raw), "\n"))
          }

          trueWhereValues := arrayOperations.DifferenceString(allForeignIds, exemptedIds)

          stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
          if err != nil {
            return nil, err
          }
          beforeFilter = append(beforeFilter, stringIds)

        } else {
          rowFIs, err := os.ReadDir(filepath.Join(tablePath, "data"))
          if err != nil {
            return nil, errors.Wrap(err, "read dir failed.")
          }
          allIds := make([]string, 0)
          for _, rowFi := range rowFIs {
            allIds = append(allIds, rowFi.Name())
          }

          allIndexes, err := os.ReadDir(filepath.Join(tablePath, "indexes", whereStruct.FieldName))
          if err != nil {
            return nil, errors.Wrap(err, "read dir failed.")
          }

          exemptedIds := make([]string, 0)
          for _, indexFI := range allIndexes {
            raw, err := os.ReadFile(filepath.Join(tablePath, "indexes", whereStruct.FieldName, indexFI.Name()))
            if err != nil {
              return nil, errors.Wrap(err, "read file failed.")
            }
            exemptedIds = arrayOperations.UnionString(exemptedIds, strings.Split(string(raw), "\n"))
          }

          stringIds := arrayOperations.DifferenceString(allIds, exemptedIds)
          beforeFilter = append(beforeFilter, stringIds)
        }

      } else if whereStruct.Relation == "notnull" {

        stringIds := make([]string, 0)

        if whereStruct.FieldName == "id" {
          rowFIs, err := os.ReadDir(filepath.Join(tablePath, "data"))
          if err != nil {
            return nil, errors.Wrap(err, "read dir failed.")
          }
          for _, rowFi := range rowFIs {
            stringIds = append(stringIds, rowFi.Name())
          }
        } else if strings.Contains(whereStruct.FieldName, ".") {
          trueWhereValues := make([]string, 0)
          parts := strings.Split(whereStruct.FieldName, ".")
          pTbl, ok := expDetails[parts[0]]
          if ! ok {
            continue
          }

          allIndexes, err := os.ReadDir(filepath.Join(getTablePath(projName, pTbl), "indexes", parts[1]))
          if err != nil {
            return nil, errors.Wrap(err, "read file failed.")
          }
          for _, indexFI := range allIndexes {
            raw, err := os.ReadFile(filepath.Join(getTablePath(projName, pTbl), "indexes", parts[1], indexFI.Name()))
            if err != nil {
              return nil, errors.Wrap(err, "read file failed.")
            }
            trueWhereValues = arrayOperations.UnionString(trueWhereValues, strings.Split(string(raw), "\n"))
          }

          stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
          if err != nil {
            return nil, err
          }
          beforeFilter = append(beforeFilter, stringIds)


        } else {
          allIndexes, err := os.ReadDir(filepath.Join(tablePath, "indexes", whereStruct.FieldName))
          if err != nil {
            return nil, errors.Wrap(err, "read dir failed.")
          }

          for _, indexFI := range allIndexes {
            raw, err := os.ReadFile(filepath.Join(tablePath, "indexes", whereStruct.FieldName, indexFI.Name()))
            if err != nil {
              return nil, errors.Wrap(err, "read file failed.")
            }
            stringIds = arrayOperations.UnionString(stringIds, strings.Split(string(raw), "\n"))
          }
        }

        beforeFilter = append(beforeFilter, stringIds)

      } else if whereStruct.Relation == "like" {
				charsOfData := strings.Split(strings.ToLower(whereStruct.FieldValue), "")

				if strings.Contains(whereStruct.FieldName, ".") {
          parts := strings.Split(whereStruct.FieldName, ".")

          pTbl, ok := expDetails[parts[0]]
          if ! ok {
            continue
          }
          indexesPath := filepath.Join(getTablePath(projName, pTbl), "likeindexes", parts[1])

					tmpIds := make([][]string, 0)
					for _, char := range charsOfData {
						if char == "/" {
							continue
						}

						indexesForAChar := filepath.Join(indexesPath, char)
						if flaarum_shared.DoesPathExists(indexesForAChar) {
							raw, err := os.ReadFile(indexesForAChar)
							if err != nil {
								return nil, errors.Wrap(err, "read file failed.")
							}
							tmpIds = append(tmpIds, strings.Split(string(raw), "\n"))
						}
					}

					trueWhereValues := arrayOperations.IntersectString(tmpIds...)

					stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
          if err != nil {
            return nil, err
          }
          beforeFilter = append(beforeFilter, stringIds)

				} else {
					tmpIds := make([][]string, 0)
					for _, char := range charsOfData {
						if char == "/" || char == " " || char == "\t" {
							continue
						}

						indexesForAChar := filepath.Join(dataPath, projName, tableName, "likeindexes", whereStruct.FieldName, char)
						if flaarum_shared.DoesPathExists(indexesForAChar) {
							raw, err := os.ReadFile(indexesForAChar)
							if err != nil {
								return nil, errors.Wrap(err, "read file failed.")
							}
							tmpIds = append(tmpIds, strings.Split(string(raw), "\n"))
						}
					}

					stringIds := arrayOperations.IntersectString(tmpIds...)

					beforeFilter = append(beforeFilter, stringIds)
				}

			} // end of like search

		}

		// do the 'and' / 'or' transformations
		andsCount := 0
		orsCount := 0
		for _, whereStruct := range stmtStruct.WhereOptions {
			if whereStruct.Joiner == "and" {
				andsCount += 1
			} else if whereStruct.Joiner == "or" {
				orsCount += 1
			}
		}

		if andsCount == len(stmtStruct.WhereOptions) - 1 {
			retIds = arrayOperations.IntersectString(beforeFilter...)
		} else if orsCount == len(stmtStruct.WhereOptions) - 1 {
			retIds = arrayOperations.UnionString(beforeFilter...)
		} else {

			beforeUnion := make([][]string, 0)
			index := 1

			for {
				if index > len(stmtStruct.WhereOptions) - 1 {
					break
				}
				whereStruct := stmtStruct.WhereOptions[0]
				if whereStruct.Joiner == "and" {
					tmpIntersected := beforeFilter[index - 1]
					innerIndex := index
					for {
						if stmtStruct.WhereOptions[innerIndex].Joiner == "or" {
							break
						}
						tmpIntersected = arrayOperations.IntersectString(tmpIntersected, beforeFilter[innerIndex])
						innerIndex += 1
						continue
					}
					beforeUnion = append(beforeUnion, tmpIntersected)
					index = innerIndex

				} else if whereStruct.Joiner == "or" {
					if index == 1 {
						beforeUnion = append(beforeUnion, beforeFilter[0])
					}
					if stmtStruct.WhereOptions[index+1].Joiner == "and" {
						index += 1
						continue
					} else if stmtStruct.WhereOptions[index+1].Joiner == "or" {
						beforeUnion = append(beforeUnion, beforeFilter[index])
					}
				}


				index += 1
				continue
			}

			retIds = arrayOperations.UnionString(beforeUnion...)
		}
	}

	tmpRet := make([]map[string]string, 0)
  for _, retId := range retIds {
    rowMap := make(map[string]string)
    raw, err := os.ReadFile(filepath.Join(tablePath, "data", retId))
    if err != nil {
      continue
    }
    err = json.Unmarshal(raw, &rowMap)
    if err != nil {
      return nil, errors.Wrap(err, "json error.")
    }

    for field, data := range rowMap {

      pTbl, ok := expDetails[field]
      if ok {
        rowMap2 := make(map[string]string)
        raw2, err := os.ReadFile(filepath.Join(dataPath, projName, pTbl, "data", data))
        if err != nil {
          continue
        }
        err = json.Unmarshal(raw2, &rowMap2)
        if err != nil {
          return nil, errors.Wrap(err, "json error.")
        }
        for f, d := range rowMap2 {
          rowMap[field + "." + f] = d
        }
      }
    }

    rowMap["id"] = retId
    tmpRet = append(tmpRet, rowMap)
  }

  elems := tmpRet
  if stmtStruct.OrderBy != "" {
    if stmtStruct.OrderDirection == "asc" {
      sort.SliceStable(elems, func(i, j int) bool {
        if confirmFieldType(projName, tableName, stmtStruct.OrderBy, "int", elems[i]["_version"]) &&
        confirmFieldType(projName, tableName, stmtStruct.OrderBy, "int", elems[j]["_version"]) {
          x, err1 := strconv.ParseInt(elems[i][stmtStruct.OrderBy], 10, 64)
          y, err2 := strconv.ParseInt(elems[j][stmtStruct.OrderBy], 10, 64)
          if err1 == nil && err2 == nil {
            return x < y
          } else {
            return elems[i][stmtStruct.OrderBy] < elems[j][stmtStruct.OrderBy]
          }
        } else if confirmFieldType(projName, tableName, stmtStruct.OrderBy, "float", elems[i]["_version"]) &&
        confirmFieldType(projName, tableName, stmtStruct.OrderBy, "float", elems[j]["_version"]) {
          x, err1 := strconv.ParseFloat(elems[i][stmtStruct.OrderBy], 64)
          y, err2 := strconv.ParseFloat(elems[j][stmtStruct.OrderBy], 64)
          if err1 == nil && err2 == nil {
            return x < y
          } else {
            return elems[i][stmtStruct.OrderBy] < elems[j][stmtStruct.OrderBy]
          }
        } else {
          return elems[i][stmtStruct.OrderBy] < elems[j][stmtStruct.OrderBy]
        }
      })
    } else {
      sort.SliceStable(elems, func(i, j int) bool {
        if confirmFieldType(projName, tableName, stmtStruct.OrderBy, "int", elems[i]["_version"]) &&
        confirmFieldType(projName, tableName, stmtStruct.OrderBy, "int", elems[j]["_version"]) {
          x, err1 := strconv.ParseInt(elems[i][stmtStruct.OrderBy], 10, 64)
          y, err2 := strconv.ParseInt(elems[j][stmtStruct.OrderBy], 10, 64)
          if err1 == nil && err2 == nil {
            return x > y
          } else {
            return elems[i][stmtStruct.OrderBy] > elems[j][stmtStruct.OrderBy]
          }
        } else if confirmFieldType(projName, tableName, stmtStruct.OrderBy, "float", elems[i]["_version"]) &&
        confirmFieldType(projName, tableName, stmtStruct.OrderBy, "float", elems[j]["_version"]) {
          x, err1 := strconv.ParseFloat(elems[i][stmtStruct.OrderBy], 64)
          y, err2 := strconv.ParseFloat(elems[j][stmtStruct.OrderBy], 64)
          if err1 == nil && err2 == nil {
            return x > y
          } else {
            return elems[i][stmtStruct.OrderBy] > elems[j][stmtStruct.OrderBy]
          }
        } else {
          return elems[i][stmtStruct.OrderBy] > elems[j][stmtStruct.OrderBy]
        }
      })
    }
  }

  // limits and start_index
  limitedRet := make([]map[string]string, 0)
  if stmtStruct.StartIndex != 0 {
    for i, toOut := range elems {
      if int64(i) >= stmtStruct.StartIndex {
        limitedRet = append(limitedRet, toOut)
      }
    }
  } else {
    limitedRet = elems
  }

  limitedRet2 := make([]map[string]string, 0)
  if stmtStruct.Limit != 0 {
    for i, toOut := range limitedRet {
      if int64(i) == stmtStruct.Limit {
        break
      }
      limitedRet2 = append(limitedRet2, toOut)
    }
  } else {
    limitedRet2 = limitedRet
  }

  // selected fields
  beforeDistinct := make([]map[string]string, 0)
  if len(stmtStruct.Fields) != 0 {
    for _, toOut := range limitedRet2 {
      newOut := make(map[string]string)
      for field, _ := range toOut {
        if strings.HasSuffix(field, ".id") || strings.HasSuffix(field, "._version") {
          newOut[field] = toOut[field]
        }
      }
      for _, field := range stmtStruct.Fields {
        newOut[field] = toOut[field]
      }
      newOut["_version"] = toOut["_version"]
      newOut["id"] = toOut["id"]
      beforeDistinct = append(beforeDistinct, newOut)
    }
  } else {
    beforeDistinct = limitedRet2
  }

  ret := make([]map[string]string, 0)
  if stmtStruct.Distinct {
    distinctWatch := make(map[string]int) // hash[index]
    for i, datum := range beforeDistinct {
      cleanerDatum := make(map[string]string)
      for k, v := range datum {
        if k == "id" || k == "_version" {
          continue
        }
        if strings.HasSuffix(k, ".id") || strings.HasSuffix(k, "._version") {
          continue
        }
        cleanerDatum[k] = v
      }
      jsonBytes, _ := json.Marshal(cleanerDatum)
      h := MakeHash(string(jsonBytes))
      _, ok := distinctWatch[h]
      if ! ok {
        distinctWatch[h] = i
      }
    }

    for _, v := range distinctWatch {
      ret = append(ret, beforeDistinct[v])
    }
  } else {
    ret = beforeDistinct
  }

	return &ret, nil
}
