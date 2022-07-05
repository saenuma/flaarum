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
	arrayOperations "github.com/adam-hanna/arrayOperations"
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
		dataF1Path := filepath.Join(tablePath, "data.flaa1")
	  elemsMap, err := flaarum_shared.ParseDataF1File(dataF1Path)
	  if err != nil {
			return nil, errors.Wrap(err, "ioutil error.")
	  }

		for k, _ := range elemsMap {
			retIds = append(retIds, k)
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

          pTbl, ok := expDetails[parts[0]]
          if ! ok {
            continue
          }

					indexesF1Path := filepath.Join(getTablePath(projName, pTbl), parts[1] + "_indexes.flaa1")

					if doesPathExists(indexesF1Path) {
						elemsMap, err := flaarum_shared.ParseDataF1File(indexesF1Path)
						if err != nil {
							return nil, err
						}
						elemHandle, ok := elemsMap[whereStruct.FieldValue]
						if ok {
							readBytes, err := flaarum_shared.ReadPortionF2File(projName, pTbl, parts[1] + "_indexes",
								elemHandle.DataBegin, elemHandle.DataEnd)
							if err != nil {
								fmt.Printf("%+v\n", err)
							}
							trueWhereValues = append(trueWhereValues, strings.Split(string(readBytes), ",")...)
						}
          }

					stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
					if err != nil {
						return nil, err
					}
					beforeFilter = append(beforeFilter, stringIds)


        } else {
					indexesF1Path := filepath.Join(tablePath, whereStruct.FieldName + "_indexes.flaa1")

					if doesPathExists(indexesF1Path) {
						elemsMap, err := flaarum_shared.ParseDataF1File(indexesF1Path)
						if err != nil {
							return nil, err
						}
						elemHandle, ok := elemsMap[whereStruct.FieldValue]
						if ok {
							readBytes, err := flaarum_shared.ReadPortionF2File(projName, tableName,
								whereStruct.FieldName + "_indexes", elemHandle.DataBegin, elemHandle.DataEnd)
							if err != nil {
								fmt.Printf("%+v\n", err)
							}
							beforeFilter = append(beforeFilter, strings.Split(string(readBytes), ","))
						}
          }
        }

      } else if whereStruct.Relation == "!=" {
        if whereStruct.FieldName == "id" {
					dataF1Path := filepath.Join(tablePath, "data.flaa1")

					elemsMap, err := flaarum_shared.ParseDataF1File(dataF1Path)
					if err != nil {
						return nil, err
					}

					stringIds := make([]string, 0)

					for k, _ := range elemsMap {
						if k != whereStruct.FieldValue {
							stringIds = append(stringIds, k)
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

					otherTableindexesF1Path := filepath.Join(getTablePath(projName, pTbl), parts[1] + "_indexes.flaa1")

					if doesPathExists(otherTableindexesF1Path) {
						elemsMap, err := flaarum_shared.ParseDataF1File(otherTableindexesF1Path)
						if err != nil {
							return nil, err
						}
						for k, elem := range elemsMap {
							if k != whereStruct.FieldValue {
								readBytes, err := flaarum_shared.ReadPortionF2File(projName, pTbl,
									parts[1] + "_indexes", elem.DataBegin, elem.DataEnd)
								if err != nil {
									fmt.Printf("%+v\n", err)
								}
								trueWhereValues = append(trueWhereValues, strings.Split(string(readBytes), ",")...)
							}
						}

					}

          stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
          if err != nil {
            return nil, err
          }
          beforeFilter = append(beforeFilter, stringIds)

        } else {

					indexesF1Path := filepath.Join(tablePath, whereStruct.FieldName + "_indexes.flaa1")

					if doesPathExists(indexesF1Path) {
						elemsMap, err := flaarum_shared.ParseDataF1File(indexesF1Path)
						if err != nil {
							return nil, err
						}

						stringIds := make([]string, 0)
						for k, elem := range elemsMap {
							if k != whereStruct.FieldValue {
								readBytes, err := flaarum_shared.ReadPortionF2File(projName, tableName,
									whereStruct.FieldName + "_indexes", elem.DataBegin, elem.DataEnd)
								if err != nil {
									fmt.Printf("%+v\n", err)
								}
								stringIds = append(stringIds, strings.Split(string(readBytes), ",")...)
							}
						}
						beforeFilter = append(beforeFilter, stringIds)
					}
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

					otherTableindexesF1Path := filepath.Join(getTablePath(projName, pTbl), resolvedFieldName + "_indexes.flaa1")

					if currentFieldType == "date" || currentFieldType == "datetime" {

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

						if doesPathExists(otherTableindexesF1Path) {
							elemsMap, err := flaarum_shared.ParseDataF1File(otherTableindexesF1Path)
							if err != nil {
								return nil, err
							}

							elemsKeys := make([]time.Time, 0, len(elemsMap))

							for k, _ := range elemsMap {
								var elemValueTime time.Time
								if currentFieldType == "date" {
									elemValueTime, err = time.Parse(flaarum_shared.DATE_FORMAT, k)
									if err != nil {
										return nil, errors.Wrap(err, "time parsing error")
									}
								} else {
									elemValueTime, err = time.Parse(flaarum_shared.DATETIME_FORMAT, k)
									if err != nil {
										return nil, errors.Wrap(err, "time parsing error")
									}
								}

								elemsKeys = append(elemsKeys, elemValueTime)
							}

							sort.Slice(elemsKeys, func(i, j int) bool {
								return elemsKeys[i].Before(elemsKeys[j])
							})

							exactMatch := false
							brokeLoop := false
							index := 0
							for i, indexedValue := range elemsKeys {
								if indexedValue.Equal(whereStructFieldValueTime) {
									exactMatch = true
									brokeLoop = true
									index = i
									break
								}
								if indexedValue.After(whereStructFieldValueTime) {
									index = i
									brokeLoop = true
									break
								}
							}

							foundIndexedValues := make([]time.Time, 0)
							if brokeLoop && exactMatch {
								if whereStruct.Relation == ">" {
									newIndex := index + 1
									if len(elemsKeys) != newIndex {
										foundIndexedValues = elemsKeys[newIndex: ]
									}
								} else if whereStruct.Relation == ">=" {
									foundIndexedValues = elemsKeys[index: ]
								}
							} else if brokeLoop {
								foundIndexedValues = elemsKeys[index: ]
							}

							// retrieve the id of the foundIndexedValues
							for _, indexedValue := range foundIndexedValues {
								var indexedValueStr string
								if currentFieldType == "date" {
									indexedValueStr = indexedValue.Format(flaarum_shared.DATE_FORMAT)
								} else {
									indexedValueStr = indexedValue.Format(flaarum_shared.DATETIME_FORMAT)
								}

								elem, ok := elemsMap[indexedValueStr]
								if ok {
									readBytes, err := flaarum_shared.ReadPortionF2File(projName, pTbl,
										resolvedFieldName + "_indexes", elem.DataBegin, elem.DataEnd)
									if err != nil {
										fmt.Printf("%+v\n", err)
									}
									trueWhereValues = append(trueWhereValues, strings.Split(string(readBytes), ",")...)
								}
							}

							stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
		          if err != nil {
		            return nil, err
		          }

							beforeFilter = append(beforeFilter, stringIds)
						}

					} else if currentFieldType == "int" || currentFieldType == "float" {

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

						if doesPathExists(otherTableindexesF1Path) {
							elemsMap, err := flaarum_shared.ParseDataF1File(otherTableindexesF1Path)
							if err != nil {
								return nil, err
							}

							elemsKeys := make([]int64, 0, len(elemsMap))

							storeOfOriginalFloatStr := make(map[int64]string)
							for k, _ := range elemsMap {
								var elemValueInt int64
								if currentFieldType == "float" {
									tmp, err := strconv.ParseFloat(k, 64)
									if err != nil {
										return nil, errors.Wrap(err, "strconv error")
									}
									whereStructFieldValueInt = int64(tmp)
									storeOfOriginalFloatStr[whereStructFieldValueInt] = k
								} else {
									elemValueInt, err = strconv.ParseInt(k, 10, 64)
									if err != nil {
										return nil, errors.Wrap(err, "strconv error")
									}
								}

								elemsKeys = append(elemsKeys, elemValueInt)
							}

							sort.Slice(elemsKeys, func(i, j int) bool {
								return elemsKeys[i] < elemsKeys[j]
							})

							fmt.Println("elemsKeys sorted", elemsKeys)

							exactMatch := false
							brokeLoop := false
							index := 0
							for i, indexedValue := range elemsKeys {
								if indexedValue == whereStructFieldValueInt {
									exactMatch = true
									brokeLoop = true
									index = i
									break
								}
								if indexedValue > whereStructFieldValueInt {
									index = i
									brokeLoop = true
									break
								}
							}

							foundIndexedValues := make([]int64, 0)
							if brokeLoop && exactMatch {
								if whereStruct.Relation == ">" {
									newIndex := index + 1
									if len(elemsKeys) != newIndex {
										foundIndexedValues = elemsKeys[newIndex: ]
									}
								} else if whereStruct.Relation == ">=" {
									foundIndexedValues = elemsKeys[index: ]
								}
							} else if brokeLoop {
								foundIndexedValues = elemsKeys[index: ]
							}

							// retrieve the id of the foundIndexedValues
							for _, indexedValue := range foundIndexedValues {
								var indexedValueStr string
								if currentFieldType == "float" {
									indexedValueStr = storeOfOriginalFloatStr[indexedValue]
								} else {
									indexedValueStr = strconv.FormatInt(indexedValue, 10)
								}

								elem, ok := elemsMap[indexedValueStr]
								if ok {
									readBytes, err := flaarum_shared.ReadPortionF2File(projName, pTbl,
										resolvedFieldName + "_indexes", elem.DataBegin, elem.DataEnd)
									if err != nil {
										fmt.Printf("%+v\n", err)
									}
									trueWhereValues = append(trueWhereValues, strings.Split(string(readBytes), ",")...)
								}
							}

							stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
		          if err != nil {
		            return nil, err
		          }

							beforeFilter = append(beforeFilter, stringIds)
						}

					}

        } else {
					stringIds := make([]string, 0)

					indexesF1Path := filepath.Join(tablePath, whereStruct.FieldName + "_indexes.flaa1")

					currentFieldType := flaarum_shared.GetFieldType(projName, tableName, whereStruct.FieldName)

					if currentFieldType == "date" || currentFieldType == "datetime" {

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

						if doesPathExists(indexesF1Path) {
							elemsMap, err := flaarum_shared.ParseDataF1File(indexesF1Path)
							if err != nil {
								return nil, err
							}

							elemsKeys := make([]time.Time, 0, len(elemsMap))

							for k, _ := range elemsMap {
								var elemValueTime time.Time
								if currentFieldType == "date" {
									elemValueTime, err = time.Parse(flaarum_shared.DATE_FORMAT, k)
									if err != nil {
										return nil, errors.Wrap(err, "time parsing error")
									}
								} else {
									elemValueTime, err = time.Parse(flaarum_shared.DATETIME_FORMAT, k)
									if err != nil {
										return nil, errors.Wrap(err, "time parsing error")
									}
								}

								elemsKeys = append(elemsKeys, elemValueTime)
							}

							sort.Slice(elemsKeys, func(i, j int) bool {
								return elemsKeys[i].Before(elemsKeys[j])
							})

							exactMatch := false
							brokeLoop := false
							index := 0
							for i, indexedValue := range elemsKeys {
								if indexedValue.Equal(whereStructFieldValueTime) {
									exactMatch = true
									brokeLoop = true
									index = i
									break
								}
								if indexedValue.After(whereStructFieldValueTime) {
									index = i
									brokeLoop = true
									break
								}
							}

							foundIndexedValues := make([]time.Time, 0)
							if brokeLoop && exactMatch {
								if whereStruct.Relation == ">" {
									newIndex := index + 1
									if len(elemsKeys) != newIndex {
										foundIndexedValues = elemsKeys[newIndex: ]
									}
								} else if whereStruct.Relation == ">=" {
									foundIndexedValues = elemsKeys[index: ]
								}
							} else if brokeLoop {
								foundIndexedValues = elemsKeys[index: ]
							}

							// retrieve the id of the foundIndexedValues
							for _, indexedValue := range foundIndexedValues {
								var indexedValueStr string
								if currentFieldType == "date" {
									indexedValueStr = indexedValue.Format(flaarum_shared.DATE_FORMAT)
								} else {
									indexedValueStr = indexedValue.Format(flaarum_shared.DATETIME_FORMAT)
								}

								elem, ok := elemsMap[indexedValueStr]
								if ok {
									readBytes, err := flaarum_shared.ReadPortionF2File(projName, tableName,
										whereStruct.FieldName + "_indexes", elem.DataBegin, elem.DataEnd)
									if err != nil {
										fmt.Printf("%+v\n", err)
									}
									stringIds = append(stringIds, strings.Split(string(readBytes), ",")...)
								}
							}

							beforeFilter = append(beforeFilter, stringIds)
						}

					} else if currentFieldType == "int" || currentFieldType == "float" {
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

						if doesPathExists(indexesF1Path) {
							elemsMap, err := flaarum_shared.ParseDataF1File(indexesF1Path)
							if err != nil {
								return nil, err
							}

							elemsKeys := make([]int64, 0, len(elemsMap))

							storeOfOriginalFloatStr := make(map[int64]string)
							for k, _ := range elemsMap {
								var elemValueInt int64
								if currentFieldType == "float" {
									tmp, err := strconv.ParseFloat(k, 64)
									if err != nil {
										return nil, errors.Wrap(err, "strconv error")
									}
									whereStructFieldValueInt = int64(tmp)
									storeOfOriginalFloatStr[whereStructFieldValueInt] = k
								} else {
									elemValueInt, err = strconv.ParseInt(k, 10, 64)
									if err != nil {
										return nil, errors.Wrap(err, "strconv error")
									}
								}

								elemsKeys = append(elemsKeys, elemValueInt)
							}

							sort.Slice(elemsKeys, func(i, j int) bool {
								return elemsKeys[i] < elemsKeys[j]
							})

							fmt.Println("elemsKeys sorted", elemsKeys)

							exactMatch := false
							brokeLoop := false
							index := 0
							for i, indexedValue := range elemsKeys {
								if indexedValue == whereStructFieldValueInt {
									exactMatch = true
									brokeLoop = true
									index = i
									break
								}
								if indexedValue > whereStructFieldValueInt {
									index = i
									brokeLoop = true
									break
								}
							}

							foundIndexedValues := make([]int64, 0)
							if brokeLoop && exactMatch {
								if whereStruct.Relation == ">" {
									newIndex := index + 1
									if len(elemsKeys) != newIndex {
										foundIndexedValues = elemsKeys[newIndex: ]
									}
								} else if whereStruct.Relation == ">=" {
									foundIndexedValues = elemsKeys[index: ]
								}
							} else if brokeLoop {
								foundIndexedValues = elemsKeys[index: ]
							}

							// retrieve the id of the foundIndexedValues
							for _, indexedValue := range foundIndexedValues {
								var indexedValueStr string
								if currentFieldType == "float" {
									indexedValueStr = storeOfOriginalFloatStr[indexedValue]
								} else {
									indexedValueStr = strconv.FormatInt(indexedValue, 10)
								}

								elem, ok := elemsMap[indexedValueStr]
								if ok {
									readBytes, err := flaarum_shared.ReadPortionF2File(projName, tableName,
										whereStruct.FieldName + "_indexes", elem.DataBegin, elem.DataEnd)
									if err != nil {
										fmt.Printf("%+v\n", err)
									}
									stringIds = append(stringIds, strings.Split(string(readBytes), ",")...)
								}
							}

							beforeFilter = append(beforeFilter, stringIds)
						}

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
					otherTableindexesF1Path := filepath.Join(getTablePath(projName, pTbl), parts[1] + "_indexes.flaa1")

					if currentFieldType == "date" || currentFieldType == "datetime" {

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

						if doesPathExists(otherTableindexesF1Path) {
							elemsMap, err := flaarum_shared.ParseDataF1File(otherTableindexesF1Path)
							if err != nil {
								return nil, err
							}

							elemsKeys := make([]time.Time, 0, len(elemsMap))

							for k, _ := range elemsMap {
								var elemValueTime time.Time
								if currentFieldType == "date" {
									elemValueTime, err = time.Parse(flaarum_shared.DATE_FORMAT, k)
									if err != nil {
										return nil, errors.Wrap(err, "time parsing error")
									}
								} else {
									elemValueTime, err = time.Parse(flaarum_shared.DATETIME_FORMAT, k)
									if err != nil {
										return nil, errors.Wrap(err, "time parsing error")
									}
								}

								elemsKeys = append(elemsKeys, elemValueTime)
							}

							sort.Slice(elemsKeys, func(i, j int) bool {
								return elemsKeys[i].Before(elemsKeys[j])
							})

							exactMatch := false
							brokeLoop := false
							index := 0
							for i, indexedValue := range elemsKeys {
								if indexedValue.Equal(whereStructFieldValueTime) {
									exactMatch = true
									brokeLoop = true
									index = i
									break
								}
								if indexedValue.After(whereStructFieldValueTime) {
									index = i
									brokeLoop = true
									break
								}
							}

							foundIndexedValues := make([]time.Time, 0)
							if brokeLoop && exactMatch {
								if whereStruct.Relation == "<" {
									newIndex := index + 1
									if len(elemsKeys) != newIndex {
										foundIndexedValues = elemsKeys[0: newIndex]
									}
								} else if whereStruct.Relation == "<=" {
									foundIndexedValues = elemsKeys[0: index]
								}
							} else if brokeLoop {
								foundIndexedValues = elemsKeys[0: index]
							}

							// retrieve the id of the foundIndexedValues
							for _, indexedValue := range foundIndexedValues {
								var indexedValueStr string
								if currentFieldType == "date" {
									indexedValueStr = indexedValue.Format(flaarum_shared.DATE_FORMAT)
								} else {
									indexedValueStr = indexedValue.Format(flaarum_shared.DATETIME_FORMAT)
								}

								elem, ok := elemsMap[indexedValueStr]
								if ok {
									readBytes, err := flaarum_shared.ReadPortionF2File(projName, pTbl,
										resolvedFieldName + "_indexes", elem.DataBegin, elem.DataEnd)
									if err != nil {
										fmt.Printf("%+v\n", err)
									}
									trueWhereValues = append(trueWhereValues, strings.Split(string(readBytes), ",")...)
								}
							}

							stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
		          if err != nil {
		            return nil, err
		          }

							beforeFilter = append(beforeFilter, stringIds)
						}

					} else if currentFieldType == "int" || currentFieldType == "float" {
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

						if doesPathExists(otherTableindexesF1Path) {
							elemsMap, err := flaarum_shared.ParseDataF1File(otherTableindexesF1Path)
							if err != nil {
								return nil, err
							}

							elemsKeys := make([]int64, 0, len(elemsMap))

							storeOfOriginalFloatStr := make(map[int64]string)
							for k, _ := range elemsMap {
								var elemValueInt int64
								if currentFieldType == "float" {
									tmp, err := strconv.ParseFloat(k, 64)
									if err != nil {
										return nil, errors.Wrap(err, "strconv error")
									}
									whereStructFieldValueInt = int64(tmp)
									storeOfOriginalFloatStr[whereStructFieldValueInt] = k
								} else {
									elemValueInt, err = strconv.ParseInt(k, 10, 64)
									if err != nil {
										return nil, errors.Wrap(err, "strconv error")
									}
								}

								elemsKeys = append(elemsKeys, elemValueInt)
							}

							sort.Slice(elemsKeys, func(i, j int) bool {
								return elemsKeys[i] < elemsKeys[j]
							})

							exactMatch := false
							brokeLoop := false
							index := 0
							for i, indexedValue := range elemsKeys {
								if indexedValue == whereStructFieldValueInt {
									exactMatch = true
									brokeLoop = true
									index = i
									break
								}
								if indexedValue > whereStructFieldValueInt {
									index = i
									brokeLoop = true
									break
								}
							}

							foundIndexedValues := make([]int64, 0)
							if brokeLoop && exactMatch {
								if whereStruct.Relation == "<" {
									newIndex := index + 1
									if len(elemsKeys) != newIndex {
										foundIndexedValues = elemsKeys[0: newIndex]
									}
								} else if whereStruct.Relation == "<=" {
									foundIndexedValues = elemsKeys[0: index]
								}
							} else if brokeLoop {
								foundIndexedValues = elemsKeys[0: index ]
							}

							// retrieve the id of the foundIndexedValues
							for _, indexedValue := range foundIndexedValues {
								var indexedValueStr string
								if currentFieldType == "float" {
									indexedValueStr = storeOfOriginalFloatStr[indexedValue]
								} else {
									indexedValueStr = strconv.FormatInt(indexedValue, 10)
								}

								elem, ok := elemsMap[indexedValueStr]
								if ok {
									readBytes, err := flaarum_shared.ReadPortionF2File(projName, pTbl,
										resolvedFieldName + "_indexes", elem.DataBegin, elem.DataEnd)
									if err != nil {
										fmt.Printf("%+v\n", err)
									}
									trueWhereValues = append(trueWhereValues, strings.Split(string(readBytes), ",")...)
								}
							}

							stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
							if err != nil {
								return nil, err
							}

							beforeFilter = append(beforeFilter, stringIds)
						}

					}
        } else {
					stringIds := make([]string, 0)

					currentFieldType := flaarum_shared.GetFieldType(projName, tableName, whereStruct.FieldName)
					indexesF1Path := filepath.Join(tablePath, whereStruct.FieldName + "_indexes.flaa1")

					if currentFieldType == "date" || currentFieldType == "datetime" {
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

						if doesPathExists(indexesF1Path) {
							elemsMap, err := flaarum_shared.ParseDataF1File(indexesF1Path)
							if err != nil {
								return nil, err
							}

							elemsKeys := make([]time.Time, 0, len(elemsMap))

							for k, _ := range elemsMap {
								var elemValueTime time.Time
								if currentFieldType == "date" {
									elemValueTime, err = time.Parse(flaarum_shared.DATE_FORMAT, k)
									if err != nil {
										return nil, errors.Wrap(err, "time parsing error")
									}
								} else {
									elemValueTime, err = time.Parse(flaarum_shared.DATETIME_FORMAT, k)
									if err != nil {
										return nil, errors.Wrap(err, "time parsing error")
									}
								}

								elemsKeys = append(elemsKeys, elemValueTime)
							}

							sort.Slice(elemsKeys, func(i, j int) bool {
								return elemsKeys[i].Before(elemsKeys[j])
							})

							exactMatch := false
							brokeLoop := false
							index := 0
							for i, indexedValue := range elemsKeys {
								if indexedValue.Equal(whereStructFieldValueTime) {
									exactMatch = true
									brokeLoop = true
									index = i
									break
								}
								if indexedValue.After(whereStructFieldValueTime) {
									index = i
									brokeLoop = true
									break
								}
							}

							foundIndexedValues := make([]time.Time, 0)
							if brokeLoop && exactMatch {
								if whereStruct.Relation == "<" {
									newIndex := index + 1
									if len(elemsKeys) != newIndex {
										foundIndexedValues = elemsKeys[0: newIndex ]
									}
								} else if whereStruct.Relation == "<=" {
									foundIndexedValues = elemsKeys[0: index ]
								}
							} else if brokeLoop {
								foundIndexedValues = elemsKeys[0: index ]
							}

							// retrieve the id of the foundIndexedValues
							for _, indexedValue := range foundIndexedValues {
								var indexedValueStr string
								if currentFieldType == "date" {
									indexedValueStr = indexedValue.Format(flaarum_shared.DATE_FORMAT)
								} else {
									indexedValueStr = indexedValue.Format(flaarum_shared.DATETIME_FORMAT)
								}

								elem, ok := elemsMap[indexedValueStr]
								if ok {
									readBytes, err := flaarum_shared.ReadPortionF2File(projName, tableName,
										whereStruct.FieldName + "_indexes", elem.DataBegin, elem.DataEnd)
									if err != nil {
										fmt.Printf("%+v\n", err)
									}
									stringIds = append(stringIds, strings.Split(string(readBytes), ",")...)
								}
							}

							beforeFilter = append(beforeFilter, stringIds)
						}

					} else if currentFieldType == "int" || currentFieldType == "float" {

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

						if doesPathExists(indexesF1Path) {
							elemsMap, err := flaarum_shared.ParseDataF1File(indexesF1Path)
							if err != nil {
								return nil, err
							}

							elemsKeys := make([]int64, 0, len(elemsMap))

							storeOfOriginalFloatStr := make(map[int64]string)
							for k, _ := range elemsMap {
								var elemValueInt int64
								if currentFieldType == "float" {
									tmp, err := strconv.ParseFloat(k, 64)
									if err != nil {
										return nil, errors.Wrap(err, "strconv error")
									}
									whereStructFieldValueInt = int64(tmp)
									storeOfOriginalFloatStr[whereStructFieldValueInt] = k
								} else {
									elemValueInt, err = strconv.ParseInt(k, 10, 64)
									if err != nil {
										return nil, errors.Wrap(err, "strconv error")
									}
								}

								elemsKeys = append(elemsKeys, elemValueInt)
							}

							sort.Slice(elemsKeys, func(i, j int) bool {
								return elemsKeys[i] < elemsKeys[j]
							})

							exactMatch := false
							brokeLoop := false
							index := 0
							for i, indexedValue := range elemsKeys {
								if indexedValue == whereStructFieldValueInt {
									exactMatch = true
									brokeLoop = true
									index = i
									break
								}
								if indexedValue > whereStructFieldValueInt {
									index = i
									brokeLoop = true
									break
								}
							}

							foundIndexedValues := make([]int64, 0)
							if brokeLoop && exactMatch {
								if whereStruct.Relation == "<" {
									newIndex := index + 1
									if len(elemsKeys) != newIndex {
										foundIndexedValues = elemsKeys[0: newIndex]
									}
								} else if whereStruct.Relation == "<=" {
									foundIndexedValues = elemsKeys[0: index]
								}
							} else if brokeLoop {
								foundIndexedValues = elemsKeys[0: index]
							}

							// retrieve the id of the foundIndexedValues
							for _, indexedValue := range foundIndexedValues {
								var indexedValueStr string
								if currentFieldType == "float" {
									indexedValueStr = storeOfOriginalFloatStr[indexedValue]
								} else {
									indexedValueStr = strconv.FormatInt(indexedValue, 10)
								}

								elem, ok := elemsMap[indexedValueStr]
								if ok {
									readBytes, err := flaarum_shared.ReadPortionF2File(projName, tableName,
										whereStruct.FieldName + "_indexes", elem.DataBegin, elem.DataEnd)
									if err != nil {
										fmt.Printf("%+v\n", err)
									}
									stringIds = append(stringIds, strings.Split(string(readBytes), ",")...)
								}
							}

							beforeFilter = append(beforeFilter, stringIds)
						}
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
              trueWhereValues = arrayOperations.Union(trueWhereValues, strings.Split(string(raw), "\n"))

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
              stringIds = arrayOperations.Union(stringIds, strings.Split(string(raw), "\n"))
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

            trueWhereValues = arrayOperations.Union(trueWhereValues, strings.Split(string(raw), "\n"))
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

            stringIds = arrayOperations.Union(stringIds, strings.Split(string(raw), "\n"))
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
            exemptedIds = arrayOperations.Union(exemptedIds, strings.Split(string(raw), "\n"))
          }

          trueWhereValues := arrayOperations.Difference(allForeignIds, exemptedIds)

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
            exemptedIds = arrayOperations.Union(exemptedIds, strings.Split(string(raw), "\n"))
          }

          stringIds := arrayOperations.Difference(allIds, exemptedIds)
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
            trueWhereValues = arrayOperations.Union(trueWhereValues, strings.Split(string(raw), "\n"))
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
            stringIds = arrayOperations.Union(stringIds, strings.Split(string(raw), "\n"))
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
						if char == "/" || char == " " || char == "\t" || char == "." {
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

					trueWhereValues := arrayOperations.Intersect(tmpIds...)

					stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
          if err != nil {
            return nil, err
          }
          beforeFilter = append(beforeFilter, stringIds)

				} else {
					tmpIds := make([][]string, 0)
					for _, char := range charsOfData {
						if char == "/" || char == " " || char == "\t" || char == "." {
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

					stringIds := arrayOperations.Intersect(tmpIds...)

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
			retIds = arrayOperations.Intersect(beforeFilter...)
		} else if orsCount == len(stmtStruct.WhereOptions) - 1 {
			retIds = arrayOperations.Union(beforeFilter...)
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
						tmpIntersected = arrayOperations.Intersect(tmpIntersected, beforeFilter[innerIndex])
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

			retIds = arrayOperations.Union(beforeUnion...)
		}
	}

	// read the whole foundRows using its Id
	tmpRet := make([]map[string]string, 0)
	elemsMap, err := flaarum_shared.ParseDataF1File(filepath.Join(tablePath, "data.flaa1"))
	if err != nil {
		fmt.Println(err)
	}

  for _, retId := range retIds {
		elem, ok := elemsMap[retId]
		if !ok {
			continue
		}
		fmt.Println(retId)
		rawRowData, err := flaarum_shared.ReadPortionF2File(projName, tableName, "data",
			elem.DataBegin, elem.DataEnd)
		if err != nil {
			return nil, err
		}

		rowMap, err := flaarum_shared.ParseEncodedRowData(rawRowData)
		if err != nil {
			fmt.Println(err)
			continue
		}

    // for field, data := range rowMap {
		//
    //   pTbl, ok := expDetails[field]
    //   if ok {
		// 		rowMap2, err := flaarum_shared.ReadDataFile(filepath.Join(dataPath, projName, pTbl, "data", data))
		// 		if err != nil {
		// 			continue
		// 		}
		//
    //     for f, d := range rowMap2 {
    //       rowMap[field + "." + f] = d
    //     }
    //   }
    // }

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
