package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	arrayOperations "github.com/adam-hanna/arrayOperations"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/flaarum_shared"
)

func searchTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]

	stmtStruct, err := flaarum_shared.ParseSearchStmt(r.FormValue("stmt"))
	if err != nil {
		flaarum_shared.PrintError(w, err)
		return
	}

	dataPath, _ := flaarum_shared.GetDataPath()
	tablePath := filepath.Join(dataPath, projName, stmtStruct.TableName)
	if !flaarum_shared.DoesPathExists(tablePath) {
		flaarum_shared.PrintError(w, errors.New(fmt.Sprintf("Table '%s' of Project '%s' does not exists.", stmtStruct.TableName, projName)))
		return
	}

	rets, err := innerSearch(projName, r.FormValue("stmt"))
	if err != nil {
		flaarum_shared.PrintError(w, err)
		return
	}

	if r.FormValue("query-one") == "t" {
		if len(*rets) == 0 {
			flaarum_shared.PrintError(w, errors.New("The search returned nothing."))
			return
		}
		jsonBytes, err := json.Marshal((*rets)[0])
		if err != nil {
			flaarum_shared.PrintError(w, errors.Wrap(err, "json error"))
			return
		}
		fmt.Fprint(w, string(jsonBytes))
	} else {
		jsonBytes, err := json.Marshal(rets)
		if err != nil {
			flaarum_shared.PrintError(w, errors.Wrap(err, "json error"))
			return
		}
		fmt.Fprint(w, string(jsonBytes))
	}
}

// this is needed in expanded searches
func findIdsContainingTrueWhereValues(projName, tableName, fieldName string, trueWhereValues []string) ([]string, error) {
	dataPath, _ := flaarum_shared.GetDataPath()
	retIds := make([]string, 0)

	indexesF1Path := filepath.Join(dataPath, projName, tableName, fieldName+"_indexes.flaa1")
	elemsMap, err := flaarum_shared.ParseDataF1File(indexesF1Path)
	if err != nil {
		return nil, err
	}

	for _, tmpId := range trueWhereValues {
		elemHandle, ok := elemsMap[tmpId]
		if !ok {
			continue
		}

		readBytes, err := flaarum_shared.ReadPortionF2File(projName, tableName,
			fieldName+"_indexes", elemHandle.DataBegin, elemHandle.DataEnd)
		if err != nil {
			fmt.Printf("%+v\n", err)
		}
		retIds = append(retIds, strings.Split(string(readBytes), ",")...)
	}

	return retIds, nil
}

// read a text field
func readTextField(projName, tableName, fieldName, lookedForId string) string {
	dataPath, _ := flaarum_shared.GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)

	elemsMap, _ := flaarum_shared.ParseDataF1File(filepath.Join(tablePath, "data.flaa1"))

	elem, ok := elemsMap[lookedForId]
	if !ok {
		return ""
	}

	rawRowData, err := flaarum_shared.ReadPortionF2File(projName, tableName, "data",
		elem.DataBegin, elem.DataEnd)
	if err != nil {
		return ""
	}

	rowMap, err := flaarum_shared.ParseEncodedRowData(rawRowData)
	if err != nil {
		return ""
	}

	return rowMap[fieldName]
}

func doOnlyOneSearch(projName, tableName string, expand bool, whereOpts []flaarum_shared.WhereStruct) ([]string, error) {
	dataPath, _ := flaarum_shared.GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)

	expDetails := make(map[string]string)

	tableStruct, err := getCurrentTableStructureParsed(projName, tableName)
	if err != nil {
		return nil, err
	}

	if expand {
		for _, fKeyStruct := range tableStruct.ForeignKeys {
			if !flaarum_shared.DoesPathExists(filepath.Join(dataPath, projName, fKeyStruct.PointedTable)) {
				return nil, errors.New(fmt.Sprintf("table '%s' of project '%s' does not exists.", fKeyStruct.PointedTable, projName))
			}
			expDetails[fKeyStruct.FieldName] = fKeyStruct.PointedTable
		}
	}

	// validation
	fieldNamesToFieldTypes := make(map[string]string)
	fieldNamesToNotIndexedStatus := make(map[string]bool)

	for _, fieldStruct := range tableStruct.Fields {
		fieldNamesToFieldTypes[fieldStruct.FieldName] = fieldStruct.FieldType
	}
	for _, fieldStruct := range tableStruct.Fields {
		fieldNamesToNotIndexedStatus[fieldStruct.FieldName] = fieldStruct.NotIndexed
	}
	for i, whereStruct := range whereOpts {
		if i != 0 {
			if whereStruct.Joiner != "and" && whereStruct.Joiner != "or" {
				return nil, errors.New("Invalid statement: joiner must be one of 'and', 'or'.")
			}
		}

		ft := fieldNamesToFieldTypes[whereStruct.FieldName]

		if ft == "string" || ft == "text" {

			if whereStruct.Relation == ">" || whereStruct.Relation == ">=" || whereStruct.Relation == "<" || whereStruct.Relation == "<=" {
				return nil, errors.New(fmt.Sprintf("Invalid statement: The type '%s' does not support the query relation '%s'",
					ft, whereStruct.Relation))
			}

		}

		if ft == "int" && whereStruct.Relation == "has" {
			return nil, errors.New(fmt.Sprintf("The field type '%s' does not support the query relation 'has'", ft))
		}

		if whereStruct.FieldName == "id" {
			if whereStruct.Relation == ">" || whereStruct.Relation == ">=" || whereStruct.Relation == "<" || whereStruct.Relation == "<=" {
				return nil, errors.New(fmt.Sprintf("Invalid statement: The 'id' field does not support the query relation '%s'",
					whereStruct.Relation))
			}
			if whereStruct.Relation == "has" {
				return nil, errors.New("Invalid statment: the 'id' field does not support the query relation 'has'")
			}
		}

		if fieldNamesToNotIndexedStatus[whereStruct.FieldName] {
			return nil, errors.New(fmt.Sprintf("The field '%s' is not searchable because it has the 'nindex' attribute",
				whereStruct.FieldName))
		}

	}

	// main search
	beforeFilter := make([][]string, 0)

	for _, whereStruct := range whereOpts {
		if whereStruct.Relation == "=" {

			if whereStruct.FieldName == "id" {
				beforeFilter = append(beforeFilter, []string{whereStruct.FieldValue})
			} else if strings.Contains(whereStruct.FieldName, ".") {
				trueWhereValues := make([]string, 0)
				parts := strings.Split(whereStruct.FieldName, ".")

				pTbl, ok := expDetails[parts[0]]
				if !ok {
					continue
				}

				indexesF1Path := filepath.Join(flaarum_shared.GetTablePath(projName, pTbl), parts[1]+"_indexes.flaa1")

				if flaarum_shared.DoesPathExists(indexesF1Path) {
					elemsMap, err := flaarum_shared.ParseDataF1File(indexesF1Path)
					if err != nil {
						return nil, err
					}
					elemHandle, ok := elemsMap[whereStruct.FieldValue]
					if ok {
						readBytes, err := flaarum_shared.ReadPortionF2File(projName, pTbl, parts[1]+"_indexes",
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
				indexesF1Path := filepath.Join(tablePath, whereStruct.FieldName+"_indexes.flaa1")

				if flaarum_shared.DoesPathExists(indexesF1Path) {
					elemsMap, err := flaarum_shared.ParseDataF1File(indexesF1Path)
					if err != nil {
						return nil, err
					}
					elemHandle, ok := elemsMap[whereStruct.FieldValue]
					if ok {
						readBytes, err := flaarum_shared.ReadPortionF2File(projName, tableName,
							whereStruct.FieldName+"_indexes", elemHandle.DataBegin, elemHandle.DataEnd)
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

				for k := range elemsMap {
					if k != whereStruct.FieldValue {
						stringIds = append(stringIds, k)
					}
				}

				beforeFilter = append(beforeFilter, stringIds)
			} else if strings.Contains(whereStruct.FieldName, ".") {
				trueWhereValues := make([]string, 0)
				parts := strings.Split(whereStruct.FieldName, ".")

				pTbl, ok := expDetails[parts[0]]
				if !ok {
					continue
				}

				otherTableindexesF1Path := filepath.Join(flaarum_shared.GetTablePath(projName, pTbl), parts[1]+"_indexes.flaa1")

				if flaarum_shared.DoesPathExists(otherTableindexesF1Path) {
					elemsMap, err := flaarum_shared.ParseDataF1File(otherTableindexesF1Path)
					if err != nil {
						return nil, err
					}
					for k, elem := range elemsMap {
						if k != whereStruct.FieldValue {
							readBytes, err := flaarum_shared.ReadPortionF2File(projName, pTbl,
								parts[1]+"_indexes", elem.DataBegin, elem.DataEnd)
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

				indexesF1Path := filepath.Join(tablePath, whereStruct.FieldName+"_indexes.flaa1")

				if flaarum_shared.DoesPathExists(indexesF1Path) {
					elemsMap, err := flaarum_shared.ParseDataF1File(indexesF1Path)
					if err != nil {
						return nil, err
					}

					stringIds := make([]string, 0)
					for k, elem := range elemsMap {
						if k != whereStruct.FieldValue {
							readBytes, err := flaarum_shared.ReadPortionF2File(projName, tableName,
								whereStruct.FieldName+"_indexes", elem.DataBegin, elem.DataEnd)
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
				if !ok {
					continue
				}

				resolvedFieldName := parts[1]

				currentFieldType := flaarum_shared.GetFieldType(projName, pTbl, resolvedFieldName)

				otherTableindexesF1Path := filepath.Join(flaarum_shared.GetTablePath(projName, pTbl), resolvedFieldName+"_indexes.flaa1")

				if currentFieldType == "int" {

					var whereStructFieldValueInt int64
					whereStructFieldValueInt, err = strconv.ParseInt(whereStruct.FieldValue, 10, 64)
					if err != nil {
						return nil, errors.Wrap(err, "strconv error")
					}

					if flaarum_shared.DoesPathExists(otherTableindexesF1Path) {
						elemsMap, err := flaarum_shared.ParseDataF1File(otherTableindexesF1Path)
						if err != nil {
							return nil, err
						}

						elemsKeys := make([]int64, 0, len(elemsMap))

						for k := range elemsMap {
							var elemValueInt int64
							elemValueInt, err = strconv.ParseInt(k, 10, 64)
							if err != nil {
								return nil, errors.Wrap(err, "strconv error")
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
							if whereStruct.Relation == ">" {
								newIndex := index + 1
								if len(elemsKeys) != newIndex {
									foundIndexedValues = elemsKeys[newIndex:]
								}
							} else if whereStruct.Relation == ">=" {
								foundIndexedValues = elemsKeys[index:]
							}
						} else if brokeLoop {
							foundIndexedValues = elemsKeys[index:]
						}

						// retrieve the id of the foundIndexedValues
						for _, indexedValue := range foundIndexedValues {
							indexedValueStr := strconv.FormatInt(indexedValue, 10)

							elem, ok := elemsMap[indexedValueStr]
							if ok {
								readBytes, err := flaarum_shared.ReadPortionF2File(projName, pTbl,
									resolvedFieldName+"_indexes", elem.DataBegin, elem.DataEnd)
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

				indexesF1Path := filepath.Join(tablePath, whereStruct.FieldName+"_indexes.flaa1")

				currentFieldType := flaarum_shared.GetFieldType(projName, tableName, whereStruct.FieldName)

				if currentFieldType == "int" {
					var whereStructFieldValueInt int64
					whereStructFieldValueInt, err = strconv.ParseInt(whereStruct.FieldValue, 10, 64)
					if err != nil {
						return nil, errors.Wrap(err, "strconv error")
					}

					if flaarum_shared.DoesPathExists(indexesF1Path) {
						elemsMap, err := flaarum_shared.ParseDataF1File(indexesF1Path)
						if err != nil {
							return nil, err
						}

						elemsKeys := make([]int64, 0, len(elemsMap))

						for k := range elemsMap {
							var elemValueInt int64
							elemValueInt, err = strconv.ParseInt(k, 10, 64)
							if err != nil {
								return nil, errors.Wrap(err, "strconv error")
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
							if whereStruct.Relation == ">" {
								newIndex := index + 1
								if len(elemsKeys) != newIndex {
									foundIndexedValues = elemsKeys[newIndex:]
								}
							} else if whereStruct.Relation == ">=" {
								foundIndexedValues = elemsKeys[index:]
							}
						} else if brokeLoop {
							foundIndexedValues = elemsKeys[index:]
						}

						// retrieve the id of the foundIndexedValues
						for _, indexedValue := range foundIndexedValues {
							indexedValueStr := strconv.FormatInt(indexedValue, 10)

							elem, ok := elemsMap[indexedValueStr]
							if ok {
								readBytes, err := flaarum_shared.ReadPortionF2File(projName, tableName,
									whereStruct.FieldName+"_indexes", elem.DataBegin, elem.DataEnd)
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
				if !ok {
					continue
				}

				resolvedFieldName := parts[1]

				currentFieldType := flaarum_shared.GetFieldType(projName, pTbl, resolvedFieldName)
				otherTableindexesF1Path := filepath.Join(flaarum_shared.GetTablePath(projName, pTbl), parts[1]+"_indexes.flaa1")

				if currentFieldType == "int" {
					var whereStructFieldValueInt int64
					whereStructFieldValueInt, err = strconv.ParseInt(whereStruct.FieldValue, 10, 64)
					if err != nil {
						return nil, errors.Wrap(err, "strconv error")
					}

					if flaarum_shared.DoesPathExists(otherTableindexesF1Path) {
						elemsMap, err := flaarum_shared.ParseDataF1File(otherTableindexesF1Path)
						if err != nil {
							return nil, err
						}

						elemsKeys := make([]int64, 0, len(elemsMap))

						for k := range elemsMap {
							var elemValueInt int64
							elemValueInt, err = strconv.ParseInt(k, 10, 64)
							if err != nil {
								return nil, errors.Wrap(err, "strconv error")
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
									foundIndexedValues = elemsKeys[0:newIndex]
								}
							} else if whereStruct.Relation == "<=" {
								foundIndexedValues = elemsKeys[0:index]
							}
						} else if brokeLoop {
							foundIndexedValues = elemsKeys[0:index]
						}

						// retrieve the id of the foundIndexedValues
						for _, indexedValue := range foundIndexedValues {
							indexedValueStr := strconv.FormatInt(indexedValue, 10)

							elem, ok := elemsMap[indexedValueStr]
							if ok {
								readBytes, err := flaarum_shared.ReadPortionF2File(projName, pTbl,
									resolvedFieldName+"_indexes", elem.DataBegin, elem.DataEnd)
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
				indexesF1Path := filepath.Join(tablePath, whereStruct.FieldName+"_indexes.flaa1")

				if currentFieldType == "int" {

					var whereStructFieldValueInt int64
					whereStructFieldValueInt, err = strconv.ParseInt(whereStruct.FieldValue, 10, 64)
					if err != nil {
						return nil, errors.Wrap(err, "strconv error")
					}

					if flaarum_shared.DoesPathExists(indexesF1Path) {
						elemsMap, err := flaarum_shared.ParseDataF1File(indexesF1Path)
						if err != nil {
							return nil, err
						}

						elemsKeys := make([]int64, 0, len(elemsMap))

						for k := range elemsMap {
							var elemValueInt int64
							elemValueInt, err = strconv.ParseInt(k, 10, 64)
							if err != nil {
								return nil, errors.Wrap(err, "strconv error")
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
									foundIndexedValues = elemsKeys[0:newIndex]
								}
							} else if whereStruct.Relation == "<=" {
								foundIndexedValues = elemsKeys[0:index]
							}
						} else if brokeLoop {
							foundIndexedValues = elemsKeys[0:index]
						}

						// retrieve the id of the foundIndexedValues
						for _, indexedValue := range foundIndexedValues {
							indexedValueStr := strconv.FormatInt(indexedValue, 10)
							elem, ok := elemsMap[indexedValueStr]
							if ok {
								readBytes, err := flaarum_shared.ReadPortionF2File(projName, tableName,
									whereStruct.FieldName+"_indexes", elem.DataBegin, elem.DataEnd)
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
				pTbl, ok := expDetails[parts[0]]
				if !ok {
					continue
				}

				otherTableIndexesF1Path := filepath.Join(flaarum_shared.GetTablePath(projName, pTbl), parts[1]+"_indexes.flaa1")

				if flaarum_shared.DoesPathExists(otherTableIndexesF1Path) {
					elemsMap, err := flaarum_shared.ParseDataF1File(otherTableIndexesF1Path)
					if err != nil {
						return nil, err
					}

					for _, inval := range whereStruct.FieldValues {
						elemHandle, ok := elemsMap[inval]
						if ok {
							readBytes, err := flaarum_shared.ReadPortionF2File(projName, pTbl, parts[1]+"_indexes",
								elemHandle.DataBegin, elemHandle.DataEnd)
							if err != nil {
								fmt.Printf("%+v\n", err)
							}
							trueWhereValues = append(trueWhereValues, strings.Split(string(readBytes), ",")...)
						}
					}
				}

				stringIds, err = findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
				if err != nil {
					return nil, err
				}
				beforeFilter = append(beforeFilter, stringIds)
			} else {
				indexesF1Path := filepath.Join(tablePath, whereStruct.FieldName+"_indexes.flaa1")

				if flaarum_shared.DoesPathExists(indexesF1Path) {
					elemsMap, err := flaarum_shared.ParseDataF1File(indexesF1Path)
					if err != nil {
						return nil, err
					}

					for _, inval := range whereStruct.FieldValues {
						elemHandle, ok := elemsMap[inval]
						if ok {
							readBytes, err := flaarum_shared.ReadPortionF2File(projName, tableName,
								whereStruct.FieldName+"_indexes", elemHandle.DataBegin, elemHandle.DataEnd)
							if err != nil {
								fmt.Printf("%+v\n", err)
							}
							stringIds = append(stringIds, strings.Split(string(readBytes), ",")...)
						}

					}
				}

			}

			beforeFilter = append(beforeFilter, stringIds)

		} else if whereStruct.Relation == "has" {

			stringIds := make([]string, 0)

			if strings.Contains(whereStruct.FieldName, ".") {
				trueWhereValues := make([]string, 0)
				parts := strings.Split(whereStruct.FieldName, ".")

				pTbl, ok := expDetails[parts[0]]
				if !ok {
					continue
				}

				otherTablePath := filepath.Join(dataPath, projName, pTbl)
				pointedTableElemsMap, _ := flaarum_shared.ParseDataF1File(filepath.Join(otherTablePath, "data.flaa1"))

				for _, elem := range pointedTableElemsMap {
					aTextToSearch := readTextField(projName, pTbl, whereStruct.FieldName, elem.DataKey)
					if strings.Contains(aTextToSearch, whereStruct.FieldValue) {
						trueWhereValues = append(trueWhereValues, elem.DataKey)
					}
				}

				stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
				if err != nil {
					return nil, err
				}
				beforeFilter = append(beforeFilter, stringIds)

			} else {
				elemsMap, _ := flaarum_shared.ParseDataF1File(filepath.Join(tablePath, "data.flaa1"))

				for _, elem := range elemsMap {
					aTextToSearch := readTextField(projName, tableName, whereStruct.FieldName, elem.DataKey)
					if strings.Contains(aTextToSearch, whereStruct.FieldValue) {
						stringIds = append(stringIds, elem.DataKey)
					}
				}
			}

			beforeFilter = append(beforeFilter, stringIds)
		}

	}

	retIds := make([]string, 0)

	// do the 'and' / 'or' transformations
	andsCount := 0
	orsCount := 0
	for _, whereStruct := range whereOpts {
		if whereStruct.Joiner == "and" {
			andsCount += 1
		} else if whereStruct.Joiner == "or" {
			orsCount += 1
		}
	}

	if andsCount == len(whereOpts)-1 {
		retIds = arrayOperations.Intersect(beforeFilter...)
	} else if orsCount == len(whereOpts)-1 {
		retIds = arrayOperations.Union(beforeFilter...)
	}

	return retIds, nil
}

func innerSearch(projName, stmt string) (*[]map[string]string, error) {
	stmtStruct, err := flaarum_shared.ParseSearchStmt(stmt)
	if err != nil {
		return nil, err
	}

	dataPath, _ := flaarum_shared.GetDataPath()
	tablePath := filepath.Join(dataPath, projName, stmtStruct.TableName)
	tableName := stmtStruct.TableName

	createTableMutexIfNecessary(projName, stmtStruct.TableName)
	fullTableName := projName + ":" + stmtStruct.TableName
	tablesMutexes[fullTableName].RLock()
	defer tablesMutexes[fullTableName].RUnlock()

	// map of fieldName to pointed_table
	expDetails := make(map[string]string)

	tableStruct, err := getCurrentTableStructureParsed(projName, stmtStruct.TableName)
	if err != nil {
		return nil, err
	}

	if stmtStruct.Expand {
		for _, fKeyStruct := range tableStruct.ForeignKeys {
			if !flaarum_shared.DoesPathExists(filepath.Join(dataPath, projName, fKeyStruct.PointedTable)) {
				return nil, errors.New(fmt.Sprintf("table '%s' of project '%s' does not exists.", fKeyStruct.PointedTable, projName))
			}
			expDetails[fKeyStruct.FieldName] = fKeyStruct.PointedTable
		}
	}

	retIds := make([]string, 0)

	if stmtStruct.Multi {
		if len(stmtStruct.MultiWhereOptions) == 0 {
			dataF1Path := filepath.Join(tablePath, "data.flaa1")

			if flaarum_shared.DoesPathExists(dataF1Path) {
				elemsMap, err := flaarum_shared.ParseDataF1File(dataF1Path)
				if err != nil {
					return nil, err
				}

				for k := range elemsMap {
					retIds = append(retIds, k)
				}
			}

		} else {
			outs := make([][]string, 0)

			for _, whereOpt := range stmtStruct.MultiWhereOptions {
				tmpIds, err := doOnlyOneSearch(projName, tableName, stmtStruct.Expand, whereOpt)
				if err != nil {
					return nil, err
				}

				outs = append(outs, tmpIds)
			}

			if stmtStruct.Joiner == "and" {
				retIds = arrayOperations.Intersect(outs...)
			} else if stmtStruct.Joiner == "or" {
				retIds = arrayOperations.Union(outs...)
			}

		}

	} else {
		if len(stmtStruct.WhereOptions) == 0 {
			dataF1Path := filepath.Join(tablePath, "data.flaa1")

			if flaarum_shared.DoesPathExists(dataF1Path) {
				elemsMap, err := flaarum_shared.ParseDataF1File(dataF1Path)
				if err != nil {
					return nil, err
				}

				for k := range elemsMap {
					retIds = append(retIds, k)
				}
			}

		} else {
			retIds, err = doOnlyOneSearch(projName, tableName, stmtStruct.Expand, stmtStruct.WhereOptions)
			if err != nil {
				return nil, err
			}
		}

	}

	// read the whole foundRows using its Id
	tmpRet := make([]map[string]string, 0)
	elemsMap, _ := flaarum_shared.ParseDataF1File(filepath.Join(tablePath, "data.flaa1"))

	for _, retId := range retIds {
		elem, ok := elemsMap[retId]
		if !ok {
			continue
		}
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

		for field, data := range rowMap {

			pTbl, ok := expDetails[field]
			if ok {
				pTblelemsMap, err := flaarum_shared.ParseDataF1File(filepath.Join(flaarum_shared.GetTablePath(projName, pTbl), "data.flaa1"))
				if err != nil {
					fmt.Println(err)
				}

				pTblelem, ok := pTblelemsMap[data]
				if !ok {
					continue
				}
				rawRowData2, err := flaarum_shared.ReadPortionF2File(projName, pTbl, "data",
					pTblelem.DataBegin, pTblelem.DataEnd)
				if err != nil {
					return nil, err
				}

				rowMap2, err := flaarum_shared.ParseEncodedRowData(rawRowData2)
				if err != nil {
					fmt.Println(err)
					continue
				}

				for f, d := range rowMap2 {
					rowMap[field+"."+f] = d
				}
			}
		}

		rowMap["id"] = retId
		tmpRet = append(tmpRet, rowMap)
	}

	elems := tmpRet
	if stmtStruct.OrderBy != "" {
		if stmtStruct.OrderDirection == "asc" {
			sort.Slice(elems, func(i, j int) bool {
				if confirmFieldType(projName, tableName, stmtStruct.OrderBy, "int", elems[i]["_version"]) &&
					confirmFieldType(projName, tableName, stmtStruct.OrderBy, "int", elems[j]["_version"]) {
					x, err1 := strconv.ParseInt(elems[i][stmtStruct.OrderBy], 10, 64)
					y, err2 := strconv.ParseInt(elems[j][stmtStruct.OrderBy], 10, 64)
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

	// enforces a default limit of 1000 per search.
	rowSearchLimit := stmtStruct.Limit
	if rowSearchLimit == 0 {
		rowSearchLimit = 1000
	}

	for i, toOut := range limitedRet {
		if int64(i) == rowSearchLimit {
			break
		}
		limitedRet2 = append(limitedRet2, toOut)
	}

	// selected fields
	beforeDistinct := make([]map[string]string, 0)
	if len(stmtStruct.Fields) != 0 {
		for _, toOut := range limitedRet2 {
			newOut := make(map[string]string)
			for field := range toOut {
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
			if !ok {
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
