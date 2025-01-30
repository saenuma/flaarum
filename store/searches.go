package main

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	arrayOperations "github.com/adam-hanna/arrayOperations"
	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/internal"
)

func searchTable(w http.ResponseWriter, r *http.Request) {

	projName := r.PathValue("proj")

	stmtStruct, err := internal.ParseSearchStmt(r.FormValue("stmt"))
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	dataPath, _ := internal.GetDataPath()
	tablePath := filepath.Join(dataPath, projName, stmtStruct.TableName)
	if !internal.DoesPathExists(tablePath) {
		internal.PrintError(w, errors.New(fmt.Sprintf("Table '%s' of Project '%s' does not exists.", stmtStruct.TableName, projName)))
		return
	}

	rets, err := innerSearch(projName, r.FormValue("stmt"))
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	if r.FormValue("query-one") == "t" {
		if len(*rets) == 0 {
			internal.PrintError(w, errors.New("The search returned nothing."))
			return
		}
		jsonBytes, err := json.Marshal((*rets)[0])
		if err != nil {
			internal.PrintError(w, errors.Wrap(err, "json error"))
			return
		}
		fmt.Fprint(w, string(jsonBytes))
	} else {
		jsonBytes, err := json.Marshal(rets)
		if err != nil {
			internal.PrintError(w, errors.Wrap(err, "json error"))
			return
		}
		fmt.Fprint(w, string(jsonBytes))
	}
}

// this is needed in expanded searches
func findIdsContainingTrueWhereValues(projName, tableName, fieldName string, trueWhereValues []string) ([]string, error) {
	retIds := make([]string, 0)
	indexesObj, _ := internal.ReadIndexesToMap(projName, tableName, fieldName)
	for _, tmpId := range trueWhereValues {
		foundIds := indexesObj[tmpId]
		retIds = append(retIds, foundIds...)
	}
	return retIds, nil
}

// read a text field
func readTextField(projName, tableName, fieldName, lookedForId string) string {
	dataPath, _ := internal.GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)

	elemsMap, _ := internal.ParseDataF1File(filepath.Join(tablePath, "data.flaa1"))

	elem, ok := elemsMap[lookedForId]
	if !ok {
		return ""
	}

	rawRowData, err := internal.ReadPortionF2File(projName, tableName, "data",
		elem.DataBegin, elem.DataEnd)
	if err != nil {
		return ""
	}

	rowMap, err := internal.ParseEncodedRowData(rawRowData)
	if err != nil {
		return ""
	}

	return rowMap[fieldName]
}

func doOnlyOneSearch(projName, tableName string, expand bool, whereOpts []internal.WhereStruct) ([]string, error) {
	dataPath, _ := internal.GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)

	expDetails := make(map[string]string)

	tableStruct, err := getCurrentTableStructureParsed(projName, tableName)
	if err != nil {
		return nil, err
	}

	if expand {
		for _, fKeyStruct := range tableStruct.ForeignKeys {
			if !internal.DoesPathExists(filepath.Join(dataPath, projName, fKeyStruct.PointedTable)) {
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

				pTblIndexesObj, _ := internal.ReadIndexesToMap(projName, pTbl, parts[1])
				trueWhereValues = pTblIndexesObj[whereStruct.FieldValue]

				stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
				if err != nil {
					return nil, err
				}
				beforeFilter = append(beforeFilter, stringIds)

			} else {

				indexesObj, _ := internal.ReadIndexesToMap(projName, tableName, whereStruct.FieldName)
				if _, ok := indexesObj[whereStruct.FieldValue]; ok {
					beforeFilter = append(beforeFilter, indexesObj[whereStruct.FieldValue])
				} else {
					beforeFilter = append(beforeFilter, []string{})
				}

			}

		} else if whereStruct.Relation == "!=" {
			if whereStruct.FieldName == "id" {
				dataF1Path := filepath.Join(tablePath, "data.flaa1")

				elemsMap, err := internal.ParseDataF1File(dataF1Path)
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

				pTblIndexesObj, _ := internal.ReadIndexesToMap(projName, pTbl, parts[1])
				delete(pTblIndexesObj, whereStruct.FieldValue)
				for _, indexesSlice := range pTblIndexesObj {
					trueWhereValues = append(trueWhereValues, indexesSlice...)
				}

				stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
				if err != nil {
					return nil, err
				}
				beforeFilter = append(beforeFilter, stringIds)

			} else {

				indexesObj, _ := internal.ReadIndexesToMap(projName, tableName, whereStruct.FieldName)
				delete(indexesObj, whereStruct.FieldValue)

				stringIds := make([]string, 0)
				for _, indexesSlice := range indexesObj {
					stringIds = append(stringIds, indexesSlice...)
				}
				beforeFilter = append(beforeFilter, stringIds)
			}

		} else if whereStruct.Relation == ">" || whereStruct.Relation == ">=" {

			if strings.Contains(whereStruct.FieldName, ".") {
				trueWhereValues := make([]string, 0)
				parts := strings.Split(whereStruct.FieldName, ".")

				pTbl, ok := expDetails[parts[0]]
				if !ok {
					continue
				}

				pTblIndexesObj, _ := internal.ReadIndexesToMap(projName, pTbl, parts[1])

				var whereStructFieldValueInt int64
				whereStructFieldValueInt, err = strconv.ParseInt(whereStruct.FieldValue, 10, 64)
				if err != nil {
					return nil, errors.Wrap(err, "strconv error")
				}

				keys := slices.Collect(maps.Keys(pTblIndexesObj))
				keysInt := make([]int64, 0)
				for _, key := range keys {
					tmpInt, _ := strconv.ParseInt(key, 10, 64)
					keysInt = append(keysInt, tmpInt)
				}

				slices.SortFunc(keysInt, func(a, b int64) int {
					return cmp.Compare(a, b)
				})

				exactMatch := false
				brokeLoop := false
				index := 0
				for i, indexedValue := range keysInt {
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
						if len(keysInt) != newIndex {
							foundIndexedValues = keysInt[newIndex:]
						}
					} else if whereStruct.Relation == ">=" {
						foundIndexedValues = keysInt[index:]
					}
				} else if brokeLoop {
					foundIndexedValues = keysInt[index:]
				}

				// retrieve the id of the foundIndexedValues
				for _, indexedValue := range foundIndexedValues {
					indexedValueStr := strconv.FormatInt(indexedValue, 10)
					trueWhereValues = append(trueWhereValues, pTblIndexesObj[indexedValueStr]...)
				}

				stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
				if err != nil {
					return nil, err
				}

				beforeFilter = append(beforeFilter, stringIds)

			} else {
				stringIds := make([]string, 0)

				var whereStructFieldValueInt int64
				whereStructFieldValueInt, err = strconv.ParseInt(whereStruct.FieldValue, 10, 64)
				if err != nil {
					return nil, errors.Wrap(err, "strconv error")
				}

				indexesObj, _ := internal.ReadIndexesToMap(projName, tableName, whereStruct.FieldName)
				keys := slices.Collect(maps.Keys(indexesObj))
				keysInt := make([]int64, 0)
				for _, key := range keys {
					tmpInt, _ := strconv.ParseInt(key, 10, 64)
					keysInt = append(keysInt, tmpInt)
				}

				slices.SortFunc(keysInt, func(a, b int64) int {
					return cmp.Compare(a, b)
				})

				exactMatch := false
				brokeLoop := false
				index := 0
				for i, indexedValue := range keysInt {
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
						if len(keysInt) != newIndex {
							foundIndexedValues = keysInt[newIndex:]
						}
					} else if whereStruct.Relation == ">=" {
						foundIndexedValues = keysInt[index:]
					}
				} else if brokeLoop {
					foundIndexedValues = keysInt[index:]
				}

				// retrieve the id of the foundIndexedValues
				for _, indexedValue := range foundIndexedValues {
					indexedValueStr := strconv.FormatInt(indexedValue, 10)
					stringIds = append(stringIds, indexesObj[indexedValueStr]...)
				}

				beforeFilter = append(beforeFilter, stringIds)
			}

		} else if whereStruct.Relation == "<" || whereStruct.Relation == "<=" {

			if strings.Contains(whereStruct.FieldName, ".") {

				trueWhereValues := make([]string, 0)
				parts := strings.Split(whereStruct.FieldName, ".")

				pTbl, ok := expDetails[parts[0]]
				if !ok {
					continue
				}

				pTblIndexesObj, _ := internal.ReadIndexesToMap(projName, pTbl, parts[1])

				var whereStructFieldValueInt int64
				whereStructFieldValueInt, err = strconv.ParseInt(whereStruct.FieldValue, 10, 64)
				if err != nil {
					return nil, errors.Wrap(err, "strconv error")
				}

				keys := slices.Collect(maps.Keys(pTblIndexesObj))
				keysInt := make([]int64, 0)
				for _, key := range keys {
					tmpInt, _ := strconv.ParseInt(key, 10, 64)
					keysInt = append(keysInt, tmpInt)
				}

				slices.SortFunc(keysInt, func(a, b int64) int {
					return cmp.Compare(a, b)
				})

				exactMatch := false
				brokeLoop := false
				index := 0
				for i, indexedValue := range keysInt {
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
						if len(keysInt) != newIndex {
							foundIndexedValues = keysInt[0:newIndex]
						}
					} else if whereStruct.Relation == "<=" {
						foundIndexedValues = keysInt[0:index]
					}
				} else if brokeLoop {
					foundIndexedValues = keysInt[0:index]
				}

				// retrieve the id of the foundIndexedValues
				for _, indexedValue := range foundIndexedValues {
					indexedValueStr := strconv.FormatInt(indexedValue, 10)
					trueWhereValues = append(trueWhereValues, pTblIndexesObj[indexedValueStr]...)
				}

				stringIds, err := findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
				if err != nil {
					return nil, err
				}

				beforeFilter = append(beforeFilter, stringIds)

			} else {
				stringIds := make([]string, 0)

				var whereStructFieldValueInt int64
				whereStructFieldValueInt, err = strconv.ParseInt(whereStruct.FieldValue, 10, 64)
				if err != nil {
					return nil, errors.Wrap(err, "strconv error")
				}

				indexesObj, _ := internal.ReadIndexesToMap(projName, tableName, whereStruct.FieldName)
				keys := slices.Collect(maps.Keys(indexesObj))
				keysInt := make([]int64, 0)
				for _, key := range keys {
					tmpInt, _ := strconv.ParseInt(key, 10, 64)
					keysInt = append(keysInt, tmpInt)
				}

				slices.SortFunc(keysInt, func(a, b int64) int {
					return cmp.Compare(a, b)
				})

				exactMatch := false
				brokeLoop := false
				index := 0
				for i, indexedValue := range keysInt {
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
						if len(keysInt) != newIndex {
							foundIndexedValues = keysInt[0:newIndex]
						}
					} else if whereStruct.Relation == "<=" {
						foundIndexedValues = keysInt[0:index]
					}
				} else if brokeLoop {
					foundIndexedValues = keysInt[0:index]
				}

				// retrieve the id of the foundIndexedValues
				for _, indexedValue := range foundIndexedValues {
					indexedValueStr := strconv.FormatInt(indexedValue, 10)
					stringIds = append(stringIds, indexesObj[indexedValueStr]...)
				}

				beforeFilter = append(beforeFilter, stringIds)
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

				pTblIndexesObj, _ := internal.ReadIndexesToMap(projName, pTbl, parts[1])
				for _, inval := range whereStruct.FieldValues {
					trueWhereValues = append(trueWhereValues, pTblIndexesObj[inval]...)
				}

				stringIds, err = findIdsContainingTrueWhereValues(projName, tableName, parts[0], trueWhereValues)
				if err != nil {
					return nil, err
				}
			} else {

				indexesObj, _ := internal.ReadIndexesToMap(projName, tableName, whereStruct.FieldName)
				for _, inval := range whereStruct.FieldValues {
					stringIds = append(stringIds, indexesObj[inval]...)
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
				pointedTableElemsMap, _ := internal.ParseDataF1File(filepath.Join(otherTablePath, "data.flaa1"))

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
				elemsMap, _ := internal.ParseDataF1File(filepath.Join(tablePath, "data.flaa1"))

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
	stmtStruct, err := internal.ParseSearchStmt(stmt)
	if err != nil {
		return nil, err
	}

	dataPath, _ := internal.GetDataPath()
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
			if !internal.DoesPathExists(filepath.Join(dataPath, projName, fKeyStruct.PointedTable)) {
				return nil, errors.New(fmt.Sprintf("table '%s' of project '%s' does not exists.", fKeyStruct.PointedTable, projName))
			}
			expDetails[fKeyStruct.FieldName] = fKeyStruct.PointedTable
		}
	}

	retIds := make([]string, 0)

	if stmtStruct.Multi {
		if len(stmtStruct.MultiWhereOptions) == 0 {
			dataF1Path := filepath.Join(tablePath, "data.flaa1")

			if internal.DoesPathExists(dataF1Path) {
				elemsMap, err := internal.ParseDataF1File(dataF1Path)
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

			if internal.DoesPathExists(dataF1Path) {
				elemsMap, err := internal.ParseDataF1File(dataF1Path)
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
	elemsMap, _ := internal.ParseDataF1File(filepath.Join(tablePath, "data.flaa1"))

	for _, retId := range retIds {
		elem, ok := elemsMap[retId]
		if !ok {
			continue
		}
		rawRowData, err := internal.ReadPortionF2File(projName, tableName, "data",
			elem.DataBegin, elem.DataEnd)
		if err != nil {
			return nil, err
		}

		rowMap, err := internal.ParseEncodedRowData(rawRowData)
		if err != nil {
			fmt.Println(err)
			continue
		}

		for field, data := range rowMap {

			pTbl, ok := expDetails[field]
			if ok {
				pTblelemsMap, err := internal.ParseDataF1File(filepath.Join(internal.GetTablePath(projName, pTbl), "data.flaa1"))
				if err != nil {
					fmt.Println(err)
				}

				pTblelem, ok := pTblelemsMap[data]
				if !ok {
					continue
				}
				rawRowData2, err := internal.ReadPortionF2File(projName, pTbl, "data",
					pTblelem.DataBegin, pTblelem.DataEnd)
				if err != nil {
					return nil, err
				}

				rowMap2, err := internal.ParseEncodedRowData(rawRowData2)
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
			slices.SortFunc(elems, func(a, b map[string]string) int {
				if confirmFieldType(projName, tableName, stmtStruct.OrderBy, "int", a["_version"]) &&
					confirmFieldType(projName, tableName, stmtStruct.OrderBy, "int", b["_version"]) {
					x, err1 := strconv.ParseInt(a[stmtStruct.OrderBy], 10, 64)
					y, err2 := strconv.ParseInt(b[stmtStruct.OrderBy], 10, 64)
					if err1 == nil && err2 == nil {
						return cmp.Compare(x, y)
					} else {
						return strings.Compare(a[stmtStruct.OrderBy], b[stmtStruct.OrderBy])
					}

				} else {
					return strings.Compare(a[stmtStruct.OrderBy], b[stmtStruct.OrderBy])
				}
			})
		} else {
			slices.SortFunc(elems, func(a, b map[string]string) int {
				if confirmFieldType(projName, tableName, stmtStruct.OrderBy, "int", a["_version"]) &&
					confirmFieldType(projName, tableName, stmtStruct.OrderBy, "int", b["_version"]) {
					x, err1 := strconv.ParseInt(a[stmtStruct.OrderBy], 10, 64)
					y, err2 := strconv.ParseInt(b[stmtStruct.OrderBy], 10, 64)
					if err1 == nil && err2 == nil {
						return cmp.Compare(x, y) * -1
					} else {
						return strings.Compare(a[stmtStruct.OrderBy], b[stmtStruct.OrderBy]) * -1
					}

				} else {
					return strings.Compare(a[stmtStruct.OrderBy], b[stmtStruct.OrderBy]) * -1
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
