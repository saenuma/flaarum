package main

import (
	"net/http"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/bankole7782/flaarum/flaarum_shared"
	"path/filepath"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"github.com/adam-hanna/arrayOperations"
	"sort"
	"strconv"
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

	createTableMutexIfNecessary(projName, stmtStruct.TableName)
	fullTableName := projName + ":" + stmtStruct.TableName
	rowsMutexes[fullTableName].RLock()
	defer rowsMutexes[fullTableName].RUnlock()

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


func innerSearch(projName, stmt string) (*[]map[string]string, error) {
	stmtStruct, err := flaarum_shared.ParseSearchStmt(stmt)
	if err != nil {
		return nil, err
	}

	dataPath, _ := GetDataPath()
	tablePath := filepath.Join(dataPath, projName, stmtStruct.TableName)
	tableName := stmtStruct.TableName

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
		dataFIs, err := ioutil.ReadDir(filepath.Join(tablePath, "data"))
		if err != nil {
			return nil, errors.Wrap(err, "ioutil error.")
		}
		for _, dataFI := range dataFIs {
			retIds = append(retIds, dataFI.Name())
		}

	} else {
		// validation
		fieldNamesToFieldTypes := make(map[string]string)

		for _, fieldStruct := range tableStruct.Fields {
			fieldNamesToFieldTypes[fieldStruct.FieldName] = fieldStruct.FieldType
		}

		for i, whereStruct := range stmtStruct.WhereOptions {
			if i != 0 {
				if whereStruct.Joiner != "and" && whereStruct.Joiner != "or" {
					return nil, errors.New("Invalid statment: joiner must be one of 'and', 'or'.")
				}				
			}

			if fieldNamesToFieldTypes[whereStruct.FieldName] == "text" {
				return nil, errors.New(fmt.Sprintf("The field '%s' is not searchable since it is of type 'text'", 
					whereStruct.FieldName))
			}
		}

		// main search
		beforeFilter := make([][]string, 0)

		for _, whereStruct := range stmtStruct.WhereOptions {
			if whereStruct.Relation == "=" {
				stringIds := make([]string, 0)
				if whereStruct.FieldName == "id" {
					stringIds = append(stringIds, whereStruct.FieldValue)
				} else {
					indexesPath := filepath.Join(tablePath, "indexes", whereStruct.FieldName, makeSafeIndexValue(whereStruct.FieldValue))
					if doesPathExists(indexesPath) {
						raw, err := ioutil.ReadFile(indexesPath)
						if err != nil {
							return nil, errors.Wrap(err, "ioutil error")
						}
						stringIds = append(stringIds, strings.Split(string(raw), "\n")...)
					}
				}

				beforeFilter = append(beforeFilter, stringIds)

			} else if whereStruct.Relation == "!=" {
				stringIds := make([]string, 0)
				if whereStruct.FieldName == "id" {
					dataFIs, err := ioutil.ReadDir(filepath.Join(tablePath, "data"))
					if err != nil {
						return nil, errors.Wrap(err, "ioutil error.")
					}
					for _, dataFI := range dataFIs {
						if dataFI.Name() != whereStruct.FieldValue {
							stringIds = append(stringIds, dataFI.Name())							
						}
					}
				} else {
					safeVal := makeSafeIndexValue(whereStruct.FieldValue)
					allIndexesPath := filepath.Join(tablePath, "indexes", whereStruct.FieldName)
					if doesPathExists(allIndexesPath) {
						allIndexesFIs, err := ioutil.ReadDir(allIndexesPath)
						if err != nil {
							return nil, errors.Wrap(err, "ioutil error")
						}
						for _, aifi := range allIndexesFIs {
							if aifi.Name() != safeVal {
								raw, err := ioutil.ReadFile(filepath.Join(allIndexesPath, aifi.Name()))
								if err != nil {
									return nil, errors.Wrap(err, "ioutil error")
								}
								stringIds = append(stringIds, strings.Split(string(raw), "\n")...)
							}
						}
					}
				}

				beforeFilter = append(beforeFilter, stringIds)
			}


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
    raw, err := ioutil.ReadFile(filepath.Join(tablePath, "data", retId))
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
        raw2, err := ioutil.ReadFile(filepath.Join(dataPath, projName, pTbl, "data", data))
        if err != nil {
          continue
        }
        err = json.Unmarshal(raw2, &rowMap2)
        if err != nil {
          return nil, errors.Wrap(err, "json error.")
        }
        rowMap2["id"] = data
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
