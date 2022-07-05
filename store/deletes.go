package main

import (
  "net/http"
  "github.com/gorilla/mux"
  "github.com/pkg/errors"
  "path/filepath"
  "os"
  "fmt"
  "github.com/saenuma/flaarum/flaarum_shared"
  "strings"
  arrayOperations "github.com/adam-hanna/arrayOperations"
  "time"
  "strconv"
)


func deleteRows(w http.ResponseWriter, r *http.Request) {
  vars := mux.Vars(r)
  projName := vars["proj"]

  stmt := r.FormValue("stmt")
  stmtStruct, err := flaarum_shared.ParseSearchStmt(stmt)
  if err != nil {
    printError(w, err)
    return
  }

  tableName := stmtStruct.TableName
  if ! doesTableExists(projName, tableName) {
    printError(w, errors.New(fmt.Sprintf("table '%s' of project '%s' does not exists.", tableName, projName)))
    return
  }

  rows, err := innerSearch(projName, stmt)
  if err != nil {
    printError(w, err)
    return
  }

  existingTables, err := getExistingTables(projName)
  if err != nil {
    printError(w, err)
    return
  }

  relatedRelationshipDetails := make(map[string]flaarum_shared.FKeyStruct)
  for _, tbl := range existingTables {
    ts, err := getCurrentTableStructureParsed(projName, tbl)
    if err != nil {
      printError(w, err)
      return
    }

    for _, fkd := range ts.ForeignKeys {
      if fkd.PointedTable == tableName {
        relatedRelationshipDetails[tbl] = fkd
      }
    }
  }

  for _, row := range *rows {
    for otherTbl, fkd := range relatedRelationshipDetails {
      innerStmt := fmt.Sprintf(`
        table: %s
        where:
          %s = %s
        `, otherTbl, fkd.FieldName, row["id"])

      toCheckRows, err := innerSearch(projName, innerStmt)
      if err != nil {
        printError(w, err)
        return
      }

      if fkd.OnDelete == "on_delete_restrict" {
        if len(*toCheckRows) > 0 {
          printError(w, errors.New(fmt.Sprintf("This row with id '%s' is used in table '%s'",
            row["id"], otherTbl )))
          return
        }

      } else if fkd.OnDelete == "on_delete_delete" {
        otherTblFullName := projName + ":" + otherTbl
        tablesMutexes[otherTblFullName].Lock()

        err := innerDelete(projName, otherTbl, toCheckRows)
        if err != nil {
          printError(w, err)
          tablesMutexes[otherTblFullName].Unlock()
          return
        }
        tablesMutexes[otherTblFullName].Unlock()

      }

    }
  }

  fullTableName := projName + ":" + tableName
  tablesMutexes[fullTableName].Lock()
  defer tablesMutexes[fullTableName].Unlock()

  err = innerDelete(projName, tableName, rows)
  if err != nil {
    printError(w, err)
    return
  }

  fmt.Fprintf(w, "ok")
}


func innerDelete(projName, tableName string, rows *[]map[string]string) error {
  for _, row := range *rows {
    // delete index
    for f, d := range row {
      if f == "id" {
        continue
      }

      if isNotIndexedField(projName, tableName, f) {
        // do nothing
      } else {
        deleteIndex(projName, tableName, f, d, row["id"], row["_version"])
      }
    }

    dataPath, _ := GetDataPath()
    dataF1Path := filepath.Join(dataPath, projName, tableName, "data.flaa1")
    // update flaa1 file by rewriting it.
    elemsMap, err := flaarum_shared.ParseDataF1File(dataF1Path)
		if err != nil {
			return err
		}

    if _, ok := elemsMap[ row["id"] ]; ok {
      delete(elemsMap, row["id"])
    }

    err = flaarum_shared.RewriteF1File(projName, tableName, "data", elemsMap)
    if err != nil {
      return err
    }
  }

  return nil
}


func deleteIndex(projName, tableName, fieldName, data, rowId, version string) error {
  dataPath, _ := GetDataPath()

  if confirmFieldType(projName, tableName, fieldName, "date", version) {
    valueInTimeType, err := time.Parse(flaarum_shared.DATE_FORMAT, data)
    if err != nil {
      return errors.New(fmt.Sprintf("The value '%s' to field '%s' is not a date.", data, fieldName ))
    }

    dMap := make(map[string]string)
    f := fieldName
    dMap[f + "_year"] = strconv.Itoa(valueInTimeType.Year())
    dMap[f + "_month"] = strconv.Itoa(int(valueInTimeType.Month()))
    dMap[f + "_day"] = strconv.Itoa(valueInTimeType.Day())

    for toDeleteField, fieldData := range dMap {
      err := deleteIndex(projName, tableName, toDeleteField, fieldData, rowId, version)
      if err != nil {
        return err
      }
    }

  } else if confirmFieldType(projName, tableName, fieldName, "datetime", version) {
    valueInTimeType, err := time.Parse(flaarum_shared.DATETIME_FORMAT, data)
    if err != nil {
      return errors.New(fmt.Sprintf("The value '%s' to field '%s' is not a datetime.", data, fieldName))
    }

    dMap := make(map[string]string)
    f := fieldName
    dMap[f + "_year"] = strconv.Itoa(valueInTimeType.Year())
    dMap[f + "_month"] = strconv.Itoa(int(valueInTimeType.Month()))
    dMap[f + "_day"] = strconv.Itoa(valueInTimeType.Day())
    dMap[f + "_hour"] = strconv.Itoa(valueInTimeType.Hour())
    dMap[f + "_date"] = valueInTimeType.Format(flaarum_shared.DATE_FORMAT)
    dMap[f + "_tzname"], _ = valueInTimeType.Zone()

    for toDeleteField, fieldData := range dMap {
      err := deleteIndex(projName, tableName, toDeleteField, fieldData, rowId, version)
      if err != nil {
        return err
      }
    }

  }


  indexFileName := makeSafeIndexName(data)
  indexesPath := filepath.Join(getTablePath(projName, tableName), "indexes", fieldName, indexFileName)
  if doesPathExists(indexesPath) {
    raw, err := os.ReadFile(indexesPath)
    if err != nil {
      return errors.Wrap(err, "read file failed.")
    }
    similarIds := strings.Split(string(raw), "\n")
    toWriteIds := arrayOperations.Difference([]string{rowId}, similarIds)
    if len(toWriteIds) == 0 {
      err = os.Remove(indexesPath)
      if err != nil {
        return errors.Wrap(err, "file delete failed.")
      }
    } else {
      err = os.WriteFile(indexesPath, []byte(strings.Join(toWriteIds, "\n")), 0777)
      if err != nil {
        return errors.Wrap(err, "file write failed.")
      }
    }
  }

  indexesF1Path := filepath.Join(dataPath, projName, tableName, fieldName + "_indexes.flaa1")
  // update flaa1 file by rewriting it.
  elemsMap, err := flaarum_shared.ParseDataF1File(indexesF1Path)
  if err != nil {
    return err
  }

  if elem, ok := elemsMap[ data ]; ok {
    readBytes, err := flaarum_shared.ReadPortionF2File(projName, tableName, fieldName + "_indexes",
      elem.DataBegin, elem.DataEnd)
    if err != nil {
      fmt.Printf("%+v\n", err)
    }
    similarIds := strings.Split(string(readBytes), ",")
    toWriteIds := arrayOperations.Difference([]string{rowId}, similarIds)

    tablePath := getTablePath(projName, tableName)
  	indexesF2Path := filepath.Join(tablePath, fieldName + "_indexes.flaa2")
    toWriteData := strings.Join(toWriteIds, ",")

    var begin int64
  	var end int64
  	if doesPathExists(indexesF2Path) {
  		indexesF2Handle, err := os.OpenFile(indexesF2Path,	os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
  		if err != nil {
  			return errors.Wrap(err, "os error")
  		}
  		defer indexesF2Handle.Close()

  		stat, err := indexesF2Handle.Stat()
  		if err != nil {
  			return errors.Wrap(err, "os error")
  		}

  		size := stat.Size()
  		indexesF2Handle.Write([]byte(toWriteData))
  		begin = size
  		end = int64(len([]byte(toWriteData))) + size
  	} else {
  		err := os.WriteFile(indexesF2Path, []byte(toWriteData), 0777)
  		if err != nil {
  			return errors.Wrap(err, "os error")
  		}

  		begin = 0
  		end = int64(len([]byte(toWriteData)))
  	}

    elem := flaarum_shared.DataF1Elem{data, begin, end}
    err = flaarum_shared.AppendDataF1File(projName, tableName, fieldName + "_indexes", elem)
    if err != nil {
      return errors.Wrap(err, "os error")
    }
  }

  return nil
}
