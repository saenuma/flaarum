package main

import (
  "net/http"
  "github.com/gorilla/mux"
  "github.com/pkg/errors"
  "path/filepath"
  "os"
  "fmt"
  "github.com/bankole7782/flaarum/flaarum_shared"
  "io/ioutil"
  "strings"
  "github.com/adam-hanna/arrayOperations"
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

      } else if fkd.OnDelete == "on_delete_empty" {
        otherTblFullName := projName + ":" + otherTbl
        tablesMutexes[otherTblFullName].Lock()

        err := innerDeleteField(projName, otherTbl, fkd.FieldName, rows)
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
  dataPath, _ := GetDataPath()

  for _, row := range *rows {
    // delete index
    for f, d := range row {
      if f == "id" {
        continue
      }

      if ! isFieldOfTypeText(projName, tableName, f) {
        err := deleteIndex(projName, tableName, f, d, row["id"], row["_version"])
        if err != nil {
          return err
        }
      } else {
        err := ioutil.WriteFile(filepath.Join(dataPath, projName, tableName, "data", fmt.Sprintf("%s.rtext", row["id"])), 
          []byte("ok"), 0777)
        if err != nil {
          return err
        }
      }
    }

    // delete row file
    err := os.Remove(filepath.Join(getTablePath(projName, tableName), "data", row["id"]))
    if err != nil {
      return errors.Wrap(err, "file delete failed.")
    }
  }

  return nil
}


func deleteIndex(projName, tableName, fieldName, data, rowId, version string) error {
  if confirmFieldType(projName, tableName, fieldName, "date", version) {
    valueInTimeType, err := time.Parse(flaarum_shared.BROWSER_DATE_FORMAT, data)
    if err != nil {
      return errors.New(fmt.Sprintf("The value '%s' to field '%s' is not a date.", data, fieldName ))
    }

    dMap := make(map[string]string)
    f := fieldName
    dMap[f + "_year"] = strconv.Itoa(valueInTimeType.Year())
    dMap[f + "_month"] = strconv.Itoa(int(valueInTimeType.Month()))
    dMap[f + "_day"] = strconv.Itoa(valueInTimeType.Day())
    dMap[f + "_tz"] = valueInTimeType.Location().String()

    for toDeleteField, fieldData := range dMap {
      err := deleteIndex(projName, tableName, toDeleteField, fieldData, rowId, version)
      if err != nil {
        return err
      }
    }
  } else if confirmFieldType(projName, tableName, fieldName, "datetime", version) {
    valueInTimeType, err := time.Parse(flaarum_shared.BROWSER_DATETIME_FORMAT, data)
    if err != nil {
      return errors.New(fmt.Sprintf("The value '%s' to field '%s' is not a datetime.", data, fieldName))
    }

    dMap := make(map[string]string)
    f := fieldName
    dMap[f + "_year"] = strconv.Itoa(valueInTimeType.Year())
    dMap[f + "_month"] = strconv.Itoa(int(valueInTimeType.Month()))
    dMap[f + "_day"] = strconv.Itoa(valueInTimeType.Day())
    dMap[f + "_tz"] = valueInTimeType.Location().String()
    dMap[f + "_hour"] = strconv.Itoa(valueInTimeType.Hour())

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
    raw, err := ioutil.ReadFile(indexesPath)
    if err != nil {
      return errors.Wrap(err, "read file failed.")
    }
    similarIds := strings.Split(string(raw), "\n")
    toWriteIds := arrayOperations.DifferenceString([]string{rowId}, similarIds)
    if len(toWriteIds) == 0 {
      err = os.Remove(indexesPath)
      if err != nil {
        return errors.Wrap(err, "file delete failed.")
      }
    } else {
      err = ioutil.WriteFile(indexesPath, []byte(strings.Join(toWriteIds, "\n")), 0777)
      if err != nil {
        return errors.Wrap(err, "file write failed.")
      }
    }
  }

  indexesFieldsPath := filepath.Join(getTablePath(projName, tableName), "indexes", fieldName)
  fileFIs, err := ioutil.ReadDir(indexesFieldsPath)
  if err == nil && len(fileFIs) == 0 {
    err = os.Remove(indexesFieldsPath)
    if err != nil {
      return errors.Wrap(err, "delete failed.")
    }
  }

  return nil
}


func innerDeleteField(projName, tableName, fieldName string, rows *[]map[string]string) error {
  // validation
  if ! doesTableExists(projName, tableName) {
    return errors.New(fmt.Sprintf("table '%s' of database '%s' does not exists.", tableName, projName))
  }

  td, err := getCurrentTableStructureParsed(projName, tableName)
  if err != nil {
    return err
  }

  for _, fd := range td.Fields {
    if fd.FieldName == fieldName && fd.Required {
      return errors.New(fmt.Sprintf("The field '%s' is required and so cannot be deleted.", fieldName))
    }
  }

  for _, row := range *rows {
    f := fieldName
    data, ok := row[f]
    if ok {
      isFieldExempted := isFieldOfTypeText(projName, tableName, f)
      if isFieldExempted == false {
        err := deleteIndex(projName, tableName, f, data, row["id"], row["_version"])
        if err != nil {
          return err
        }
      }

      delete(row, f)
      if confirmFieldType(projName, tableName, f, "date", row["_version"]) {
        delete(row, f + "_year")
        delete(row, f + "_month")
        delete(row, f + "_day")
        delete(row, f + "_tz")
      } else if confirmFieldType(projName, tableName, f, "datetime", row["_version"]) {
        delete(row, f + "_year")
        delete(row, f + "_month")
        delete(row, f + "_day")
        delete(row, f + "_tz")
        delete(row, f + "_hour")
      }
    }

    rowId := row["id"]
    delete(row, "id")
    // write data
    err := saveRowData(projName, tableName, rowId, row)
    if err != nil {
      return err
    }

  }

  return nil
}


func deleteFields(w http.ResponseWriter, r *http.Request) {
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
    printError(w, errors.New(fmt.Sprintf("table '%s' of database '%s' does not exists.", tableName, projName)))
    return
  }

  toDeleteFields := make([]string, 0)
  for j := 1;; j++ {
    f := r.FormValue("to_delete_field" + strconv.Itoa(j))
    if f == "" {
      break
    }
    toDeleteFields = append(toDeleteFields, f)
  }

  rows, err := innerSearch(projName, stmt)
  if err != nil {
    printError(w, err)
    return
  }

  fullTableName := projName + ":" + tableName
  tablesMutexes[fullTableName].Lock()
  defer tablesMutexes[fullTableName].Unlock()

  for _, fieldName := range toDeleteFields {
    err := innerDeleteField(projName, tableName, fieldName, rows)
    if err != nil {
      printError(w, err)
      return
    }
  }

  fmt.Fprintf(w, "ok")
}