package main

import (
  "net/http"
  "github.com/gorilla/mux"
  "github.com/pkg/errors"
  "fmt"
  "github.com/bankole7782/flaarum/flaarum_shared"
  "strconv"
  "os"
  "path/filepath"
)


func updateRows(w http.ResponseWriter, r *http.Request) {
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
    printValError(w, errors.New(fmt.Sprintf("table '%s' of project '%s' does not exists.", tableName, projName)))
    return
  }

  rows, err := innerSearch(projName, stmt)
  if err != nil {
    printError(w, err)
    return
  }

  if len(*rows) == 0 {
    printError(w, errors.New("There is no data to update. The search statement returned nothing."))
    return
  }

  updatedValues := make(map[string]string)
  for j := 1;; j++ {
    k := r.FormValue("set" + strconv.Itoa(j) + "_k")
    if k == "" {
      break
    }
    updatedValues[k] = r.FormValue("set" + strconv.Itoa(j) + "_v")
  }

  currentVersion, err := getCurrentVersionNum(projName, tableName)
  if err != nil {
    printError(w, err)
    return
  }
  updatedValues["_version"] = strconv.Itoa(currentVersion)

  tableStruct, err := getCurrentTableStructureParsed(projName, tableName)
  if err != nil {
    printError(w, err)
    return
  }

  fieldsDescs := make(map[string]flaarum_shared.FieldStruct)
  for _, fd := range tableStruct.Fields {
    fieldsDescs[fd.FieldName] = fd
  }

  patchedRows := make([]map[string]string, 0)
  for _, row := range *rows {
    newRow := make(map[string]string)
    for k, v := range row {
      if k == "id" {
        newRow[k] = v
      }
      _, ok := fieldsDescs[k]
      if ok {
        newRow[k] = v
      } else if isNotIndexedFieldVersioned(projName, tableName, k, row["_version"]) {
        // do nothing
      } else {
        err = deleteIndex(projName, tableName, k, v, row["id"], row["_version"])
        if err != nil {
          printError(w, err)
          return
        }
      }
    }
    for k, v := range updatedValues {
      newRow[k] = v
    }
    patchedRows = append(patchedRows, newRow)
  }



  // validation
  for i, row := range patchedRows {
    validatedRow, err := validateAndMutateDataMap(projName, tableName, row, (*rows)[i])
    if err != nil {
      printValError(w, err)
      return
    }
    patchedRows[i] = validatedRow
  }

  dataPath, _ := GetDataPath()

  createTableMutexIfNecessary(projName, tableName)
  fullTableName := projName + ":" + tableName
  tablesMutexes[fullTableName].Lock()
  defer tablesMutexes[fullTableName].Unlock()

  // create or delete indexes.
  for i, row := range patchedRows {
    for fieldName, newData := range row {
      if fieldName == "id" {
        continue
      }
      if isFieldOfTypeText(projName, tableName, fieldName) {
        // create a .text file which is a message to the tindexer program.
        newTextFileName := row["id"] + flaarum_shared.TEXT_INTR_DELIM + fieldName + ".text"
        err = os.WriteFile(filepath.Join(dataPath, projName, tableName, "txtinstrs", newTextFileName),
          []byte(newData), 0777)
        if err != nil {
          printError(w, errors.Wrap(err, "ioutil error"))
          return
        }
        continue
      }

      if isNotIndexedField(projName, tableName, fieldName) {
        continue
      }

      allOldRows := *rows
      oldRow := allOldRows[i]

      oldData, ok := oldRow[fieldName]
      if ok && oldData != newData {
        err = deleteIndex(projName, tableName, fieldName, oldData, row["id"], (*rows)[i]["_version"])
        if err != nil {
          printError(w, err)
          return
        }
        err = makeIndex(projName, tableName, fieldName, newData, row["id"])
        if err != nil {
          printError(w, err)
          return
        }
      }
    }

    // write data
    err = saveRowData(projName, tableName, row["id"], row)
    if err != nil {
      printError(w, err)
      return
    }
  }

  fmt.Fprintf(w, "ok")
}
