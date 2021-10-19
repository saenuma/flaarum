package main

import (
	"net/http"
	"github.com/bankole7782/flaarum/flaarum_shared"
	"fmt"
	"github.com/pkg/errors"
	"path/filepath"
	"github.com/gorilla/mux"
	"strings"
	"os"
	"strconv"
	"encoding/json"
)


func doesTableExists(projName, tableName string) bool {
  return flaarum_shared.DoesTableExists(projName, tableName)
}


func validateTableStruct(projName string, tableStruct flaarum_shared.TableStruct) error {
  fields := make([]string, 0)
  fTypeMap := make(map[string]string)
  td := tableStruct
  for _, fd := range td.Fields {
    if fd.Unique && fd.FieldType == "text" {
      return errors.New(fmt.Sprintf("The field '%s' is unique and cannot be of type 'text'", fd.FieldName))
    }
    fields = append(fields, fd.FieldName)
    fTypeMap[fd.FieldName] = fd.FieldType
  }

  for _, fkd := range td.ForeignKeys {
    if ! doesTableExists(projName, fkd.PointedTable) {
      return errors.New(fmt.Sprintf("Pointed Table '%s' in foreign key definition does not exist.", fkd.PointedTable))
    }
    if flaarum_shared.FindIn(fields, fkd.FieldName) == -1 {
      return errors.New(fmt.Sprintf("The field '%s' in a foreign key definition is not defined in the fields section",
        fkd.FieldName))
    }
    if fTypeMap[fkd.FieldName] != "int" {
      return errors.New(fmt.Sprintf("The field '%s' is not of type 'int' and so cannot be used in a foreign key defnition",
        fkd.FieldName))
    }
  }


  return nil

}


func formatTableStruct(tableStruct flaarum_shared.TableStruct) string {
	stmt := "table: " + tableStruct.TableName + "\n"
	stmt += "table_type: " + tableStruct.TableType + "\n"
	stmt += "fields:\n"
	for _, fieldStruct := range tableStruct.Fields {
		stmt += "\t" + fieldStruct.FieldName + " " + fieldStruct.FieldType
		if fieldStruct.Required {
			stmt += " required"
		}
		if fieldStruct.Unique {
			stmt += " unique"
		}
		stmt += "\n"
	}
	stmt += "::\n"
	if len(tableStruct.ForeignKeys) > 0 {
		stmt += "foreign_keys:\n"
		for _, fks := range tableStruct.ForeignKeys {
			stmt += "\t" + fks.FieldName + " " + fks.PointedTable + " " + fks.OnDelete + "\n"
		}
		stmt += "::\n"
	}

	return stmt
}


func createTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]

	stmt := r.FormValue("stmt")

	tableStruct, err := flaarum_shared.ParseTableStructureStmt(stmt)
	if err != nil {
		printError(w, err)
		return
	}

	dataPath, _ := GetDataPath()

	projsMutex.Lock()
	defer projsMutex.Unlock()

	err = validateTableStruct(projName, tableStruct)
	if err != nil {
		printError(w, err)
		return
	}

	if doesPathExists(filepath.Join(dataPath, projName, tableStruct.TableName)) {
		printError(w, errors.New(fmt.Sprintf("Table '%s' of Project '%s' has already being created.", tableStruct.TableName, projName)))
		return
	}

	toMake := []string{"data", "indexes", "tindexes", "structures", "txtinstrs", "intindexes"}
	for _, tm := range toMake {
		err := os.MkdirAll(filepath.Join(dataPath, projName, tableStruct.TableName, tm), 0777)
		if err != nil {
			printError(w, errors.Wrap(err, "os error."))
			return
		}
	}

	formattedStmt := formatTableStruct(tableStruct)
	err = os.WriteFile(filepath.Join(dataPath, projName, tableStruct.TableName, "structures", "1"),
		[]byte(formattedStmt), 0777)
	if err != nil {
		printError(w, errors.Wrap(err, "ioutil error."))
		return
	}

	fmt.Fprintf(w, "ok")
}


func updateTableStructure(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]

	stmt := r.FormValue("stmt")

	tableStruct, err := flaarum_shared.ParseTableStructureStmt(stmt)
	if err != nil {
		printError(w, err)
		return
	}

	dataPath, _ := GetDataPath()

	projsMutex.Lock()
	defer projsMutex.Unlock()

	err = validateTableStruct(projName, tableStruct)
	if err != nil {
		printError(w, err)
		return
	}

	tablePath := filepath.Join(dataPath, projName, tableStruct.TableName)
	if ! doesPathExists(tablePath) {
		printError(w, errors.New(fmt.Sprintf("Table '%s' of Project '%s' does not exists.", tableStruct.TableName, projName)))
		return
	}

	currentVersionNum, err := getCurrentVersionNum(projName, tableStruct.TableName)
	if err != nil {
		printError(w, err)
		return
	}

	oldFormattedStmt, err := os.ReadFile(filepath.Join(tablePath, "structures", strconv.Itoa(currentVersionNum)))
	if err != nil {
		printError(w, errors.Wrap(err, "ioutil error"))
		return
	}

	tableStructOld, err := flaarum_shared.ParseTableStructureStmt(string(oldFormattedStmt))
	if err != nil {
		printError(w, err)
		return
	}

	if tableStructOld.TableType != tableStruct.TableType {
		printError(w, errors.New("An existing table's table type cannot be changed."))
		return
	}

	formattedStmt := formatTableStruct(tableStruct)
	if formattedStmt != string(oldFormattedStmt) {
		nextVersionNumber := currentVersionNum + 1
		err = os.WriteFile(filepath.Join(tablePath, "structures", strconv.Itoa(nextVersionNumber)), []byte(formattedStmt), 0777)
		if err != nil {
			printError(w, errors.Wrap(err, "ioutil error."))
			return
		}
	}

	fmt.Fprintf(w, "ok")
}


func getCurrentVersionNum(projName, tableName string) (int, error) {
	return flaarum_shared.GetCurrentVersionNum(projName, tableName)
}


func getTableStructureParsed(projName, tableName string, versionNum int) (flaarum_shared.TableStruct, error) {
	return flaarum_shared.GetTableStructureParsed(projName, tableName, versionNum)
}


func getCurrentTableStructureParsed(projName, tableName string) (flaarum_shared.TableStruct, error) {
	currentVersionNum, err := getCurrentVersionNum(projName, tableName)
	if err != nil {
		return flaarum_shared.TableStruct{}, err
	}
	return getTableStructureParsed(projName, tableName, currentVersionNum)
}


func getCurrentVersionNumHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]
	tableName := vars["tbl"]

	projsMutex.Lock()
	defer projsMutex.Unlock()

	currentVersionNum, err := getCurrentVersionNum(projName, tableName)
	if err != nil {
		printError(w, err)
		return
	}

	fmt.Fprintf(w, "%d", currentVersionNum)
}


func getTableStructureHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]
	tableName := vars["tbl"]
	versionNum := vars["vnum"]

	dataPath, _ := GetDataPath()

	projsMutex.Lock()
	defer projsMutex.Unlock()

	tablePath := filepath.Join(dataPath, projName, tableName)
	stmt, err := os.ReadFile(filepath.Join(tablePath, "structures", versionNum))
	if err != nil {
		printError(w, errors.Wrap(err, "ioutil error"))
		return
	}

	fmt.Fprintf(w, string(stmt))
}


func getExistingTables(projName string) ([]string, error) {
  dataPath, _ := GetDataPath()
  tablesPath := filepath.Join(dataPath, projName)

  tablesFIs, err := os.ReadDir(tablesPath)
  if err != nil {
    return nil, errors.Wrap(err, "read directory failed.")
  }

  tables := make([]string, 0)
  for _, tfi := range tablesFIs {
    tables = append(tables, tfi.Name())
  }

  return tables, nil
}


func emptyTable(w http.ResponseWriter, r *http.Request) {
  vars := mux.Vars(r)
  projName := vars["proj"]
  tableName := vars["tbl"]
  dataPath, _ := GetDataPath()

  projsMutex.Lock()
  defer projsMutex.Unlock()

  createTableMutexIfNecessary(projName, tableName)
  fullTableName := projName + ":" + tableName
  tablesMutexes[fullTableName].Lock()
  defer tablesMutexes[fullTableName].Unlock()

  if ! doesTableExists(projName, tableName) {
    printError(w, errors.New(fmt.Sprintf("Table '%s' does not exist in project '%s'.", tableName, projName)))
    return
  }

  toDelete := []string{"data", "indexes", "tindexes", "txtinstrs", "intindexes", "lastId"}
  for _, todo := range toDelete {
    err := os.RemoveAll(filepath.Join(dataPath, projName, tableName, todo))
    if err != nil {
      printError(w, errors.Wrap(err, "delete directory failed."))
      return
    }
  }

  for _, tm := range toDelete[:4] {
    toMakePath := filepath.Join(dataPath, projName, tableName, tm)
    err := os.MkdirAll(toMakePath, 0777)
    if err != nil {
      err = errors.Wrap(err, "directory creation failed.")
      printError(w, err)
      return
    }
  }

  fmt.Fprintf(w, "ok")
}


func listTables(w http.ResponseWriter, r *http.Request) {
  vars := mux.Vars(r)
  projName := vars["proj"]

  projsMutex.Lock()
  defer projsMutex.Unlock()

  tables, err := getExistingTables(projName)
  if err != nil {
    printError(w, err)
    return
  }

  jsonBytes, err := json.Marshal(tables)
  if err != nil {
    printError(w, errors.Wrap(err, "json error."))
    return
  }

  fmt.Fprintf(w, string(jsonBytes))
}


func renameTable(w http.ResponseWriter, r *http.Request) {
  vars := mux.Vars(r)
  projName := vars["proj"]
  tableName := vars["tbl"]
  newTableName := vars["ntbl"]

  dataPath, _ := GetDataPath()

  projsMutex.Lock()
  defer projsMutex.Unlock()

  createTableMutexIfNecessary(projName, tableName)
  fullTableName := projName + ":" + tableName
  tablesMutexes[fullTableName].Lock()
  defer tablesMutexes[fullTableName].Unlock()

  if ! doesTableExists(projName, tableName) {
    printError(w, errors.New(fmt.Sprintf("Table '%s' does not exist in project '%s'.", tableName, projName)))
    return
  }

  if doesTableExists(projName, newTableName) {
    printError(w, errors.New(fmt.Sprintf("Table '%s' does exists in project '%s' and cannot be used as a new name",
      newTableName, projName)))
  }

  structuresFolder := filepath.Join(dataPath, projName, tableName, "structures")
  structuresFIs, err := os.ReadDir(structuresFolder)
  if err != nil {
    printError(w, errors.Wrap(err, "read directory failed."))
    return
  }

  for _, vfi := range structuresFIs {
    structurePath := filepath.Join(structuresFolder, vfi.Name())
    raw, err := os.ReadFile(structurePath)
    if err != nil {
      printError(w, errors.Wrap(err, "read failed."))
      return
    }

    structurePathContents := string(raw)
    out := strings.ReplaceAll(structurePathContents, tableName, newTableName)

    err = os.WriteFile(structurePath, []byte(out), 0777)
    if err != nil {
      printError(w, errors.Wrap(err, "write failed."))
      return
    }
  }

  oldPath := filepath.Join(dataPath, projName, tableName)
  newPath := filepath.Join(dataPath, projName, newTableName)
  err = os.Rename(oldPath, newPath)
  if err != nil {
    printError(w, errors.Wrap(err, "rename failed."))
    return
  }

  fmt.Fprintf(w, "ok")
}


func deleteTable(w http.ResponseWriter, r *http.Request) {
  vars := mux.Vars(r)
  projName := vars["proj"]
  tableName := vars["tbl"]

  dataPath, _ := GetDataPath()

  projsMutex.Lock()
  defer projsMutex.Unlock()

  createTableMutexIfNecessary(projName, tableName)
  fullTableName := projName + ":" + tableName
  tablesMutexes[fullTableName].Lock()

  if ! doesTableExists(projName, tableName) {
    tablesMutexes[fullTableName].Unlock()
    printError(w, errors.New(fmt.Sprintf("Table '%s' does not exist in project '%s'.", tableName, projName)))
    return
  }

  err := os.RemoveAll(filepath.Join(dataPath, projName, tableName))
  if err != nil {
    tablesMutexes[fullTableName].Unlock()
    printError(w, errors.Wrap(err, "delete dir failed."))
    return
  }

  tablesMutexes[fullTableName].Unlock()
  delete(tablesMutexes, fullTableName)

  fmt.Fprintf(w, "ok")
}
