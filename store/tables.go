package main

import (
	"net/http"
	"github.com/saenuma/flaarum/flaarum_shared"
	"fmt"
	"github.com/pkg/errors"
	"path/filepath"
	"github.com/gorilla/mux"
	"strings"
	"os"
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
	stmt += "fields:\n"
	for _, fieldStruct := range tableStruct.Fields {
		stmt += "  " + fieldStruct.FieldName + " " + fieldStruct.FieldType
		if fieldStruct.Required {
			stmt += " required"
		}
		if fieldStruct.Unique {
			stmt += " unique"
		}
		if fieldStruct.NotIndexed {
			stmt += " nindex"
		}
		stmt += "\n"
	}
	stmt += "::\n"
	if len(tableStruct.ForeignKeys) > 0 {
		stmt += "foreign_keys:\n"
		for _, fks := range tableStruct.ForeignKeys {
			stmt += "  " + fks.FieldName + " " + fks.PointedTable + " " + fks.OnDelete + "\n"
		}
		stmt += "::\n"
	}

	if len(tableStruct.UniqueGroups) > 0 {
		stmt += "unique_groups:\n"
		for _, ug := range tableStruct.UniqueGroups {
			stmt += "  " + strings.Join(ug, " ") + "\n"
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

	err = os.MkdirAll(filepath.Join(dataPath, projName, tableStruct.TableName), 0777)
	if err != nil {
		printError(w, errors.Wrap(err, "os error."))
		return
	}

	formattedStmt := formatTableStruct(tableStruct)
	err = os.WriteFile(filepath.Join(dataPath, projName, tableStruct.TableName, "structure1.txt"),
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

	oldFormattedStmt, err := os.ReadFile(filepath.Join(tablePath, fmt.Sprintf("structure%d.txt", currentVersionNum)))
	if err != nil {
		printError(w, err)
		return
	}

	formattedStmt := formatTableStruct(tableStruct)
	if formattedStmt != string(oldFormattedStmt) {
		nextVersionNumber := currentVersionNum + 1
		newStructurePath := filepath.Join(tablePath, fmt.Sprintf("structure%d.txt", nextVersionNumber))
		err = os.WriteFile(newStructurePath, []byte(formattedStmt), 0777)
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
	stmt, err := os.ReadFile(filepath.Join(tablePath, fmt.Sprintf("structure%s.txt", versionNum)))
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
