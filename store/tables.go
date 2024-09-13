package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/internal"
)

func doesTableExists(projName, tableName string) bool {
	return internal.DoesTableExists(projName, tableName)
}

func validateTableStruct(projName string, tableStruct internal.TableStruct) error {
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
		if !doesTableExists(projName, fkd.PointedTable) {
			return errors.New(fmt.Sprintf("Pointed Table '%s' in foreign key definition does not exist.", fkd.PointedTable))
		}
		if internal.FindIn(fields, fkd.FieldName) == -1 {
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

func createTable(w http.ResponseWriter, r *http.Request) {

	projName := r.PathValue("proj")

	stmt := r.FormValue("stmt")

	tableStruct, err := internal.ParseTableStructureStmt(stmt)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	dataPath, _ := internal.GetDataPath()

	projsMutex.Lock()
	defer projsMutex.Unlock()

	err = validateTableStruct(projName, tableStruct)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	if internal.DoesPathExists(filepath.Join(dataPath, projName, tableStruct.TableName)) {
		internal.PrintError(w, errors.New(fmt.Sprintf("Table '%s' of Project '%s' has already being created.", tableStruct.TableName, projName)))
		return
	}

	err = os.MkdirAll(filepath.Join(dataPath, projName, tableStruct.TableName), 0777)
	if err != nil {
		internal.PrintError(w, errors.Wrap(err, "os error."))
		return
	}

	formattedStmt := internal.FormatTableStruct(tableStruct)
	err = os.WriteFile(filepath.Join(dataPath, projName, tableStruct.TableName, "structure1.txt"),
		[]byte(formattedStmt), 0777)
	if err != nil {
		internal.PrintError(w, errors.Wrap(err, "ioutil error."))
		return
	}

	fmt.Fprintf(w, "ok")
}

func updateTableStructure(w http.ResponseWriter, r *http.Request) {

	projName := r.PathValue("proj")

	stmt := r.FormValue("stmt")

	tableStruct, err := internal.ParseTableStructureStmt(stmt)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	dataPath, _ := internal.GetDataPath()

	projsMutex.Lock()
	defer projsMutex.Unlock()

	err = validateTableStruct(projName, tableStruct)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	tablePath := filepath.Join(dataPath, projName, tableStruct.TableName)
	if !internal.DoesPathExists(tablePath) {
		internal.PrintError(w, errors.New(fmt.Sprintf("Table '%s' of Project '%s' does not exists.", tableStruct.TableName, projName)))
		return
	}

	currentVersionNum, err := getCurrentVersionNum(projName, tableStruct.TableName)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	oldFormattedStmt, err := os.ReadFile(filepath.Join(tablePath, fmt.Sprintf("structure%d.txt", currentVersionNum)))
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	formattedStmt := internal.FormatTableStruct(tableStruct)
	if formattedStmt != string(oldFormattedStmt) {
		nextVersionNumber := currentVersionNum + 1
		newStructurePath := filepath.Join(tablePath, fmt.Sprintf("structure%d.txt", nextVersionNumber))
		err = os.WriteFile(newStructurePath, []byte(formattedStmt), 0777)
		if err != nil {
			internal.PrintError(w, errors.Wrap(err, "ioutil error."))
			return
		}
	}

	fmt.Fprintf(w, "ok")
}

func getCurrentVersionNum(projName, tableName string) (int, error) {
	return internal.GetCurrentVersionNum(projName, tableName)
}

func getTableStructureParsed(projName, tableName string, versionNum int) (internal.TableStruct, error) {
	return internal.GetTableStructureParsed(projName, tableName, versionNum)
}

func getCurrentTableStructureParsed(projName, tableName string) (internal.TableStruct, error) {
	currentVersionNum, err := getCurrentVersionNum(projName, tableName)
	if err != nil {
		return internal.TableStruct{}, err
	}
	return getTableStructureParsed(projName, tableName, currentVersionNum)
}

func getCurrentVersionNumHTTP(w http.ResponseWriter, r *http.Request) {

	projName := r.PathValue("proj")
	tableName := r.PathValue("tbl")

	projsMutex.Lock()
	defer projsMutex.Unlock()

	currentVersionNum, err := getCurrentVersionNum(projName, tableName)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	fmt.Fprintf(w, "%d", currentVersionNum)
}

func getTableStructureHTTP(w http.ResponseWriter, r *http.Request) {

	projName := r.PathValue("proj")
	tableName := r.PathValue("tbl")
	versionNum := r.PathValue("vnum")

	dataPath, _ := internal.GetDataPath()

	projsMutex.Lock()
	defer projsMutex.Unlock()

	tablePath := filepath.Join(dataPath, projName, tableName)
	stmt, err := os.ReadFile(filepath.Join(tablePath, fmt.Sprintf("structure%s.txt", versionNum)))
	if err != nil {
		internal.PrintError(w, errors.Wrap(err, "ioutil error"))
		return
	}

	fmt.Fprint(w, string(stmt))
}

func getExistingTables(projName string) ([]string, error) {
	dataPath, _ := internal.GetDataPath()
	tablesPath := filepath.Join(dataPath, projName)

	tablesFIs, err := os.ReadDir(tablesPath)
	if err != nil {
		return nil, errors.Wrap(err, "read directory failed.")
	}

	tables := make([]string, 0)
	for _, tfi := range tablesFIs {
		if tfi.IsDir() {
			tables = append(tables, tfi.Name())
		}
	}

	return tables, nil
}

func listTables(w http.ResponseWriter, r *http.Request) {

	projName := r.PathValue("proj")

	projsMutex.Lock()
	defer projsMutex.Unlock()

	tables, err := getExistingTables(projName)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	jsonBytes, err := json.Marshal(tables)
	if err != nil {
		internal.PrintError(w, errors.Wrap(err, "json error."))
		return
	}

	fmt.Fprint(w, string(jsonBytes))
}

func deleteTable(w http.ResponseWriter, r *http.Request) {

	projName := r.PathValue("proj")
	tableName := r.PathValue("tbl")

	dataPath, _ := internal.GetDataPath()

	projsMutex.Lock()
	defer projsMutex.Unlock()

	createTableMutexIfNecessary(projName, tableName)
	fullTableName := projName + ":" + tableName
	tablesMutexes[fullTableName].Lock()

	if !doesTableExists(projName, tableName) {
		tablesMutexes[fullTableName].Unlock()
		internal.PrintError(w, errors.New(fmt.Sprintf("Table '%s' does not exist in project '%s'.", tableName, projName)))
		return
	}

	err := os.RemoveAll(filepath.Join(dataPath, projName, tableName))
	if err != nil {
		tablesMutexes[fullTableName].Unlock()
		internal.PrintError(w, errors.Wrap(err, "delete dir failed."))
		return
	}

	tablesMutexes[fullTableName].Unlock()
	delete(tablesMutexes, fullTableName)

	fmt.Fprintf(w, "ok")
}
