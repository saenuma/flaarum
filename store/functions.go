package main

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/internal"
)

func countRows(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]

	stmt := r.FormValue("stmt")
	qd, err := internal.ParseSearchStmt(stmt)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	tableName := qd.TableName

	if !doesTableExists(projName, tableName) {
		internal.PrintError(w, errors.New(fmt.Sprintf("table '%s' of project '%s' does not exists.", tableName, projName)))
		return
	}

	rows, err := innerSearch(projName, stmt)
	if err != nil {
		internal.PrintError(w, err)
		return
	}
	fmt.Fprintf(w, "%d", len(*rows))
}

func allRowsCount(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]
	tableName := vars["tbl"]

	dataPath, _ := internal.GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)

	createTableMutexIfNecessary(projName, tableName)
	fullTableName := projName + ":" + tableName
	tablesMutexes[fullTableName].RLock()
	defer tablesMutexes[fullTableName].RUnlock()

	dataF1Path := filepath.Join(tablePath, "data.flaa1")
	elemsMap, err := internal.ParseDataF1File(dataF1Path)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	fmt.Fprintf(w, "%d", len(elemsMap))
}

func sumRows(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]

	stmt := r.FormValue("stmt")
	qd, err := internal.ParseSearchStmt(stmt)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	tableName := qd.TableName

	if !doesTableExists(projName, tableName) {
		internal.PrintError(w, errors.New(fmt.Sprintf("table '%s' of project '%s' does not exists.", tableName, projName)))
		return
	}

	rows, err := innerSearch(projName, stmt)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	toSumField := r.FormValue("tosum")
	tableStruct, err := getCurrentTableStructureParsed(projName, tableName)
	if err != nil {
		internal.PrintError(w, err)
		return
	}
	found := false
	for _, fd := range tableStruct.Fields {
		if fd.FieldName == toSumField {
			found = true
		}
	}

	if !found {
		internal.PrintError(w, errors.New(fmt.Sprintf("The field '%s' does not exist in this table structure", toSumField)))
		return
	}

	var sumInt int64
	for _, row := range *rows {
		oneData, err := strconv.ParseInt(row[toSumField], 10, 64)
		if err != nil {
			internal.PrintError(w, errors.Wrap(err, "strconv failed."))
			return
		}
		sumInt += oneData

	}

	fmt.Fprintf(w, "%d", sumInt)
}
