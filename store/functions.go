package main

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/flaarum_shared"
)

func countRows(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]

	stmt := r.FormValue("stmt")
	qd, err := flaarum_shared.ParseSearchStmt(stmt)
	if err != nil {
		printError(w, err)
		return
	}

	tableName := qd.TableName

	if !doesTableExists(projName, tableName) {
		printError(w, errors.New(fmt.Sprintf("table '%s' of project '%s' does not exists.", tableName, projName)))
		return
	}

	rows, err := innerSearch(projName, stmt)
	if err != nil {
		printError(w, err)
		return
	}
	fmt.Fprintf(w, "%d", len(*rows))
}

func allRowsCount(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]
	tableName := vars["tbl"]

	dataPath, _ := GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)

	createTableMutexIfNecessary(projName, tableName)
	fullTableName := projName + ":" + tableName
	tablesMutexes[fullTableName].RLock()
	defer tablesMutexes[fullTableName].RUnlock()

	dataF1Path := filepath.Join(tablePath, "data.flaa1")
	elemsMap, err := flaarum_shared.ParseDataF1File(dataF1Path)
	if err != nil {
		printError(w, err)
		return
	}

	fmt.Fprintf(w, "%d", len(elemsMap))
}

func sumRows(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]

	stmt := r.FormValue("stmt")
	qd, err := flaarum_shared.ParseSearchStmt(stmt)
	if err != nil {
		printError(w, err)
		return
	}

	tableName := qd.TableName

	if !doesTableExists(projName, tableName) {
		printError(w, errors.New(fmt.Sprintf("table '%s' of project '%s' does not exists.", tableName, projName)))
		return
	}

	rows, err := innerSearch(projName, stmt)
	if err != nil {
		printError(w, err)
		return
	}

	toSumField := r.FormValue("tosum")
	tableStruct, err := getCurrentTableStructureParsed(projName, tableName)
	if err != nil {
		printError(w, err)
		return
	}
	var toSumFieldType string
	found := false
	for _, fd := range tableStruct.Fields {
		if fd.FieldName == toSumField {
			toSumFieldType = fd.FieldType
			found = true
		}
	}

	if !found {
		printError(w, errors.New(fmt.Sprintf("The field '%s' does not exist in this table structure", toSumField)))
		return
	}
	if toSumFieldType != "int" && toSumFieldType != "float" {
		printError(w, errors.New(fmt.Sprintf("The field '%s' is not a summable field.", toSumField)))
		return
	}

	var sumInt int64
	var sumFloat float64
	for _, row := range *rows {
		if toSumFieldType == "int" {
			oneData, err := strconv.ParseInt(row[toSumField], 10, 64)
			if err != nil {
				printError(w, errors.Wrap(err, "strconv failed."))
				return
			}
			sumInt += oneData
		} else {
			oneData, err := strconv.ParseFloat(row[toSumField], 64)
			if err != nil {
				printError(w, errors.Wrap(err, "strconv failed."))
				return
			}
			sumFloat += oneData
		}
	}

	if toSumFieldType == "int" {
		fmt.Fprintf(w, "%d", sumInt)
	} else {
		fmt.Fprintf(w, "%v", sumFloat)
	}
}
