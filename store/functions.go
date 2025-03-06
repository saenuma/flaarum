package main

import (
	"fmt"
	"net/http"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/internal"
	"github.com/saenuma/flaarumlib"
)

func countRows(w http.ResponseWriter, r *http.Request) {

	projName := r.PathValue("proj")

	stmt := r.FormValue("stmt")
	qd, err := flaarumlib.ParseSearchStmt(stmt)
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
	projName := r.PathValue("proj")
	tableName := r.PathValue("tbl")

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
