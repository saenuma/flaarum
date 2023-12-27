package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/saenuma/flaarum/flaarum_shared"
)

const ROW_QTY_PER_FILE = 100000

func export(projName, tableName, eventPath string) {
	cl := getFlaarumCLIClient()
	cl.ProjName = projName

	if !flaarum_shared.DoesTableExists(projName, tableName) {
		P(fmt.Errorf("table '%s' of project '%s' does not exists", tableName, projName))
		return
	}

	dataPath, _ := flaarum_shared.GetDataPath()

	eOutBasePath := filepath.Join(dataPath,
		fmt.Sprintf("flaarum_export_%s_%s_%s", time.Now().Format("20060102T150405"), projName, tableName))

	os.MkdirAll(eOutBasePath, 0777)

	allRowCount, err := cl.AllRowsCount(tableName)
	if err != nil {
		P(err)
		return
	}

	count := 1
	for i := int64(0); i < allRowCount; i += ROW_QTY_PER_FILE {
		stmt := fmt.Sprintf(`
		table: %s
		start_index: %d
		limit: %d
	`, tableName, i, i+ROW_QTY_PER_FILE)

		rows, err := cl.Search(stmt)
		if err != nil {
			P(err)
			return
		}

		rawJson, _ := json.Marshal(rows)

		os.WriteFile(filepath.Join(eOutBasePath, fmt.Sprintf("%d", count)), rawJson, 0777)
		count += 1
		os.RemoveAll(eventPath)
	}

}
