package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/saenuma/flaarum/flaarum_shared"
)

func export(projName, tableName, eventPath string) {
	cl := getFlaarumCLIClient()
	cl.ProjName = projName

	if !flaarum_shared.DoesTableExists(projName, tableName) {
		P(fmt.Errorf("table '%s' of project '%s' does not exists", tableName, projName))
		return
	}

	dataPath, _ := flaarum_shared.GetDataPath()
	eOutPath := filepath.Join(dataPath, fmt.Sprintf("export_%s_%s_%s.json", projName, tableName, time.Now().Format("20060102T150405")))
	stmt := fmt.Sprintf(`
		table: %s
	`, tableName)

	rows, err := cl.Search(stmt)
	if err != nil {
		P(err)
		return
	}

	rawJson, _ := json.Marshal(rows)

	os.WriteFile(eOutPath, rawJson, 0777)
	os.RemoveAll(eventPath)
}
