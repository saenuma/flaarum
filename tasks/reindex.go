package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/otiai10/copy"
	"github.com/saenuma/flaarum/flaarum_shared"
)

func reindex(projName, tableName, eventPath string) {
	cl := getFlaarumCLIClient()

	if !flaarum_shared.DoesTableExists(projName, tableName) {
		P(fmt.Errorf("table '%s' of project '%s' does not exists", tableName, projName))
		return
	}

	cl.ProjName = projName

	stmt := fmt.Sprintf(`
		table: %s
	`, tableName)

	rows, err := cl.Search(stmt)
	if err != nil {
		P(err)
		return
	}

	tmpTableName := ".tmp_table_" + flaarum_shared.UntestedRandomString(5)
	dataPath, _ := flaarum_shared.GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)
	tmpTablePath := filepath.Join(dataPath, projName, tmpTableName)

	os.Rename(tablePath, tmpTablePath) // move the old contents to temporary directory
	os.MkdirAll(tablePath, 0777)

	// copy structures from tmpTablePath back to tablePath
	index := 1
	for {
		testedStructurePath := filepath.Join(tmpTablePath, fmt.Sprintf("structure%d.txt", index))
		newStructurePath := filepath.Join(tablePath, fmt.Sprintf("structure%d.txt", index))
		if flaarum_shared.DoesPathExists(testedStructurePath) {
			copy.Copy(testedStructurePath, newStructurePath)
		} else {
			break
		}
	}

	// begin insertion
	lastIdPath := filepath.Join(tablePath, "lastId.txt")
	var lastIdStr string
	for _, toInsert := range *rows {
		toInsertStrs, _ := cl.ConvertInterfaceMapToStringMap(tableName, toInsert)

		err = flaarum_shared.SaveRowData(projName, tableName, toInsertStrs["id"], toInsertStrs)
		if err != nil {
			P(err)
			return
		}

		lastIdStr = toInsert["id"].(string)

		// create indexes
		for k, v := range toInsertStrs {
			if flaarum_shared.IsNotIndexedField(projName, tableName, k) {
				continue
			}

			err := flaarum_shared.MakeIndex(projName, tableName, k, v, toInsertStrs["id"])
			if err != nil {
				P(err)
				return
			}

		}

	}

	os.WriteFile(lastIdPath, []byte(lastIdStr), 0777)
	os.RemoveAll(tmpTablePath)

	os.RemoveAll(tmpTablePath)
	os.RemoveAll(eventPath)
}
