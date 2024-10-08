package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/internal"
)

func reIndex(projName, tableName string) error {
	dataPath, _ := internal.GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)
	tmpTableName := tableName + "_ridx_tmp"
	workingTablePath := filepath.Join(dataPath, projName, tmpTableName)

	if internal.DoesPathExists(workingTablePath) {
		os.RemoveAll(workingTablePath)
	}

	os.MkdirAll(workingTablePath, 0777)

	// copy the data to the temporary table
	rawDataFlaa1, err := os.ReadFile(filepath.Join(tablePath, "data.flaa1"))
	if err != nil {
		return errors.Wrap(err, "file read error")
	}
	workingF1Path := filepath.Join(workingTablePath, "data.flaa1")
	os.WriteFile(workingF1Path, rawDataFlaa1, 0777)

	rawDataFlaa2, err := os.ReadFile(filepath.Join(tablePath, "data.flaa2"))
	if err != nil {
		return errors.Wrap(err, "file read error")
	}
	workingF2Path := filepath.Join(workingTablePath, "data.flaa2")
	os.WriteFile(workingF2Path, rawDataFlaa2, 0777)

	// start the reindexing
	elemsMap, _ := internal.ParseDataF1File(workingF1Path)

	var wg sync.WaitGroup

	for _, elem := range elemsMap {
		rawRowData, err := internal.ReadPortionF2File(projName, tmpTableName, "data",
			elem.DataBegin, elem.DataEnd)
		if err != nil {
			return err
		}

		rowMap, err := internal.ParseEncodedRowData(rawRowData)
		if err != nil {
			fmt.Println(err)
			continue
		}

		for k, v := range rowMap {
			if k == "id" {
				continue
			}
			idStr := rowMap["id"]
			wg.Add(1)
			go func(k, v, idStr string) {
				defer wg.Done()

				if !internal.IsNotIndexedField(projName, tmpTableName, k) {
					err := internal.MakeIndex(projName, tmpTableName, k, v, idStr)
					if err != nil {
						fmt.Println(err)
					}
				}
			}(k, v, idStr)

		}

		wg.Wait()
	}

	// delete old table and make temporary default.
	os.RemoveAll(tablePath)
	os.Rename(workingTablePath, tablePath)

	return nil
}
